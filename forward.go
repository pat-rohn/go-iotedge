package iotedge

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/pat-rohn/go-iotedge/internal/store"
	"github.com/pat-rohn/timeseries"
	"log/slog"
)

// ForwardingConfig sends the MQTT readings to a central go-iotedge instead of
// storing them locally.
type ForwardingConfig struct {
	// URL of the central server, e.g. "http://central:3004".
	URL string
	// Token is the central server's Security.IngestToken, sent as
	// "Authorization: Bearer"; empty sends none.
	Token string
}

const (
	forwardOutboxTable = store.ForwardOutboxTable
	forwardBaseBackoff = 10 * time.Second
	forwardMaxBackoff  = time.Hour
	// forwardBatchLimit bounds the batches sent per delivery round.
	forwardBatchLimit = 50
)

// forwarding returns the effective forwarding settings; the deprecated
// MQTTRedirectAddress still works when Forwarding.URL is empty.
func (c IoTConfig) forwarding() ForwardingConfig {
	f := c.Forwarding
	if f.URL == "" && c.MQTTRedirectAddress != "" {
		f.URL = c.MQTTRedirectAddress
	}
	f.URL = strings.TrimRight(f.URL, "/")
	return f
}

// forwardBackoff is the wait after the given number of failed attempts.
func forwardBackoff(attempts int) time.Duration {
	delay := forwardBaseBackoff
	for i := 1; i < attempts; i++ {
		delay *= 2
		if delay >= forwardMaxBackoff {
			return forwardMaxBackoff
		}
	}
	return delay
}

// forwarder delivers queued batches from the forward_outbox table to the
// central server, oldest first. A batch leaves the outbox only once the
// central server accepted it, so readings survive outages and restarts.
type forwarder struct {
	db     *DeviceDB
	url    string
	token  string
	client *http.Client
	now    func() time.Time

	mu       sync.Mutex // one delivery round at a time
	lastWarn time.Time

	statusMu      sync.Mutex
	lastDelivered time.Time
	lastError     string
	lastErrorAt   time.Time
}

// forwardStatus is what the system page shows about forwarding.
type forwardStatus struct {
	URL           string
	Pending       int
	LastDelivered time.Time
	LastError     string
	LastErrorAt   time.Time
}

func (f *forwarder) status() forwardStatus {
	pending, _ := f.pending()
	f.statusMu.Lock()
	defer f.statusMu.Unlock()
	return forwardStatus{URL: f.url, Pending: pending, LastDelivered: f.lastDelivered,
		LastError: f.lastError, LastErrorAt: f.lastErrorAt}
}

func newForwarder(db *DeviceDB, cfg ForwardingConfig) *forwarder {
	return &forwarder{
		db:     db,
		url:    strings.TrimRight(cfg.URL, "/"),
		token:  cfg.Token,
		client: &http.Client{Timeout: 10 * time.Second},
		now:    time.Now,
	}
}

// enqueue stores one batch for delivery.
func (f *forwarder) enqueue(data []timeseries.TimeseriesImportStruct) error {
	if len(data) == 0 {
		return nil
	}
	payload, err := json.Marshal(data)
	if err != nil {
		return err
	}
	now := f.now().Unix()
	_, err = f.db.DB.Exec(f.db.Rebind(`INSERT INTO `+forwardOutboxTable+
		` (created_at, payload, attempts, next_attempt_at) VALUES (?, ?, 0, ?)`), now, string(payload), now)
	return err
}

// pending is the number of batches not yet delivered.
func (f *forwarder) pending() (int, error) {
	var n int
	err := f.db.DB.QueryRow(`SELECT COUNT(*) FROM ` + forwardOutboxTable).Scan(&n)
	return n, err
}

type outboxBatch struct {
	id            int64
	payload       string
	attempts      int
	nextAttemptAt int64
}

// deliverDue sends the oldest batches in order. It stops at the first batch
// that is not due yet or fails (rescheduled with backoff), so batches never
// overtake each other. It returns the number delivered.
func (f *forwarder) deliverDue(ctx context.Context) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	rows, err := f.db.DB.QueryContext(ctx, f.db.Rebind(`SELECT id, payload, attempts, next_attempt_at FROM `+forwardOutboxTable+
		` ORDER BY id LIMIT ?`), forwardBatchLimit)
	if err != nil {
		return 0, err
	}
	var oldest []outboxBatch
	for rows.Next() {
		var b outboxBatch
		if err := rows.Scan(&b.id, &b.payload, &b.attempts, &b.nextAttemptAt); err != nil {
			rows.Close()
			return 0, err
		}
		oldest = append(oldest, b)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}

	delivered := 0
	for _, b := range oldest {
		if b.nextAttemptAt > f.now().Unix() {
			break
		}
		if err := f.post(ctx, b.payload); err != nil {
			attempts := b.attempts + 1
			next := f.now().Add(forwardBackoff(attempts)).Unix()
			if _, dbErr := f.db.DB.Exec(f.db.Rebind(`UPDATE `+forwardOutboxTable+
				` SET attempts = ?, next_attempt_at = ? WHERE id = ?`), attempts, next, b.id); dbErr != nil {
				return delivered, dbErr
			}
			f.statusMu.Lock()
			f.lastError, f.lastErrorAt = err.Error(), f.now()
			f.statusMu.Unlock()
			f.warn(err)
			return delivered, err
		}
		if _, err := f.db.DB.Exec(f.db.Rebind(`DELETE FROM `+forwardOutboxTable+` WHERE id = ?`), b.id); err != nil {
			return delivered, err
		}
		delivered++
		f.statusMu.Lock()
		f.lastDelivered = f.now()
		f.statusMu.Unlock()
	}
	return delivered, nil
}

func (f *forwarder) post(ctx context.Context, payload string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, f.url+URISaveTimeseries, bytes.NewBufferString(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if f.token != "" {
		req.Header.Set("Authorization", "Bearer "+f.token)
	}
	resp, err := f.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("central server answered %s", resp.Status)
	}
	return nil
}

// warn logs a delivery failure at most once per hour: warnings are persisted.
func (f *forwarder) warn(err error) {
	now := f.now()
	if now.Sub(f.lastWarn) < time.Hour {
		return
	}
	f.lastWarn = now
	n, _ := f.pending()
	slog.Warn(fmt.Sprintf("forwarding to %s failed, %d batches queued: %v", f.url, n, err), "fnct", "forwarder")
}

// deliverInBackground starts a delivery round unless one is still running
// (a slow central server must not pile up rounds).
func (f *forwarder) deliverInBackground(ctx context.Context) {
	if !f.mu.TryLock() {
		return
	}
	f.mu.Unlock()
	go func() {
		if _, err := f.deliverDue(ctx); err != nil && ctx.Err() == nil {
			slog.Debug(fmt.Sprintf("delivery round: %v", err), "fnct", "forwarder")
		}
	}()
}
