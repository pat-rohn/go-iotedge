package iotedge

// forward_test.go — F1 of doc/EDGE_SERVER_WORKBOOK.md: MQTT readings are
// forwarded to a central go-iotedge through a durable outbox, with retries
// and the central server's ingest token.

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pat-rohn/timeseries"
)

// fakeCentral records the batches POSTed to /timeseries/save and answers
// with status (200 unless changed).
type fakeCentral struct {
	*httptest.Server
	mu      sync.Mutex
	status  int
	batches [][]timeseries.TimeseriesImportStruct
	auth    []string
}

func newFakeCentral(t *testing.T) *fakeCentral {
	t.Helper()
	c := &fakeCentral{status: http.StatusOK}
	c.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != URISaveTimeseries || r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		body, _ := io.ReadAll(r.Body)
		c.mu.Lock()
		defer c.mu.Unlock()
		c.auth = append(c.auth, r.Header.Get("Authorization"))
		if c.status != http.StatusOK {
			w.WriteHeader(c.status)
			return
		}
		var batch []timeseries.TimeseriesImportStruct
		if err := json.Unmarshal(body, &batch); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		c.batches = append(c.batches, batch)
	}))
	t.Cleanup(c.Close)
	return c
}

func (c *fakeCentral) setStatus(status int) {
	c.mu.Lock()
	c.status = status
	c.mu.Unlock()
}

func (c *fakeCentral) received() ([][]timeseries.TimeseriesImportStruct, []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([][]timeseries.TimeseriesImportStruct(nil), c.batches...), append([]string(nil), c.auth...)
}

// testForwarder returns a forwarder on the shared test DB with an empty outbox
// and a clock the test controls.
func testForwarder(t *testing.T, url, token string) (*forwarder, *time.Time) {
	t.Helper()
	iot := regressionIoT()
	f := newForwarder(iot.DeviceDB, ForwardingConfig{URL: url, Token: token})
	if _, err := iot.DeviceDB.DB.Exec(`DELETE FROM ` + forwardOutboxTable); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	f.now = func() time.Time { return now }
	return f, &now
}

func series(tag string, values ...string) []timeseries.TimeseriesImportStruct {
	ts := timeseries.TimeseriesImportStruct{Tag: tag}
	for i, v := range values {
		ts.Timestamps = append(ts.Timestamps, time.Date(2026, 10, 24, 12, 0, i, 0, time.UTC).Format("2006-01-02 15:04:05.000"))
		ts.Values = append(ts.Values, v)
	}
	return []timeseries.TimeseriesImportStruct{ts}
}

func TestForwarderDeliversWithTokenAndEmptiesOutbox(t *testing.T) {
	central := newFakeCentral(t)
	f, _ := testForwarder(t, central.URL, "central-token")
	if err := f.enqueue(series("fwd-a", "1", "2")); err != nil {
		t.Fatal(err)
	}
	if n, _ := f.pending(); n != 1 {
		t.Fatalf("pending after enqueue = %d, want 1", n)
	}
	delivered, err := f.deliverDue(context.Background())
	if err != nil || delivered != 1 {
		t.Fatalf("deliverDue = %d, %v; want 1, nil", delivered, err)
	}
	batches, auth := central.received()
	if len(batches) != 1 || batches[0][0].Tag != "fwd-a" || len(batches[0][0].Values) != 2 {
		t.Fatalf("central received %+v", batches)
	}
	if auth[0] != "Bearer central-token" {
		t.Errorf("Authorization = %q, want Bearer central-token", auth[0])
	}
	if n, _ := f.pending(); n != 0 {
		t.Errorf("pending after delivery = %d, want 0", n)
	}
}

func TestForwarderWithoutTokenSendsNoAuthorization(t *testing.T) {
	central := newFakeCentral(t)
	f, _ := testForwarder(t, central.URL, "")
	f.enqueue(series("fwd-noauth", "1"))
	f.deliverDue(context.Background())
	_, auth := central.received()
	if len(auth) != 1 || auth[0] != "" {
		t.Fatalf("Authorization headers = %q, want one empty", auth)
	}
}

func TestForwarderKeepsBatchesWhileCentralFailsAndRetriesWithBackoff(t *testing.T) {
	central := newFakeCentral(t)
	central.setStatus(http.StatusServiceUnavailable)
	f, now := testForwarder(t, central.URL, "")
	f.enqueue(series("fwd-first", "1"))
	f.enqueue(series("fwd-second", "2"))

	if delivered, err := f.deliverDue(context.Background()); err == nil || delivered != 0 {
		t.Fatalf("deliverDue with central down = %d, %v; want 0 and an error", delivered, err)
	}
	if _, auth := central.received(); len(auth) != 1 {
		t.Fatalf("requests = %d, want 1: stop at the first failure, keep the order", len(auth))
	}
	if n, _ := f.pending(); n != 2 {
		t.Fatalf("pending = %d, want 2", n)
	}

	// Not due yet: no request.
	*now = now.Add(forwardBaseBackoff / 2)
	f.deliverDue(context.Background())
	if _, auth := central.received(); len(auth) != 1 {
		t.Fatalf("requests before the backoff ran out = %d, want 1", len(auth))
	}

	central.setStatus(http.StatusOK)
	*now = now.Add(forwardBaseBackoff)
	delivered, err := f.deliverDue(context.Background())
	if err != nil || delivered != 2 {
		t.Fatalf("deliverDue after recovery = %d, %v; want 2, nil", delivered, err)
	}
	batches, _ := central.received()
	if batches[0][0].Tag != "fwd-first" || batches[1][0].Tag != "fwd-second" {
		t.Fatalf("delivery order = %s, %s; want fwd-first, fwd-second", batches[0][0].Tag, batches[1][0].Tag)
	}
}

func TestForwardBackoffDoublesUpToAnHour(t *testing.T) {
	cases := map[int]time.Duration{
		1:  forwardBaseBackoff,
		2:  2 * forwardBaseBackoff,
		3:  4 * forwardBaseBackoff,
		30: time.Hour,
	}
	for attempts, want := range cases {
		if got := forwardBackoff(attempts); got != want {
			t.Errorf("forwardBackoff(%d) = %v, want %v", attempts, got, want)
		}
	}
}

// The outbox lives in the database: a restart delivers what an earlier
// process queued.
func TestForwarderOutboxSurvivesRestart(t *testing.T) {
	central := newFakeCentral(t)
	first, _ := testForwarder(t, central.URL, "")
	first.enqueue(series("fwd-durable", "7"))

	second := newForwarder(first.db, ForwardingConfig{URL: central.URL})
	if delivered, err := second.deliverDue(context.Background()); err != nil || delivered != 1 {
		t.Fatalf("new forwarder delivered %d, %v; want 1, nil", delivered, err)
	}
}

func TestForwardingConfigMapsLegacyRedirectAddress(t *testing.T) {
	legacy := IoTConfig{MQTTRedirectAddress: "http://central:3004"}
	if got := legacy.forwarding(); got.URL != "http://central:3004" || got.Token != "" {
		t.Errorf("legacy forwarding = %+v", got)
	}
	both := IoTConfig{MQTTRedirectAddress: "http://old:3004",
		Forwarding: ForwardingConfig{URL: "http://new:3004", Token: "t"}}
	if got := both.forwarding(); got.URL != "http://new:3004" || got.Token != "t" {
		t.Errorf("Forwarding.URL must win over MQTTRedirectAddress: %+v", got)
	}
	if got := (IoTConfig{}).forwarding(); got.URL != "" {
		t.Errorf("no forwarding configured: %+v", got)
	}
}

// End to end: an MQTT reading reaches the central server and, as before, is
// not stored locally.
func TestMQTTBrokerForwardsReadingsInsteadOfStoring(t *testing.T) {
	central := newFakeCentral(t)
	iot := regressionIoT()
	if _, err := iot.DeviceDB.DB.Exec(`DELETE FROM ` + forwardOutboxTable); err != nil {
		t.Fatal(err)
	}
	iot.IoTConfig.MQTTPort = freePort(t)
	iot.IoTConfig.UploadInterval = 1
	iot.IoTConfig.Forwarding = ForwardingConfig{URL: central.URL, Token: "central-token"}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- iot.StartMQTTBroker(ctx) }()
	defer func() {
		cancel()
		waitReturn(t, done, "StartMQTTBroker")
	}()

	var client interface{ Disconnect(uint) }
	tag := "fwd-mqtt-" + uuid.NewString()
	eventually(t, "broker up", func() bool {
		c, err := connectClient(iot.IoTConfig.MQTTPort, "")
		if err != nil {
			return false
		}
		client = c
		publish(t, c, "frick/test/dev/"+tag+"/data", "42")
		return true
	})
	defer client.Disconnect(100)

	eventually(t, "reading forwarded", func() bool {
		batches, _ := central.received()
		for _, b := range batches {
			for _, s := range b {
				if s.Tag == tag && len(s.Values) == 1 && s.Values[0] == "42" {
					return true
				}
			}
		}
		return false
	})
	var n int
	row := iot.DeviceDB.DB.QueryRow(iot.DeviceDB.Rebind(
		"SELECT COUNT(*) FROM "+iot.IoTConfig.TimeseriesTable+" WHERE tag = ?"), tag)
	if err := row.Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("forwarded reading also stored locally (%d rows)", n)
	}
}

// Contract with the real ingest path: a central go-iotedge that enforces its
// ingest token accepts the forwarded batch and stores it.
func TestForwarderIsAcceptedByEnforcingCentral(t *testing.T) {
	central, router := securityRouter(t, SecurityConfig{IngestAuth: IngestAuthEnforce, IngestToken: testIngestToken})
	server := httptest.NewServer(router)
	defer server.Close()
	tag := "fwd-real-" + uuid.NewString()

	rejected, _ := testForwarder(t, server.URL, "wrong-token")
	rejected.now = time.Now
	rejected.enqueue(series(tag, "5"))
	if _, err := rejected.deliverDue(context.Background()); err == nil {
		t.Fatal("central accepted a batch with the wrong token")
	}

	accepted := newForwarder(rejected.db, ForwardingConfig{URL: server.URL, Token: testIngestToken})
	accepted.now = func() time.Time { return time.Now().Add(forwardMaxBackoff) }
	if delivered, err := accepted.deliverDue(context.Background()); err != nil || delivered != 1 {
		t.Fatalf("deliverDue = %d, %v; want 1, nil", delivered, err)
	}
	var n int
	row := central.DeviceDB.DB.QueryRow(central.DeviceDB.Rebind(
		"SELECT COUNT(*) FROM "+central.IoTConfig.TimeseriesTable+" WHERE tag = ?"), tag)
	if err := row.Scan(&n); err != nil || n != 1 {
		t.Fatalf("central stored %d rows (%v), want 1", n, err)
	}
}
