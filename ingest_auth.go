package iotedge

import (
	"crypto/subtle"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
)

// Security.IngestAuth modes for the shared ingest token.
const (
	IngestAuthOff     = "off"     // no check (default)
	IngestAuthWarn    = "warn"    // accept, but report clients without a valid token
	IngestAuthEnforce = "enforce" // reject requests without a valid token
)

// ingestWarnInterval limits warnings to one per client in this interval.
const ingestWarnInterval = time.Hour

// UnauthenticatedClient is a client that sent device data without a valid
// ingest token while IngestAuth is "warn".
type UnauthenticatedClient struct {
	IP       string    `json:"IP"`
	LastSeen time.Time `json:"LastSeen"`
	Requests int       `json:"Requests"`
}

type ingestAuth struct {
	mode  string
	token []byte

	mu       sync.Mutex
	clients  map[string]*UnauthenticatedClient
	lastWarn map[string]time.Time
}

func normalizeIngestAuthMode(mode string) string {
	mode = strings.ToLower(strings.TrimSpace(mode))
	if mode == "" {
		return IngestAuthOff
	}
	return mode
}

func newIngestAuth(sec SecurityConfig) (*ingestAuth, error) {
	mode := normalizeIngestAuthMode(sec.IngestAuth)
	switch mode {
	case IngestAuthOff:
	case IngestAuthWarn, IngestAuthEnforce:
		if sec.IngestToken == "" {
			return nil, fmt.Errorf("Security.IngestAuth %q needs Security.IngestToken", mode)
		}
	default:
		return nil, fmt.Errorf("Security.IngestAuth must be %q, %q or %q, not %q",
			IngestAuthOff, IngestAuthWarn, IngestAuthEnforce, sec.IngestAuth)
	}
	return &ingestAuth{
		mode:     mode,
		token:    []byte(sec.IngestToken),
		clients:  map[string]*UnauthenticatedClient{},
		lastWarn: map[string]time.Time{},
	}, nil
}

func (a *ingestAuth) validToken(c *gin.Context) bool {
	token, ok := strings.CutPrefix(c.GetHeader("Authorization"), "Bearer ")
	return ok && subtle.ConstantTimeCompare([]byte(token), a.token) == 1
}

// handler is the middleware for the endpoints the firmware uses.
func (a *ingestAuth) handler(c *gin.Context) {
	if a.mode == IngestAuthOff || a.validToken(c) {
		c.Next()
		return
	}
	if a.mode == IngestAuthEnforce {
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing or invalid ingest token"})
		return
	}
	a.report(c.ClientIP(), c.Request.URL.Path, time.Now())
	c.Next()
}

func (a *ingestAuth) report(ip, path string, now time.Time) {
	a.mu.Lock()
	client, ok := a.clients[ip]
	if !ok {
		client = &UnauthenticatedClient{IP: ip}
		a.clients[ip] = client
	}
	client.LastSeen = now
	client.Requests++
	warn := now.Sub(a.lastWarn[ip]) >= ingestWarnInterval
	if warn {
		a.lastWarn[ip] = now
	}
	a.mu.Unlock()
	if warn {
		log.WithField("fnct", "ingestAuth").Warnf(
			"device data from %s (%s) without a valid ingest token; accepted because Security.IngestAuth is %q",
			ip, path, IngestAuthWarn)
	}
}

// unauthenticated lists clients reported in warn mode, most recent first.
func (a *ingestAuth) unauthenticated() []UnauthenticatedClient {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	list := make([]UnauthenticatedClient, 0, len(a.clients))
	for _, client := range a.clients {
		list = append(list, *client)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].LastSeen.After(list[j].LastSeen) })
	return list
}
