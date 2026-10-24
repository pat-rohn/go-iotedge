package iotedge

// ingest_auth_test.go — Phase 1 of doc/EDGE_SERVER_WORKBOOK.md: opt-in shared
// ingest token (S2) on the endpoints the firmware uses.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"log/slog"
)

const testIngestToken = "unit-test-ingest-token"

type ingestCase struct {
	path string
	body any
}

// ingestCases returns one valid request per firmware endpoint.
func ingestCases() []ingestCase {
	name := "ingest-" + uuid.NewString()
	return []ingestCase{
		{URIInitDevice, map[string]any{"Device": DeviceDesc{Name: name, Sensors: []string{name + "T"}}}},
		{URISaveTimeseries, []map[string]any{{"Tag": name + "T", "Timestamps": []string{"2026-10-22T19:22:08.499Z"}, "Values": []string{"21.5"}}}},
		{URIUploadData, []map[string]any{{"Tag": name + "T", "Timestamps": []string{"2026-10-22 19:22:09.000"}, "Values": []string{"21.6"}}}},
		{URIUpdateSensor, sensorValues{Data: []TimeSeriesValue{{Name: name + "T", Value: 1}}}},
		{URILogging, map[string]any{"Device": name, "Text": "hello", "Level": 1}},
		{URIMonitoringHealth, DeviceHealthReport{Device: name, BootID: "boot-1", FirmwareVersion: "test"}},
	}
}

// serveIngest sends body to path from remoteAddr with an optional bearer token.
func serveIngest(router http.Handler, path string, body any, token, remoteAddr string) *httptest.ResponseRecorder {
	b, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(string(b)))
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.RemoteAddr = remoteAddr
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	return w
}

func TestIngestAuthOffAcceptsRequestsWithoutToken(t *testing.T) {
	_, router := securityRouter(t, SecurityConfig{IngestToken: testIngestToken})
	for _, c := range ingestCases() {
		if w := serveIngest(router, c.path, c.body, "", "192.0.2.1:1234"); w.Code != http.StatusOK {
			t.Errorf("%s without token, auth off = %d (%s), want 200", c.path, w.Code, w.Body.String())
		}
	}
}

func TestIngestAuthEnforceRequiresToken(t *testing.T) {
	_, router := securityRouter(t, SecurityConfig{IngestAuth: IngestAuthEnforce, IngestToken: testIngestToken})
	for _, c := range ingestCases() {
		if w := serveIngest(router, c.path, c.body, "", "192.0.2.2:1234"); w.Code != http.StatusUnauthorized {
			t.Errorf("%s without token = %d, want 401", c.path, w.Code)
		}
		if w := serveIngest(router, c.path, c.body, "wrong-token", "192.0.2.2:1234"); w.Code != http.StatusUnauthorized {
			t.Errorf("%s with wrong token = %d, want 401", c.path, w.Code)
		}
		if w := serveIngest(router, c.path, c.body, testIngestToken, "192.0.2.2:1234"); w.Code != http.StatusOK {
			t.Errorf("%s with token = %d (%s), want 200", c.path, w.Code, w.Body.String())
		}
	}
}

// The token protects device ingest only; the dashboard login stays as it is.
func TestIngestAuthEnforceLeavesLoginAlone(t *testing.T) {
	_, router := securityRouter(t, SecurityConfig{IngestAuth: IngestAuthEnforce, IngestToken: testIngestToken})
	cookie := login(t, router)
	if w := serve(router, http.MethodGet, URIDashboard, nil, cookie); w.Code != http.StatusOK {
		t.Fatalf("dashboard = %d, want 200", w.Code)
	}
}

func TestIngestAuthWarnAcceptsAndReportsClientOnce(t *testing.T) {
	iot, router := securityRouter(t, SecurityConfig{IngestAuth: IngestAuthWarn, IngestToken: testIngestToken})
	cases := ingestCases()
	withLogHook(func(h *logHook) {
		for _, c := range cases {
			if w := serveIngest(router, c.path, c.body, "", "192.0.2.3:1234"); w.Code != http.StatusOK {
				t.Errorf("%s without token, warn = %d (%s), want 200", c.path, w.Code, w.Body.String())
			}
		}
		if w := serveIngest(router, cases[0].path, cases[0].body, testIngestToken, "192.0.2.4:1234"); w.Code != http.StatusOK {
			t.Errorf("with token, warn = %d, want 200", w.Code)
		}
		h.mu.Lock()
		defer h.mu.Unlock()
		warnings := 0
		for _, e := range h.entries {
			if e.Level == slog.LevelWarn && strings.Contains(e.Message, "192.0.2.3") {
				warnings++
			}
			if strings.Contains(e.Message, "192.0.2.4") {
				t.Errorf("client with valid token was reported: %q", e.Message)
			}
		}
		if warnings != 1 {
			t.Errorf("warnings for 192.0.2.3 = %d, want 1 for %d requests", warnings, len(cases))
		}
	})

	clients := iot.ingestAuth.unauthenticated()
	if len(clients) != 1 || clients[0].IP != "192.0.2.3" || clients[0].Requests != len(cases) {
		t.Fatalf("unauthenticated clients = %+v, want 192.0.2.3 with %d requests", clients, len(cases))
	}
}

func TestIngestAuthUnauthenticatedClientsOnDashboard(t *testing.T) {
	_, router := securityRouter(t, SecurityConfig{IngestAuth: IngestAuthWarn, IngestToken: testIngestToken})
	c := ingestCases()[0]
	serveIngest(router, c.path, c.body, "", "192.0.2.5:1234")
	cookie := login(t, router)

	req := httptest.NewRequest(http.MethodGet, URIDashboard, nil)
	req.Header.Set("Accept", "application/json")
	req.AddCookie(cookie)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	var resp DashboardResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("dashboard JSON: %v", err)
	}
	if len(resp.UnauthenticatedIngest) != 1 || resp.UnauthenticatedIngest[0].IP != "192.0.2.5" {
		t.Fatalf("UnauthenticatedIngest = %+v, want 192.0.2.5", resp.UnauthenticatedIngest)
	}

	w = serve(router, http.MethodGet, URIDashboard, nil, cookie)
	if !strings.Contains(w.Body.String(), "192.0.2.5") {
		t.Error("dashboard page does not list the unauthenticated client")
	}
}

func TestIngestAuthConfigValidation(t *testing.T) {
	invalid := []SecurityConfig{
		{IngestAuth: IngestAuthWarn},
		{IngestAuth: IngestAuthEnforce},
		{IngestAuth: "sometimes", IngestToken: testIngestToken},
	}
	for _, sec := range invalid {
		iot := regressionIoT()
		iot.IoTConfig.Password = testPassword
		iot.IoTConfig.SessionSecret = "unit-test-session-secret-32-bytes!!"
		iot.IoTConfig.Security = sec
		if _, err := iot.newRouter(); err == nil {
			t.Errorf("newRouter accepted %+v", sec)
		}
	}
	for _, mode := range []string{"", IngestAuthOff, "OFF", " Warn "} {
		iot := regressionIoT()
		iot.IoTConfig.Password = testPassword
		iot.IoTConfig.SessionSecret = "unit-test-session-secret-32-bytes!!"
		iot.IoTConfig.Security = SecurityConfig{IngestAuth: mode, IngestToken: testIngestToken}
		if _, err := iot.newRouter(); err != nil {
			t.Errorf("newRouter rejected IngestAuth %q: %v", mode, err)
		}
	}
}
