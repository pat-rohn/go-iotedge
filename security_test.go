package iotedge

// security_test.go — Phase 0 of doc/EDGE_SERVER_WORKBOOK.md: logout (S4),
// opt-in credential check (S6) and opt-in admin route protection (S2).

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
)

const testPassword = "unit-test-password"

// securityRouter returns the production router of a test IoTEdge with the
// given security settings and a non-default password.
func securityRouter(t *testing.T, sec SecurityConfig) (*IoTEdge, http.Handler) {
	t.Helper()
	iot := regressionIoT()
	iot.IoTConfig.Password = testPassword
	iot.IoTConfig.SessionSecret = "unit-test-session-secret-32-bytes!!"
	iot.IoTConfig.Security = sec
	router, err := iot.newRouter()
	if err != nil {
		t.Fatalf("newRouter: %v", err)
	}
	return &iot, router
}

func serve(router http.Handler, method, path string, body any, cookies ...*http.Cookie) *httptest.ResponseRecorder {
	var reader *bytes.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		reader = bytes.NewReader(b)
	} else {
		reader = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, path, reader)
	req.Header.Set("Content-Type", "application/json")
	for _, c := range cookies {
		req.AddCookie(c)
	}
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	return w
}

func sessionCookie(t *testing.T, w *httptest.ResponseRecorder) *http.Cookie {
	t.Helper()
	for _, c := range w.Result().Cookies() {
		if c.Name == sessionCookieName {
			return c
		}
	}
	t.Fatalf("no %s cookie in response (status %d)", sessionCookieName, w.Code)
	return nil
}

func login(t *testing.T, router http.Handler) *http.Cookie {
	t.Helper()
	w := serve(router, http.MethodPost, URILogin, LoginRequest{Password: testPassword})
	if w.Code != http.StatusFound {
		t.Fatalf("login status = %d, want %d", w.Code, http.StatusFound)
	}
	return sessionCookie(t, w)
}

// ── S4 logout ────────────────────────────────────────────────────────────────

func TestSecurityLogoutExpiresSessionCookie(t *testing.T) {
	_, router := securityRouter(t, SecurityConfig{})
	cookie := login(t, router)
	if w := serve(router, http.MethodGet, URIDashboard, nil, cookie); w.Code != http.StatusOK {
		t.Fatalf("dashboard with session = %d, want 200", w.Code)
	}

	w := serve(router, http.MethodPost, URILogout, nil, cookie)
	if w.Code != http.StatusSeeOther || w.Header().Get("Location") != "/" {
		t.Fatalf("logout = %d to %q, want 303 to /", w.Code, w.Header().Get("Location"))
	}
	cleared := sessionCookie(t, w)
	if cleared.MaxAge >= 0 {
		t.Fatalf("logout cookie MaxAge = %d, want < 0", cleared.MaxAge)
	}

	// A browser drops the expired cookie; the next request has no session.
	w = serve(router, http.MethodGet, URIDashboard, nil)
	if w.Code != http.StatusFound {
		t.Fatalf("dashboard after logout = %d, want redirect", w.Code)
	}
}

func TestSecurityDashboardLogoutUsesServer(t *testing.T) {
	data, err := templatesFS.ReadFile("templates/layout.html")
	if err != nil {
		t.Fatal(err)
	}
	page := string(data)
	if strings.Contains(page, "document.cookie") {
		t.Error("dashboard still clears the HttpOnly session cookie from JS")
	}
	if !strings.Contains(page, URILogout) {
		t.Errorf("dashboard does not call %s", URILogout)
	}
}

// ── S6 credentials ───────────────────────────────────────────────────────────

func TestSecurityCredentialCheckIsOptIn(t *testing.T) {
	defaults := []IoTConfig{
		{Password: "", SessionSecret: "unit-test-session-secret-32-bytes!!"},
		{Password: defaultConfigPassword, SessionSecret: "unit-test-session-secret-32-bytes!!"},
		{Password: testPassword, SessionSecret: ""},
		{Password: testPassword, SessionSecret: defaultConfigSessionSecret},
	}
	for _, cfg := range defaults {
		if err := checkCredentials(cfg); err != nil {
			t.Errorf("RequireCredentials off: %+v rejected: %v", cfg, err)
		}
		cfg.Security.RequireCredentials = true
		if err := checkCredentials(cfg); err == nil {
			t.Errorf("RequireCredentials on: password %q / secret %q accepted", cfg.Password, cfg.SessionSecret)
		}
	}
	good := IoTConfig{Password: testPassword, SessionSecret: "unit-test-session-secret-32-bytes!!",
		Security: SecurityConfig{RequireCredentials: true}}
	if err := checkCredentials(good); err != nil {
		t.Errorf("unique credentials rejected: %v", err)
	}
}

func TestSecurityNewRouterRefusesDefaultCredentialsWhenRequired(t *testing.T) {
	iot := regressionIoT()
	iot.IoTConfig.Password = defaultConfigPassword
	iot.IoTConfig.SessionSecret = defaultConfigSessionSecret
	iot.IoTConfig.Security.RequireCredentials = true
	if _, err := iot.newRouter(); err == nil {
		t.Fatal("newRouter started with the shipped default credentials")
	}
}

// ── S2 admin routes ──────────────────────────────────────────────────────────

func TestSecurityAdminRoutesOpenByDefault(t *testing.T) {
	iot, router := securityRouter(t, SecurityConfig{})
	name := "sec-open-" + uuid.NewString()
	if _, err := iot.Init(DeviceDesc{Name: name, Sensors: []string{name + "T"}}); err != nil {
		t.Fatal(err)
	}
	w := serve(router, http.MethodPost, URIDeviceConfigure, ConfigureDeviceReq{Name: name, Interval: 30, Buffer: 3})
	if w.Code != http.StatusOK {
		t.Fatalf("device configure without session = %d, want 200 (protection off)", w.Code)
	}
}

func TestSecurityAdminRoutesRequireSessionWhenEnabled(t *testing.T) {
	iot, router := securityRouter(t, SecurityConfig{ProtectAdminRoutes: true})
	name := "sec-prot-" + uuid.NewString()
	if _, err := iot.Init(DeviceDesc{Name: name, Sensors: []string{name + "T"}}); err != nil {
		t.Fatal(err)
	}
	deviceReq := ConfigureDeviceReq{Name: name, Interval: 30, Buffer: 3}
	sensorReq := ConfigureSensorReq{Name: name, SensorName: name + "T", SensorOffset: 1}

	if w := serve(router, http.MethodPost, URIDeviceConfigure, deviceReq); w.Code != http.StatusUnauthorized {
		t.Errorf("device configure without session = %d, want 401", w.Code)
	}
	if w := serve(router, http.MethodPost, URISensorConfigure, sensorReq); w.Code != http.StatusUnauthorized {
		t.Errorf("sensor configure without session = %d, want 401", w.Code)
	}
	dev, err := iot.DeviceDB.GetDevice(name)
	if err != nil {
		t.Fatal(err)
	}
	if dev.Interval == 30 {
		t.Error("unauthenticated request changed the device interval")
	}

	cookie := login(t, router)
	if w := serve(router, http.MethodPost, URIDeviceConfigure, deviceReq, cookie); w.Code != http.StatusOK {
		t.Errorf("device configure with session = %d, want 200", w.Code)
	}
	if w := serve(router, http.MethodPost, URISensorConfigure, sensorReq, cookie); w.Code != http.StatusOK {
		t.Errorf("sensor configure with session = %d, want 200", w.Code)
	}
}

// Firmware endpoints must keep working without a session, whatever the
// security settings: devices are not updated in Phase 0.
func TestSecurityDeviceEndpointsStayOpen(t *testing.T) {
	_, router := securityRouter(t, SecurityConfig{RequireCredentials: true, ProtectAdminRoutes: true})
	name := "sec-dev-" + uuid.NewString()
	cases := []struct {
		path string
		body any
	}{
		{URIInitDevice, map[string]any{"Device": DeviceDesc{Name: name, Sensors: []string{name + "T"}}}},
		{URISaveTimeseries, []map[string]any{{"Tag": name + "T", "Timestamps": []string{"2026-10-22T19:22:08.499Z"}, "Values": []string{"21.5"}}}},
		{URIUploadData, []map[string]any{{"Tag": name + "T", "Timestamps": []string{"2026-10-22 19:22:09.000"}, "Values": []string{"21.6"}}}},
		{URIUpdateSensor, sensorValues{Data: []TimeSeriesValue{{Name: name + "T", Value: 1}}}},
		{URILogging, map[string]any{"Device": name, "Text": "hello", "Level": 1}},
		{URIMonitoringHealth, DeviceHealthReport{Device: name, BootID: "boot-1", FirmwareVersion: "test"}},
	}
	for _, c := range cases {
		if w := serve(router, http.MethodPost, c.path, c.body); w.Code != http.StatusOK {
			t.Errorf("%s without session = %d (%s), want 200", c.path, w.Code, w.Body.String())
		}
	}
}
