package iotedge

// device_address_test.go — D1 of doc/EDGE_SERVER_WORKBOOK.md: the server
// records the address a device talks from and links it on the dashboard.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func deviceConfig(t *testing.T, iot *IoTEdge, name string) DeviceConfig {
	t.Helper()
	configs, err := iot.DeviceDB.GetDevicesConfigs()
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range configs {
		if c.Name == name {
			return c
		}
	}
	t.Fatalf("device %s not found", name)
	return DeviceConfig{}
}

func TestDeviceAddressRecordedFromDeviceRequests(t *testing.T) {
	iot, router := securityRouter(t, SecurityConfig{})
	name := "addr-" + uuid.NewString()
	requests := []struct {
		path   string
		body   any
		remote string
	}{
		{URIInitDevice, map[string]any{"Device": DeviceDesc{Name: name, Sensors: []string{name + "T"}}}, "192.0.2.10:5000"},
		{URIMonitoringHealth, DeviceHealthReport{Device: name, BootID: "b1", FirmwareVersion: "test"}, "192.0.2.11:5000"},
		{URILogging, map[string]any{"Device": name, "Text": "hi", "Level": 1}, "192.0.2.12:5000"},
	}
	for _, r := range requests {
		if w := serveIngest(router, r.path, r.body, "", r.remote); w.Code != http.StatusOK {
			t.Fatalf("%s = %d (%s)", r.path, w.Code, w.Body.String())
		}
		want := strings.Split(r.remote, ":")[0]
		got := deviceConfig(t, iot, name)
		if got.Address != want {
			t.Errorf("after %s address = %q, want %q", r.path, got.Address, want)
		}
		if got.AddressSeenAt == nil {
			t.Errorf("after %s AddressSeenAt is nil", r.path)
		}
	}
}

// Devices talk to the server directly, so a forwarded-for header must not
// be able to plant a link on the dashboard.
func TestDeviceAddressIgnoresForwardedFor(t *testing.T) {
	iot, router := securityRouter(t, SecurityConfig{})
	name := "addr-xff-" + uuid.NewString()
	req := map[string]any{"Device": DeviceDesc{Name: name}}
	w := serveIngestWithHeader(router, URIInitDevice, req, "192.0.2.20:5000", "X-Forwarded-For", "10.9.9.9")
	if w.Code != http.StatusOK {
		t.Fatalf("init = %d", w.Code)
	}
	if got := deviceConfig(t, iot, name).Address; got != "192.0.2.20" {
		t.Fatalf("address = %q, want 192.0.2.20", got)
	}
}

func TestDeviceConfigURL(t *testing.T) {
	cases := map[string]string{
		"":            "",
		"192.0.2.1":   "http://192.0.2.1/",
		"2001:db8::1": "http://[2001:db8::1]/",
	}
	for address, want := range cases {
		if got := (DeviceConfig{Address: address}).URL(); got != want {
			t.Errorf("URL(%q) = %q, want %q", address, got, want)
		}
	}
}

func TestDashboardLinksDeviceAddress(t *testing.T) {
	_, router := securityRouter(t, SecurityConfig{})
	name := "addr-dash-" + uuid.NewString()
	serveIngest(router, URIInitDevice, map[string]any{"Device": DeviceDesc{Name: name}}, "", "192.0.2.30:5000")
	cookie := login(t, router)
	w := serve(router, http.MethodGet, URIDashboard, nil, cookie)
	if !strings.Contains(w.Body.String(), `href="http://192.0.2.30/"`) {
		t.Fatal("dashboard does not link the device address")
	}
}

func serveIngestWithHeader(router http.Handler, path string, body any, remoteAddr, header, value string) *httptest.ResponseRecorder {
	b, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(string(b)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(header, value)
	req.RemoteAddr = remoteAddr
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	return w
}
