package iotedge

// api_v1_test.go — A5 of doc/EDGE_SERVER_WORKBOOK.md: versioned JSON API
// under /api/v1 with an OpenAPI spec; the firmware's paths stay as aliases.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"github.com/google/uuid"
	"go.yaml.in/yaml/v3"
)

func decode[T any](t *testing.T, w *httptest.ResponseRecorder) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(w.Body.Bytes(), &v); err != nil {
		t.Fatalf("decode %q: %v", w.Body.String(), err)
	}
	return v
}

func TestAPIv1DeviceEndpointsStoreLikeTheFirmwarePaths(t *testing.T) {
	iot, router := securityRouter(t, SecurityConfig{})
	name := "v1-dev-" + uuid.NewString()
	tag := name + "T"
	requests := []struct {
		path string
		body any
	}{
		{"/api/v1/devices", map[string]any{"Device": DeviceDesc{Name: name, Sensors: []string{tag}}}},
		{"/api/v1/measurements", []map[string]any{{"Tag": tag, "Timestamps": []string{"2026-10-24T10:00:00.000Z"}, "Values": []string{"1"}}}},
		{"/api/v1/readings", sensorValues{Data: []TimeSeriesValue{{Name: tag, Value: 2}}}},
		{"/api/v1/logs", LogMessage{Device: name, Text: "v1 log", Level: Info}},
		{"/api/v1/health", DeviceHealthReport{Device: name, BootID: "b1", FirmwareVersion: "fw"}},
	}
	for _, r := range requests {
		if w := serve(router, http.MethodPost, r.path, r.body); w.Code != http.StatusOK {
			t.Fatalf("POST %s = %d (%s)", r.path, w.Code, w.Body.String())
		}
	}
	var n int
	if err := iot.DeviceDB.DB.QueryRow(iot.DeviceDB.Rebind(
		"SELECT COUNT(*) FROM "+iot.IoTConfig.TimeseriesTable+" WHERE tag = ?"), tag).Scan(&n); err != nil || n != 2 {
		t.Fatalf("stored readings for %s = %d (%v), want 2", tag, n, err)
	}
}

func TestAPIv1DeviceEndpointsUseTheIngestToken(t *testing.T) {
	_, router := securityRouter(t, SecurityConfig{IngestAuth: IngestAuthEnforce, IngestToken: testIngestToken})
	for _, path := range []string{"/api/v1/devices", "/api/v1/measurements", "/api/v1/readings", "/api/v1/logs", "/api/v1/health"} {
		if w := serveIngest(router, path, map[string]any{}, "", "192.0.2.40:1"); w.Code != http.StatusUnauthorized {
			t.Errorf("POST %s without token = %d, want 401", path, w.Code)
		}
	}
}

func TestAPIv1AdminEndpointsRequireLogin(t *testing.T) {
	_, router := securityRouter(t, SecurityConfig{})
	for _, r := range []struct{ method, path string }{
		{http.MethodGet, "/api/v1/devices"},
		{http.MethodPut, "/api/v1/devices/x"},
		{http.MethodPut, "/api/v1/devices/x/sensors/y"},
		{http.MethodGet, "/api/v1/logs"},
		{http.MethodGet, "/api/v1/monitoring"},
	} {
		if w := serve(router, r.method, r.path, map[string]any{}); w.Code != http.StatusUnauthorized {
			t.Errorf("%s %s without login = %d, want 401", r.method, r.path, w.Code)
		}
	}
}

func TestAPIv1AdminEndpoints(t *testing.T) {
	_, router := securityRouter(t, SecurityConfig{})
	name := "v1-admin-" + uuid.NewString()
	sensor := name + "T"
	serveIngest(router, "/api/v1/devices", map[string]any{"Device": DeviceDesc{Name: name, Sensors: []string{sensor}}}, "", "192.0.2.41:1")
	serveIngest(router, "/api/v1/logs", LogMessage{Device: name, Text: "hello v1", Level: Warning}, "", "192.0.2.41:1")
	cookie := login(t, router)

	w := serve(router, http.MethodPut, "/api/v1/devices/"+name, map[string]any{"Interval": 45, "Buffer": 4}, cookie)
	if w.Code != http.StatusOK {
		t.Fatalf("PUT device = %d (%s)", w.Code, w.Body.String())
	}
	w = serve(router, http.MethodPut, "/api/v1/devices/"+name+"/sensors/"+sensor, map[string]any{"SensorOffset": -1.5}, cookie)
	if w.Code != http.StatusOK {
		t.Fatalf("PUT sensor = %d (%s)", w.Code, w.Body.String())
	}
	if w := serve(router, http.MethodPut, "/api/v1/devices/no-such-"+name, map[string]any{"Interval": 1, "Buffer": 1}, cookie); w.Code != http.StatusNotFound {
		t.Errorf("PUT unknown device = %d, want 404", w.Code)
	}
	if w := serve(router, http.MethodPut, "/api/v1/devices/"+name+"/sensors/nope", map[string]any{"SensorOffset": 1}, cookie); w.Code != http.StatusNotFound {
		t.Errorf("PUT unknown sensor = %d, want 404", w.Code)
	}

	w = serve(router, http.MethodGet, "/api/v1/devices", nil, cookie)
	devices := decode[[]DeviceConfig](t, w)
	var found *DeviceConfig
	for i := range devices {
		if devices[i].Name == name {
			found = &devices[i]
		}
	}
	if found == nil || found.Interval != 45 || found.Buffer != 4 || found.Address != "192.0.2.41" ||
		len(found.Sensors) != 1 || found.Sensors[0].SensorOffset != -1.5 {
		t.Fatalf("GET devices: %+v", found)
	}

	w = serve(router, http.MethodGet, "/api/v1/logs?device="+name+"&level=2", nil, cookie)
	logs := decode[[]LogMessage](t, w)
	if len(logs) != 1 || logs[0].Text != "hello v1" {
		t.Fatalf("GET logs = %+v", logs)
	}
	if w := serve(router, http.MethodGet, "/api/v1/logs?level=9", nil, cookie); w.Code != http.StatusBadRequest {
		t.Errorf("GET logs with bad level = %d, want 400", w.Code)
	}

	w = serve(router, http.MethodGet, "/api/v1/monitoring", nil, cookie)
	summary := decode[MonitoringSummary](t, w)
	if summary.OnlineWindowMinutes <= 0 || len(summary.Devices) == 0 {
		t.Fatalf("GET monitoring = %+v", summary)
	}
	if w := serve(router, http.MethodGet, "/api/v1/version", nil); w.Code != http.StatusOK {
		t.Errorf("GET version = %d", w.Code)
	}
}

// The firmware in the field uses the old paths; they stay.
func TestAPIv1KeepsTheFirmwarePaths(t *testing.T) {
	iot := regressionIoT()
	router, err := iot.newRouter()
	if err != nil {
		t.Fatal(err)
	}
	have := map[string]bool{}
	for _, r := range router.Routes() {
		have[r.Method+" "+r.Path] = true
	}
	for _, p := range []string{URIInitDevice, URISaveTimeseries, URIUploadData, URIUpdateSensor, URILogging,
		URIMonitoringHealth, URIDeviceConfigure, URISensorConfigure, URIVersion} {
		method := http.MethodPost
		if p == URIVersion {
			method = http.MethodGet
		}
		if !have[method+" "+p] {
			t.Errorf("route %s %s is gone", method, p)
		}
	}
}

// The spec documents exactly the /api/v1 routes the router serves.
func TestAPIv1OpenAPISpecMatchesTheRouter(t *testing.T) {
	iot := regressionIoT()
	router, err := iot.newRouter()
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/openapi.yaml", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("GET openapi.yaml = %d", w.Code)
	}
	var spec struct {
		OpenAPI string                    `yaml:"openapi"`
		Paths   map[string]map[string]any `yaml:"paths"`
	}
	if err := yaml.Unmarshal(w.Body.Bytes(), &spec); err != nil {
		t.Fatalf("spec is not YAML: %v", err)
	}
	if !strings.HasPrefix(spec.OpenAPI, "3.") {
		t.Errorf("openapi = %q, want 3.x", spec.OpenAPI)
	}
	documented := []string{}
	for path, methods := range spec.Paths {
		for method := range methods {
			// /devices/{name} in the spec is /devices/:name in gin.
			documented = append(documented, strings.ToUpper(method)+" /api/v1"+ginPath(path))
		}
	}
	served := []string{}
	for _, r := range router.Routes() {
		if strings.HasPrefix(r.Path, "/api/v1/") && r.Path != "/api/v1/openapi.yaml" {
			served = append(served, r.Method+" "+r.Path)
		}
	}
	sort.Strings(documented)
	sort.Strings(served)
	if strings.Join(documented, "\n") != strings.Join(served, "\n") {
		t.Fatalf("spec and router differ:\nspec:\n%s\nrouter:\n%s", strings.Join(documented, "\n"), strings.Join(served, "\n"))
	}
}

func ginPath(openapiPath string) string {
	parts := strings.Split(openapiPath, "/")
	for i, p := range parts {
		if strings.HasPrefix(p, "{") && strings.HasSuffix(p, "}") {
			parts[i] = ":" + strings.Trim(p, "{}")
		}
	}
	return strings.Join(parts, "/")
}
