package iotedge

// web_device_test.go — W3 of doc/EDGE_SERVER_WORKBOOK.md: device page with
// health history, restarts and editing (CSRF-protected).

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func ptrOf[T any](v T) *T { return &v }

// pageCSRF loads a page and returns its CSRF token and the (possibly
// renewed) session cookie.
func pageCSRF(t *testing.T, router http.Handler, path string, cookie *http.Cookie) (string, *http.Cookie) {
	t.Helper()
	w := serve(router, http.MethodGet, path, nil, cookie)
	m := csrfMeta.FindStringSubmatch(w.Body.String())
	if m == nil {
		t.Fatalf("no CSRF token on %s (status %d)", path, w.Code)
	}
	for _, c := range w.Result().Cookies() {
		if c.Name == sessionCookieName {
			cookie = c
		}
	}
	return m[1], cookie
}

func putForm(router http.Handler, path string, form url.Values, csrf string, cookie *http.Cookie) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPut, path, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("HX-Request", "true")
	if csrf != "" {
		req.Header.Set("X-CSRF-Token", csrf)
	}
	if cookie != nil {
		req.AddCookie(cookie)
	}
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	return w
}

func TestWebDevicePageShowsHealthHistoryAndRestarts(t *testing.T) {
	iot, router := securityRouter(t, SecurityConfig{})
	name := "web-detail-" + uuid.NewString()
	serveIngest(router, URIInitDevice, map[string]any{"Device": DeviceDesc{Name: name, Sensors: []string{name + "T"}}}, "", "192.0.2.60:1")
	now := time.Now()
	for i, rssi := range []int{-61, -62, -63} {
		r := DeviceHealthReport{Device: name, BootID: "boot-a", FirmwareVersion: "fw-1",
			WiFiRSSI: ptrOf(rssi), FreeHeapBytes: ptrOf(int64(200000)), UptimeSeconds: ptrOf(int64(3600 + i*60)),
			TimeSynced: ptrOf(true)}
		if err := iot.DeviceDB.RecordDeviceHealth(r, now.Add(time.Duration(i-3)*time.Minute)); err != nil {
			t.Fatal(err)
		}
	}
	cookie := login(t, router)

	w := serve(router, http.MethodGet, URIDashboard+"/devices/"+name, nil, cookie)
	if w.Code != http.StatusOK {
		t.Fatalf("GET device page = %d", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, "</html>") {
		t.Fatal("device page is cut off: the template failed while rendering")
	}
	for _, want := range []string{name, `href="http://192.0.2.60/"`, "fw-1", "boot-a", name + "T", `name="Interval"`, `name="SensorOffset"`, "Synced", "-63 dBm"} {
		if !strings.Contains(body, want) {
			t.Errorf("device page lacks %q", want)
		}
	}
	m := regexp.MustCompile(`<script type="application/json" id="health-data">(.*?)</script>`).FindStringSubmatch(body)
	if m == nil {
		t.Fatal("device page has no health chart data")
	}
	var data struct {
		Charts []struct {
			Key    string
			Points [][2]float64
		}
	}
	if err := json.Unmarshal([]byte(m[1]), &data); err != nil {
		t.Fatalf("chart data: %v", err)
	}
	var rssi []float64
	for _, c := range data.Charts {
		if c.Key == "rssi" {
			for _, p := range c.Points {
				rssi = append(rssi, p[1])
			}
		}
	}
	if len(rssi) != 3 || rssi[0] != -61 || rssi[2] != -63 {
		t.Errorf("RSSI chart points = %v, want [-61 -62 -63]", rssi)
	}

	if w := serve(router, http.MethodGet, URIDashboard+"/devices/no-such-"+name, nil, cookie); w.Code != http.StatusNotFound {
		t.Errorf("unknown device page = %d, want 404", w.Code)
	}
	if w := serve(router, http.MethodGet, URIDashboard+"/devices/"+name+"?range=bogus", nil, cookie); w.Code != http.StatusOK {
		t.Errorf("unknown range = %d, want 200 with the default range", w.Code)
	}
	if w := serve(router, http.MethodGet, URIDashboard+"/devices/"+name, nil); w.Code != http.StatusFound {
		t.Errorf("device page without login = %d, want redirect", w.Code)
	}
}

func TestWebDeviceEditingNeedsTheCSRFToken(t *testing.T) {
	iot, router := securityRouter(t, SecurityConfig{})
	name := "web-edit-" + uuid.NewString()
	sensor := name + "T"
	serveIngest(router, URIInitDevice, map[string]any{"Device": DeviceDesc{Name: name, Sensors: []string{sensor}}}, "", "192.0.2.61:1")
	cookie := login(t, router)
	devicePath := URIDashboard + "/devices/" + name
	csrf, cookie := pageCSRF(t, router, devicePath, cookie)

	form := url.Values{"Interval": {"45"}, "Buffer": {"4"}}
	if w := putForm(router, devicePath, form, "", cookie); w.Code != http.StatusForbidden {
		t.Errorf("PUT without CSRF token = %d, want 403", w.Code)
	}
	if w := putForm(router, devicePath, form, "wrong", cookie); w.Code != http.StatusForbidden {
		t.Errorf("PUT with wrong CSRF token = %d, want 403", w.Code)
	}
	if w := putForm(router, devicePath, form, csrf, cookie); w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "Saved") {
		t.Fatalf("PUT with CSRF token = %d (%s)", w.Code, w.Body.String())
	}
	dev, err := iot.DeviceDB.GetDevice(name)
	if err != nil || dev.Interval != 45 || dev.Buffer != 4 {
		t.Fatalf("device after edit = %+v (%v)", dev, err)
	}
	if w := putForm(router, devicePath, url.Values{"Interval": {"x"}, "Buffer": {"4"}}, csrf, cookie); w.Code != http.StatusBadRequest {
		t.Errorf("PUT with a bad interval = %d, want 400", w.Code)
	}

	sensorPath := devicePath + "/sensors/" + sensor
	if w := putForm(router, sensorPath, url.Values{"SensorOffset": {"-1.5"}}, "", cookie); w.Code != http.StatusForbidden {
		t.Errorf("sensor PUT without CSRF token = %d, want 403", w.Code)
	}
	if w := putForm(router, sensorPath, url.Values{"SensorOffset": {"-1.5"}}, csrf, cookie); w.Code != http.StatusOK {
		t.Fatalf("sensor PUT = %d (%s)", w.Code, w.Body.String())
	}
	sensors, _ := iot.DeviceDB.GetSensors(dev.ID)
	if len(sensors) != 1 || sensors[0].SensorOffset != -1.5 {
		t.Fatalf("sensor after edit = %+v", sensors)
	}
}

func TestDownsampleKeepsShapeWithinTheLimit(t *testing.T) {
	start := time.Date(2026, 10, 24, 0, 0, 0, 0, time.UTC)
	var points []chartPoint
	for i := 0; i < 10000; i++ {
		points = append(points, chartPoint{At: start.Add(time.Duration(i) * time.Second), Value: float64(i % 100)})
	}
	got := downsample(points, 300)
	if len(got) > 300 || len(got) < 250 {
		t.Fatalf("downsampled to %d points, want about 300", len(got))
	}
	for i := 1; i < len(got); i++ {
		if !got[i].At.After(got[i-1].At) {
			t.Fatalf("points out of order at %d", i)
		}
	}
	if few := downsample(points[:5], 300); len(few) != 5 {
		t.Errorf("short series changed: %d points", len(few))
	}
}
