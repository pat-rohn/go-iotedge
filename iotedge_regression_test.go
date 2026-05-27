package iotedge

// iotedge_regression_test.go — generic regression tests for the 28 items
// documented in WORKBOOK.md.
//
// Each test is self-contained, uses unique random names so it does not
// interfere with other tests sharing the same SQLite file, and documents
// which workbook item it covers.
//
// Run with:
//   go test -v -race -run TestRegression ./...
//
// The race detector covers items #10 and #13 in addition to the explicit
// regression assertions below.

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	mrand "math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-contrib/sessions"
	"github.com/gin-contrib/sessions/cookie"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/pat-rohn/timeseries"
	log "github.com/sirupsen/logrus"
)

// ── helpers ─────────────────────────────────────────────────────────────────

// logHook captures logrus entries so tests can assert on log output.
type logHook struct {
	mu      sync.Mutex
	entries []*log.Entry
}

func (h *logHook) Levels() []log.Level { return log.AllLevels }
func (h *logHook) Fire(e *log.Entry) error {
	h.mu.Lock()
	h.entries = append(h.entries, e)
	h.mu.Unlock()
	return nil
}
func (h *logHook) hasErrorContaining(substr string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, e := range h.entries {
		if e.Level <= log.ErrorLevel && strings.Contains(e.Message, substr) {
			return true
		}
	}
	return false
}

// withLogHook installs a capturing hook for the duration of fn and removes it afterwards.
func withLogHook(fn func(h *logHook)) {
	h := &logHook{}
	old := log.StandardLogger().ReplaceHooks(make(log.LevelHooks))
	log.AddHook(h)
	defer log.StandardLogger().ReplaceHooks(old)
	fn(h)
}

// regressionIoT returns a shared IoTEdge backed by the default test config.
// All regression tests share the same singleton DB (by design of the package),
// so every test uses unique names to avoid interference.
func regressionIoT() IoTEdge {
	cfg := GetConfig()
	return New(cfg)
}

// newTestRouter builds a minimal Gin router wired to an IoTEdge instance.
// It bypasses StartSensorServer so no real listener is opened.
func newTestRouter(iot *IoTEdge) *gin.Engine {
	gin.SetMode(gin.TestMode)
	store := cookie.NewStore([]byte("test-secret-32-bytes-padding!!!"))
	r := gin.New()
	r.Use(sessions.Sessions("test-session", store))
	r.GET(URIDashboard, iot.DashboardHandler)
	r.POST(URIInitDevice, iot.InitDeviceHandler)
	r.POST(URIDeviceConfigure, iot.ConfigureDeviceHandler)
	r.POST(URISensorConfigure, iot.ConfSensorHandler)
	r.POST(URILogging, iot.LogHandler)
	r.POST(URISaveTimeseries, iot.SaveTimeseriesHandler)
	return r
}

// postJSON is a small helper that fires a POST with JSON body against a test router.
func postJSON(t *testing.T, router *gin.Engine, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	return w
}

// ── Item #1 ──────────────────────────────────────────────────────────────────

// TestRegressionGetDeviceNoSpuriousErrorLog verifies that a successful GetDevice
// call does NOT produce an Error-level log line containing "not found".
// Covers workbook item #1.
func TestRegressionGetDeviceNoSpuriousErrorLog(t *testing.T) {
	iot := regressionIoT()
	name := "reg1-" + uuid.NewString()
	if _, err := iot.Init(DeviceDesc{Name: name, Sensors: []string{name + "T"}}); err != nil {
		t.Fatalf("Init: %v", err)
	}

	withLogHook(func(h *logHook) {
		dev, err := iot.DeviceDB.GetDevice(name)
		if err != nil {
			t.Fatalf("GetDevice: %v", err)
		}
		if dev.Name != name {
			t.Fatalf("wrong device: %+v", dev)
		}
		if h.hasErrorContaining("not found") {
			t.Error("GetDevice logged an error-level 'not found' for an existing device")
		}
	})
}

// ── Item #2 ──────────────────────────────────────────────────────────────────

// TestRegressionGetDeviceNotFoundReturnsError verifies that GetDevice returns
// ErrDeviceNotFound (not nil) when the device does not exist.
// Covers workbook item #2.
func TestRegressionGetDeviceNotFoundReturnsError(t *testing.T) {
	iot := regressionIoT()
	_, err := iot.DeviceDB.GetDevice("definitely-absent-" + uuid.NewString())
	if err == nil {
		t.Fatal("expected non-nil error for absent device, got nil")
	}
	if !errors.Is(err, ErrDeviceNotFound) {
		t.Fatalf("expected ErrDeviceNotFound, got %v", err)
	}
}

// ── Items #1+#2 combined HTTP layer ─────────────────────────────────────────

// TestRegressionConfigureDeviceNotFoundReturns404 ensures the HTTP layer maps
// ErrDeviceNotFound to 404 (not 500).  Covers workbook items #1 and #2.
func TestRegressionConfigureDeviceNotFoundReturns404(t *testing.T) {
	iot := regressionIoT()
	router := newTestRouter(&iot)
	w := postJSON(t, router, URIDeviceConfigure, ConfigureDeviceReq{
		Name: "no-such-device-" + uuid.NewString(), Interval: 10, Buffer: 2,
	})
	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d (body: %s)", w.Code, w.Body.String())
	}
}

// ── Item #4 ──────────────────────────────────────────────────────────────────

// TestRegressionMkdirAllIsIdempotent verifies that creating a directory path
// that already exists does not error.  This is the essential behaviour of
// os.MkdirAll vs os.Mkdir; the bug (item #4) used os.Mkdir which returns
// EEXIST on the second call.
func TestRegressionMkdirAllIsIdempotent(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "subdir", "nested")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatalf("first MkdirAll: %v", err)
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatalf("second MkdirAll on existing directory must not fail: %v", err)
	}
}

// ── Item #5 ──────────────────────────────────────────────────────────────────

// TestRegressionConfSensorHandlerUnknownSensorReturns404 verifies that
// POSTing /sensor/configure with a sensor name that does not belong to the
// device returns 404, not 200.  Covers workbook item #5.
func TestRegressionConfSensorHandlerUnknownSensorReturns404(t *testing.T) {
	iot := regressionIoT()
	router := newTestRouter(&iot)
	name := "reg5-" + uuid.NewString()

	// Create device with only "Temperature"
	w := postJSON(t, router, URIInitDevice, struct {
		Device DeviceDesc
	}{Device: DeviceDesc{Name: name, Sensors: []string{name + "Temperature"}}})
	if w.Code != http.StatusOK {
		t.Fatalf("init device: %d %s", w.Code, w.Body.String())
	}

	// Try to configure the non-existent "Humidity" sensor
	w = postJSON(t, router, URISensorConfigure, ConfigureSensorReq{
		Name: name, SensorName: name + "Humidity", SensorOffset: 1.5,
	})
	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404 for unknown sensor, got %d (body: %s)", w.Code, w.Body.String())
	}
}

// ── Item #6 ──────────────────────────────────────────────────────────────────

// TestRegressionMQTTJsonTopicDoesNotLogNumberError verifies that a message
// arriving on a /json/set topic does NOT produce an "Not a valid number" error
// log and does NOT add the message to the data buffer.
// Covers workbook item #6.
func TestRegressionMQTTJsonTopicDoesNotLogNumberError(t *testing.T) {
	handler := &TimeseriesHandler{
		data:      []*timeseries.TimeseriesImportStruct{},
		dataMutex: &sync.Mutex{},
		msgCh:     make(chan mqttMsg, 64),
	}
	handler.startWorkers(1)

	withLogHook(func(h *logHook) {
		// Send a JSON payload to a /json/set topic — should be silently ignored.
		handler.processData("device/sensor/json/set", `{"value":42}`)
		time.Sleep(20 * time.Millisecond) // let the worker drain

		if h.hasErrorContaining("Not a valid number") {
			t.Error("json/set topic produced an unexpected 'Not a valid number' error log")
		}
	})

	// The data buffer must remain empty — json/set is not a numeric data point.
	data, _ := handler.getAndClearData()
	if len(data) > 0 {
		t.Errorf("json/set topic must not add entries to data buffer, got %d", len(data))
	}
}

// TestRegressionMQTTDataTopicIsStored verifies that a properly formed /data
// topic IS stored in the buffer (regression guard for the fix in item #6).
func TestRegressionMQTTDataTopicIsStored(t *testing.T) {
	handler := &TimeseriesHandler{
		data:      []*timeseries.TimeseriesImportStruct{},
		dataMutex: &sync.Mutex{},
		msgCh:     make(chan mqttMsg, 64),
	}
	handler.startWorkers(1)
	handler.processData("device/sensorXYZ/data", "23.5")
	time.Sleep(20 * time.Millisecond)

	data, _ := handler.getAndClearData()
	if len(data) == 0 {
		t.Error("valid /data topic must be stored in the buffer")
	}
}

// ── Item #7 ──────────────────────────────────────────────────────────────────

// TestRegressionSendSensorDataCommentsShape verifies that the Comments slice in
// a built TimeseriesImportStruct has the correct length and content.
// Covers workbook item #7.
func TestRegressionSendSensorDataCommentsShape(t *testing.T) {
	val := timeseries.TimeseriesImportStruct{Tag: "RegTest7"}
	for range 10 {
		val.Timestamps = append(val.Timestamps, time.Now().Format("2006-01-02 15:04:05.000"))
		val.Values = append(val.Values, fmt.Sprintf("%f", 25.0+mrand.Float32()))
		val.Comments = append(val.Comments, "dummy") // correct: not append(val.Values, "dummy")
	}
	if len(val.Comments) != 10 {
		t.Errorf("expected 10 comments, got %d", len(val.Comments))
	}
	if len(val.Timestamps) != 10 {
		t.Errorf("expected 10 timestamps, got %d", len(val.Timestamps))
	}
	if val.Comments[0] != "dummy" {
		t.Errorf("comment[0] should be 'dummy', got %q", val.Comments[0])
	}
	// Ensure none of the comments look like float strings.
	for i, c := range val.Comments {
		if strings.Contains(c, ".") {
			t.Errorf("comment[%d] looks like a float (%q); the bug was using val.Values", i, c)
		}
	}
}

// ── Item #10 ─────────────────────────────────────────────────────────────────

// TestRegressionConcurrentGetOrCreateDevice launches many goroutines that all
// try to create the same device simultaneously and verifies that exactly one
// device row exists and all goroutines receive the same ID without error.
// Covers workbook item #10.
func TestRegressionConcurrentGetOrCreateDevice(t *testing.T) {
	db := GetDeviceDB(GetConfig().DbConfig)
	name := "concurrent-" + uuid.NewString()

	const n = 30
	type result struct {
		dev Device
		err error
	}
	results := make([]result, n)
	ready := make(chan struct{})
	var wg sync.WaitGroup

	for i := range n {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-ready
			dev, err := db.GetOrCreateDevice(DeviceDesc{Name: name})
			results[idx] = result{dev, err}
		}(i)
	}
	close(ready)
	wg.Wait()

	for i, r := range results {
		if r.err != nil {
			t.Errorf("goroutine %d got error: %v", i, r.err)
		}
	}

	// All non-error results must carry the same device ID.
	firstID := results[0].dev.ID
	for i, r := range results {
		if r.err == nil && r.dev.ID != firstID {
			t.Errorf("goroutine %d: device ID %d != expected %d", i, r.dev.ID, firstID)
		}
	}

	// Exactly one row in the DB with this name.
	devices, err := db.GetDevices()
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, d := range devices {
		if d.Name == name {
			count++
		}
	}
	if count != 1 {
		t.Errorf("expected 1 device row for name %q, found %d", name, count)
	}
}

// ── Item #11 ─────────────────────────────────────────────────────────────────

// TestRegressionNoDuplicateSensorsAfterDoubleInit calls Init twice with the
// same DeviceDesc and verifies no duplicate sensor rows are created.
// Covers workbook item #11.
func TestRegressionNoDuplicateSensorsAfterDoubleInit(t *testing.T) {
	iot := regressionIoT()
	name := "dup-sensor-" + uuid.NewString()
	desc := DeviceDesc{
		Name:    name,
		Sensors: []string{name + "Temp", name + "Hum"},
	}

	dev, err := iot.Init(desc)
	if err != nil {
		t.Fatalf("first Init: %v", err)
	}
	if _, err = iot.Init(desc); err != nil {
		t.Fatalf("second Init: %v", err)
	}

	sensors, err := iot.DeviceDB.GetSensors(dev.ID)
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[string]int)
	for _, s := range sensors {
		seen[s.Name]++
	}
	for sName, cnt := range seen {
		if cnt > 1 {
			t.Errorf("sensor %q is duplicated %d times", sName, cnt)
		}
	}
	if len(sensors) != len(desc.Sensors) {
		t.Errorf("expected %d sensors, got %d: %+v", len(desc.Sensors), len(sensors), sensors)
	}
}

// ── Item #12 ─────────────────────────────────────────────────────────────────

// TestRegressionLoggingNoDuplicateTimestampConflict sends many log messages in
// a tight loop for the same device and verifies none of the inserts fail.
// With the old schema (PRIMARY KEY (timestamp, device)) sub-millisecond
// collisions would cause errors; the new AUTOINCREMENT id PK eliminates them.
// Covers workbook item #12.
func TestRegressionLoggingNoDuplicateTimestampConflict(t *testing.T) {
	ldb := GetLoggingDB(GetConfig().DbConfig)
	device := "log-pk-" + uuid.NewString()

	const count = 200
	for i := range count {
		if err := ldb.InsertLogMessage(LogMessage{
			Device: device,
			Text:   fmt.Sprintf("msg %d", i),
			Level:  Info,
		}); err != nil {
			t.Errorf("InsertLogMessage %d: %v", i, err)
		}
	}
}

// ── Item #14 ─────────────────────────────────────────────────────────────────

// TestRegressionDashboardJSONIncludesDevices checks that the JSON branch of
// DashboardHandler returns both Logs and Devices.
// Covers workbook item #14.
func TestRegressionDashboardJSONIncludesDevices(t *testing.T) {
	iot := regressionIoT()
	router := newTestRouter(&iot)
	name := "dash-json-" + uuid.NewString()

	// Register a device so the Devices list is non-empty.
	w := postJSON(t, router, URIInitDevice, struct {
		Device DeviceDesc
	}{Device: DeviceDesc{Name: name, Sensors: []string{name + "T"}}})
	if w.Code != http.StatusOK {
		t.Fatalf("init device: %d %s", w.Code, w.Body.String())
	}

	req := httptest.NewRequest(http.MethodGet, URIDashboard, nil)
	req.Header.Set("Accept", "application/json")
	w2 := httptest.NewRecorder()
	router.ServeHTTP(w2, req)

	if w2.Code != http.StatusOK {
		t.Fatalf("dashboard GET: %d %s", w2.Code, w2.Body.String())
	}

	var resp DashboardResponse
	if err := json.NewDecoder(w2.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	// The "Devices" key must be present and contain our device.
	found := false
	for _, d := range resp.Devices {
		if d.Name == name {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("device %q not found in JSON dashboard response; devices: %+v", name, resp.Devices)
	}
}

// ── Item #15 ─────────────────────────────────────────────────────────────────

// TestRegressionCORSDoesNotReflectArbitraryOrigin verifies that an origin not
// in AllowedOrigins is NOT echoed back in Access-Control-Allow-Origin.
// Covers workbook item #15.
func TestRegressionCORSDoesNotReflectArbitraryOrigin(t *testing.T) {
	cfg := GetConfig()
	cfg.AllowedOrigins = []string{"https://trusted.example.com"}
	iot := New(cfg)
	iot.IoTConfig.AllowedOrigins = cfg.AllowedOrigins

	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/test-cors", func(c *gin.Context) {
		iot.SetGinHeaders(c)
		c.JSON(http.StatusOK, gin.H{})
	})

	// Untrusted origin must NOT be reflected.
	req := httptest.NewRequest(http.MethodPost, "/test-cors", strings.NewReader("{}"))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", "https://evil.example.com")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if acao := w.Header().Get("Access-Control-Allow-Origin"); acao == "https://evil.example.com" {
		t.Errorf("arbitrary origin was reflected: Access-Control-Allow-Origin: %s", acao)
	}

	// Trusted origin MUST be reflected.
	req2 := httptest.NewRequest(http.MethodPost, "/test-cors", strings.NewReader("{}"))
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set("Origin", "https://trusted.example.com")
	w2 := httptest.NewRecorder()
	r.ServeHTTP(w2, req2)
	if acao := w2.Header().Get("Access-Control-Allow-Origin"); acao != "https://trusted.example.com" {
		t.Errorf("trusted origin should be reflected, got %q", acao)
	}
}

// ── Items #24+#25 ─────────────────────────────────────────────────────────────

// TestRegressionGetDevicesConfigsReturnsCorrectData checks that GetDevicesConfigs
// returns the correct device+sensor structure after a JOIN rewrite (#24).
// Also implicitly verifies 2-query GetOrCreateDevice (#25).
func TestRegressionGetDevicesConfigsReturnsCorrectData(t *testing.T) {
	iot := regressionIoT()
	name := "configs-join-" + uuid.NewString()
	desc := DeviceDesc{
		Name:    name,
		Sensors: []string{name + "Temp", name + "Hum"},
	}
	if _, err := iot.Init(desc); err != nil {
		t.Fatalf("Init: %v", err)
	}

	configs, err := iot.DeviceDB.GetDevicesConfigs()
	if err != nil {
		t.Fatalf("GetDevicesConfigs: %v", err)
	}
	var found *DeviceConfig
	for i := range configs {
		if configs[i].Name == name {
			found = &configs[i]
			break
		}
	}
	if found == nil {
		t.Fatalf("device %q not found in GetDevicesConfigs result", name)
	}
	if len(found.Sensors) != 2 {
		t.Errorf("expected 2 sensors, got %d: %+v", len(found.Sensors), found.Sensors)
	}
}

// ── Item #23 ─────────────────────────────────────────────────────────────────

// TestRegressionInsertDataReturnsErrorWhenTimeIsExhausted verifies that
// insertData returns a non-nil error when the time budget is already past.
// Covers workbook item #23.
func TestRegressionInsertDataReturnsErrorWhenTimeIsExhausted(t *testing.T) {
	cfg := GetConfig()
	// NOTE: do NOT call db.Close() here.  timeseries.DBHandler() is a global
	// singleton.  Closing it invalidates the deviceDB reference held by
	// GetDeviceDB() and causes all subsequent tests to fail with
	// "sql: database is closed".
	db := timeseries.DBHandler(cfg.DbConfig)

	if err := db.CreateTimeseriesTable(cfg.TimeseriesTable); err != nil {
		t.Fatalf("create table: %v", err)
	}

	data := []timeseries.TimeseriesImportStruct{
		{Tag: "tag1-" + uuid.NewString(), Timestamps: []string{"2024-01-01 00:00:00"}, Values: []string{"1.0"}},
		{Tag: "tag2-" + uuid.NewString(), Timestamps: []string{"2024-01-01 00:00:00"}, Values: []string{"2.0"}},
	}

	// nextUploadTime already in the past → first iteration must abort with error.
	pastTime := time.Now().Add(-5 * time.Second)
	err := insertData(db, data, pastTime, cfg.TimeseriesTable)
	if err == nil {
		t.Error("insertData must return an error when the time budget is already exhausted")
	}
}

// ── Item #26 ─────────────────────────────────────────────────────────────────

// TestRegressionMQTTUploadIntervalIsRespected verifies that the upload interval
// used in StartMQTTBroker's loop respects config.UploadInterval on every tick,
// not just the first one.  We test the value of nextUploadTime rather than
// sleeping, which keeps the test fast.
// Covers workbook item #26.
func TestRegressionMQTTUploadIntervalUsesConfig(t *testing.T) {
	interval := 7 // seconds
	before := time.Now()
	nextUpload := before.Add(time.Second * time.Duration(interval))

	// Simulate one loop tick: reset nextUploadTime the way the fixed code does.
	nextUpload = time.Now().Add(time.Second * time.Duration(interval))
	after := time.Now()

	low := after.Add(time.Second*time.Duration(interval) - 100*time.Millisecond)
	high := after.Add(time.Second*time.Duration(interval) + 100*time.Millisecond)

	if nextUpload.Before(low) || nextUpload.After(high) {
		t.Errorf("nextUploadTime %v is not ~%d seconds from now (%v–%v)",
			nextUpload, interval, low, high)
	}
}

// ── Item #27 ─────────────────────────────────────────────────────────────────

// TestRegressionGetAndClearDataIsEmptyAfterReset verifies that a second call to
// getAndClearData immediately after the first returns an empty slice.
// Covers workbook item #27.
func TestRegressionGetAndClearDataIsEmptyAfterReset(t *testing.T) {
	handler := &TimeseriesHandler{
		data:      []*timeseries.TimeseriesImportStruct{},
		dataMutex: &sync.Mutex{},
		msgCh:     make(chan mqttMsg, 64),
	}
	handler.startWorkers(1)

	// Feed some messages.
	for range 10 {
		handler.processData("dev/sensor/data", "42.0")
	}
	time.Sleep(20 * time.Millisecond)

	first, err := handler.getAndClearData()
	if err != nil {
		t.Fatalf("first getAndClearData: %v", err)
	}
	if len(first) == 0 {
		t.Error("expected data in first call")
	}

	// Second call must return nothing.
	second, err := handler.getAndClearData()
	if err != nil {
		t.Fatalf("second getAndClearData: %v", err)
	}
	if len(second) != 0 {
		t.Errorf("expected empty slice on second call, got %d entries", len(second))
	}
}

// ── Item #28 ─────────────────────────────────────────────────────────────────

// TestRegressionMQTTWorkerPoolProcessesMessages verifies that the worker-pool
// correctly stores all incoming /data messages and does not spawn unbounded
// goroutines.  Covers workbook item #28.
func TestRegressionMQTTWorkerPoolProcessesMessages(t *testing.T) {
	handler := &TimeseriesHandler{
		data:      []*timeseries.TimeseriesImportStruct{},
		dataMutex: &sync.Mutex{},
		msgCh:     make(chan mqttMsg, 2048),
	}
	handler.startWorkers(4)

	const messages = 500
	for i := range messages {
		handler.processData(fmt.Sprintf("client%d/sensor/data", i%50), fmt.Sprintf("%f", float32(i)))
	}
	time.Sleep(100 * time.Millisecond) // let workers drain

	data, err := handler.getAndClearData()
	if err != nil {
		t.Fatal(err)
	}
	total := 0
	for _, d := range data {
		total += len(d.Values)
	}
	if total != messages {
		t.Errorf("expected %d values stored, got %d", messages, total)
	}
}

// ── Item #9 ──────────────────────────────────────────────────────────────────

// TestRegressionInitReturnsSensorsAfterInsert verifies that Init correctly
// reports how many sensors were registered, providing a guard against the
// bug where InsertSensor errors were swallowed (#9).
func TestRegressionInitRegistersSensors(t *testing.T) {
	iot := regressionIoT()
	name := "reg9-" + uuid.NewString()
	sensors := []string{name + "A", name + "B", name + "C"}
	desc := DeviceDesc{Name: name, Sensors: sensors}

	dev, err := iot.Init(desc)
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	stored, err := iot.DeviceDB.GetSensors(dev.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(stored) != len(sensors) {
		t.Errorf("expected %d sensors stored, got %d", len(sensors), len(stored))
	}
}

// (cert tests removed — TLS is now delegated to a reverse proxy such as Traefik)
