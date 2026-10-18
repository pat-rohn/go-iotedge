package iotedge

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pat-rohn/timeseries"
)

func TestMonitoringSummaryReportsOnlineStaleAndNeverSeen(t *testing.T) {
	iot := regressionIoT()
	now := time.Date(2026, 10, 18, 8, 0, 0, 0, time.UTC)
	online := "monitoring-online-" + uuid.NewString()
	stale := "monitoring-stale-" + uuid.NewString()
	neverSeen := "monitoring-never-" + uuid.NewString()

	for _, name := range []string{online, stale, neverSeen} {
		if _, err := iot.Init(DeviceDesc{Name: name}); err != nil {
			t.Fatalf("Init(%s): %v", name, err)
		}
	}
	if err := iot.DeviceDB.RecordDeviceHealth(DeviceHealthReport{Device: online, FirmwareVersion: "1.2.3"}, now.Add(-9*time.Minute)); err != nil {
		t.Fatalf("RecordDeviceHealth online: %v", err)
	}
	if err := iot.DeviceDB.RecordDeviceHealth(DeviceHealthReport{Device: stale, FirmwareVersion: "1.2.3"}, now.Add(-11*time.Minute)); err != nil {
		t.Fatalf("RecordDeviceHealth stale: %v", err)
	}

	summary, err := iot.DeviceDB.GetMonitoringSummary(MonitoringConfig{OnlineWindowMinutes: 10, RestartWindowDays: 7}, now)
	if err != nil {
		t.Fatalf("GetMonitoringSummary: %v", err)
	}
	statuses := make(map[string]string)
	for _, device := range summary.Devices {
		statuses[device.Device] = device.Status
	}
	if got := statuses[online]; got != "online" {
		t.Errorf("online status = %q, want online", got)
	}
	if got := statuses[stale]; got != "stale" {
		t.Errorf("stale status = %q, want stale", got)
	}
	if got := statuses[neverSeen]; got != "never_seen" {
		t.Errorf("never-seen status = %q, want never_seen", got)
	}
	if summary.Online < 1 || summary.Stale < 1 || summary.NeverSeen < 1 {
		t.Errorf("summary counts = online:%d stale:%d never_seen:%d, want at least one of each",
			summary.Online, summary.Stale, summary.NeverSeen)
	}
}

func TestMonitoringRestartCountDeduplicatesBootAndHonorsWindow(t *testing.T) {
	iot := regressionIoT()
	now := time.Date(2026, 10, 18, 8, 0, 0, 0, time.UTC)
	device := "monitoring-restarts-" + uuid.NewString()
	if err := iot.DeviceDB.RecordDeviceHealth(DeviceHealthReport{Device: device, BootID: "boot-current"}, now); err != nil {
		t.Fatalf("RecordDeviceHealth first boot: %v", err)
	}
	if err := iot.DeviceDB.RecordDeviceHealth(DeviceHealthReport{Device: device, BootID: "boot-current"}, now.Add(time.Minute)); err != nil {
		t.Fatalf("RecordDeviceHealth retry: %v", err)
	}
	if err := iot.DeviceDB.RecordDeviceHealth(DeviceHealthReport{Device: device, BootID: "boot-old"}, now.AddDate(0, 0, -8)); err != nil {
		t.Fatalf("RecordDeviceHealth old boot: %v", err)
	}

	summary, err := iot.DeviceDB.GetMonitoringSummary(MonitoringConfig{OnlineWindowMinutes: 10, RestartWindowDays: 7}, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("GetMonitoringSummary: %v", err)
	}
	for _, item := range summary.Devices {
		if item.Device == device {
			if item.Restarts != 1 {
				t.Errorf("restart count = %d, want 1", item.Restarts)
			}
			return
		}
	}
	t.Fatalf("device %q missing from summary", device)
}

func TestMonitoringHealthHandlerValidatesAndRecordsReport(t *testing.T) {
	iot := regressionIoT()
	router := newTestRouter(&iot)
	router.POST(URIMonitoringHealth, iot.MonitoringHealthHandler)

	w := postJSON(t, router, URIMonitoringHealth, DeviceHealthReport{})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("empty health report status = %d, want 400: %s", w.Code, w.Body.String())
	}

	device := "monitoring-http-" + uuid.NewString()
	unknownAge := int64(-1)
	w = postJSON(t, router, URIMonitoringHealth, DeviceHealthReport{
		Device: device, BootID: "boot-http", FirmwareVersion: "2026.10.18", SensorAgeSeconds: &unknownAge,
	})
	if w.Code != http.StatusOK {
		t.Fatalf("health report status = %d, want 200: %s", w.Code, w.Body.String())
	}

	req := httptest.NewRequest(http.MethodGet, URIDashboard, nil)
	req.Header.Set("Accept", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, req)
	if response.Code != http.StatusOK {
		t.Fatalf("dashboard status = %d: %s", response.Code, response.Body.String())
	}
	var payload DashboardResponse
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		t.Fatalf("decode dashboard: %v", err)
	}
	for _, item := range payload.Monitoring.Devices {
		if item.Device == device && item.Restarts == 1 {
			return
		}
	}
	t.Errorf("monitoring response does not contain %q with one restart: %+v", device, payload.Monitoring)
}

func TestMonitoringMQTTHealthIsRecordedAndNotStoredAsMeasurement(t *testing.T) {
	iot := regressionIoT()
	device := "monitoring-mqtt-" + uuid.NewString()
	handler := TimeseriesHandler{
		data:      []*timeseries.TimeseriesImportStruct{},
		dataMutex: &sync.Mutex{},
		recordHealth: func(report DeviceHealthReport) error {
			return iot.DeviceDB.RecordDeviceHealth(report, time.Now())
		},
	}

	handler.processData("iotedge/"+device+"/health", `{"device":"`+device+`","boot_id":"mqtt-boot","firmware_version":"1.0.0"}`)

	if len(handler.data) != 0 {
		t.Fatalf("health message must not enter measurement buffer: %+v", handler.data)
	}
	summary, err := iot.DeviceDB.GetMonitoringSummary(MonitoringConfig{}, time.Now())
	if err != nil {
		t.Fatalf("GetMonitoringSummary: %v", err)
	}
	for _, item := range summary.Devices {
		if item.Device == device {
			if item.Restarts != 1 || item.FirmwareVersion != "1.0.0" {
				t.Errorf("monitoring item = %+v, want one restart and firmware 1.0.0", item)
			}
			return
		}
	}
	t.Fatalf("device %q missing from monitoring summary", device)
}

func TestMonitoringMQTTMeasurementRefreshesDeviceActivity(t *testing.T) {
	iot := regressionIoT()
	device := "SweetHome1-" + uuid.NewString()
	handler := TimeseriesHandler{
		data:      []*timeseries.TimeseriesImportStruct{},
		dataMutex: &sync.Mutex{},
		recordActivity: func(name string) error {
			return iot.DeviceDB.RecordDeviceActivity(name, time.Now())
		},
	}
	handler.processData("frick/livingroom/"+device+"/"+device+"Temperature/data", "21.5")

	summary, err := iot.DeviceDB.GetMonitoringSummary(MonitoringConfig{}, time.Now())
	if err != nil {
		t.Fatalf("GetMonitoringSummary: %v", err)
	}
	for _, item := range summary.Devices {
		if item.Device == device {
			if item.Status != "online" {
				t.Errorf("measurement device status = %q, want online", item.Status)
			}
			return
		}
	}
	t.Fatalf("device %q missing from monitoring summary", device)
}
