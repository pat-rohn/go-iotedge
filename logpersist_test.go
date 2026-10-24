package iotedge

// logpersist_test.go — P1 of doc/EDGE_SERVER_WORKBOOK.md: which server log
// lines are written to the logs table.

import (
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"log/slog"
)

func countLogs(t *testing.T, iot *IoTEdge, device, text string) int {
	t.Helper()
	db := iot.LoggingDB
	rows, err := db.DB.Query(iot.DeviceDB.Rebind(`SELECT COUNT(*) FROM logs WHERE device = ? AND text = ?`), device, text)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var n int
	rows.Next()
	if err := rows.Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestPersistServerLogsOnlyAtConfiguredLevel(t *testing.T) {
	iot := regressionIoT()

	info := "p1-info-" + uuid.NewString()
	warn := "p1-warn-" + uuid.NewString()
	slog.Info(info)
	slog.Warn(warn)

	if n := countLogs(t, &iot, "server", info); n != 0 {
		t.Errorf("info log persisted %d times, want 0", n)
	}
	if n := countLogs(t, &iot, "server", warn); n != 1 {
		t.Errorf("warning log persisted %d times, want 1", n)
	}
}

func TestDeviceLogIsPersistedOnce(t *testing.T) {
	iot := regressionIoT()
	router := newTestRouter(&iot)

	device := "p1-dev-" + uuid.NewString()
	for _, level := range []Loglevel{Info, Error} {
		text := "device says " + uuid.NewString()
		w := postJSON(t, router, URILogging, LogMessage{Device: device, Text: text, Level: level})
		if w.Code != http.StatusOK {
			t.Fatalf("log = %d", w.Code)
		}
		if n := countLogs(t, &iot, device, text); n != 1 {
			t.Errorf("level %d device log stored %d times, want 1", level, n)
		}
	}
}

// P1: every MQTT reading refreshes the device's activity. That path must not
// log at Info, or each reading becomes a log line.
func TestRecordDeviceActivityDoesNotLogAtInfo(t *testing.T) {
	iot := regressionIoT()
	device := "p1-activity-" + uuid.NewString()
	if err := iot.DeviceDB.RecordDeviceActivity(device, time.Now()); err != nil { // creates the device
		t.Fatal(err)
	}
	withLogHook(func(h *logHook) {
		if err := iot.DeviceDB.RecordDeviceActivity(device, time.Now()); err != nil {
			t.Fatal(err)
		}
		h.mu.Lock()
		defer h.mu.Unlock()
		for _, e := range h.entries {
			if e.Level >= slog.LevelInfo {
				t.Errorf("RecordDeviceActivity logged at %s: %q", e.Level, e.Message)
			}
		}
	})
}

// The timeseries library still logs through logrus; its warnings must reach
// the logs table like the server's own.
func TestLogrusWarningsFromLibrariesArePersisted(t *testing.T) {
	iot := regressionIoT()
	text := "library-warn-" + uuid.NewString()
	logrus.WithField("package", "dbhandler").Warn(text)
	if n := countLogs(t, &iot, "server", text); n != 1 {
		t.Errorf("logrus warning persisted %d times, want 1", n)
	}
	debug := "library-debug-" + uuid.NewString()
	logrus.Debug(debug)
	if n := countLogs(t, &iot, "server", debug); n != 0 {
		t.Errorf("logrus debug persisted %d times, want 0", n)
	}
}
