package iotedge

// logpersist_test.go — P1 of doc/EDGE_SERVER_WORKBOOK.md: which server log
// lines are written to the logs table.

import (
	"net/http"
	"testing"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

func countLogs(t *testing.T, iot *IoTEdge, device, text string) int {
	t.Helper()
	db := GetLoggingDB(iot.IoTConfig.DbConfig)
	rows, err := db.DB.Query(iot.DeviceDB.rebind(`SELECT COUNT(*) FROM logs WHERE device = ? AND text = ?`), device, text)
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
	oldLevel := log.GetLevel()
	log.SetLevel(log.InfoLevel)
	defer log.SetLevel(oldLevel)

	info := "p1-info-" + uuid.NewString()
	warn := "p1-warn-" + uuid.NewString()
	log.Info(info)
	log.Warn(warn)

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
	oldLevel := log.GetLevel()
	log.SetLevel(log.InfoLevel)
	defer log.SetLevel(oldLevel)

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

func TestGetDeviceDBReuseDoesNotLogAtInfo(t *testing.T) {
	cfg := regressionIoT().IoTConfig.DbConfig
	withLogHook(func(h *logHook) {
		GetDeviceDB(cfg)
		h.mu.Lock()
		defer h.mu.Unlock()
		for _, e := range h.entries {
			if e.Level <= log.InfoLevel {
				t.Errorf("GetDeviceDB logged at %s: %q", e.Level, e.Message)
			}
		}
	})
}
