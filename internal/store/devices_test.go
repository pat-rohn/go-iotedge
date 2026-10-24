package store

import (
	"path/filepath"
	"testing"

	"github.com/pat-rohn/timeseries"
)

// openTestDB opens a migrated SQLite database of its own. Postgres runs come
// from the go-iotedge suite with IOTEDGE_TEST_POSTGRES.
func openTestDB(t *testing.T) *DeviceDB {
	t.Helper()
	db, err := OpenDeviceDB(timeseries.DBConfig{Name: "test.db", IPOrPath: filepath.Join(t.TempDir()) + "/"})
	if err != nil {
		t.Fatalf("OpenDeviceDB: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// TestRegressionRebindPostgresPlaceholders verifies that rebind rewrites '?'
// placeholders to '$n' for Postgres and leaves SQLite queries untouched.
func TestRegressionRebindPostgresPlaceholders(t *testing.T) {
	pg := &DeviceDB{conf: timeseries.DBConfig{UsePostgres: true}}
	got := pg.Rebind("UPDATE sensors SET sensor_offset = ? WHERE deviceid = ? AND name = ?")
	want := "UPDATE sensors SET sensor_offset = $1 WHERE deviceid = $2 AND name = $3"
	if got != want {
		t.Errorf("rebind (postgres):\n got  %q\n want %q", got, want)
	}

	lite := &DeviceDB{conf: timeseries.DBConfig{UsePostgres: false}}
	sqlStr := "SELECT id FROM devices WHERE name = ?"
	if got := lite.Rebind(sqlStr); got != sqlStr {
		t.Errorf("rebind (sqlite) must be a no-op, got %q", got)
	}
}
