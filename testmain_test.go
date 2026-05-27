package iotedge

// testmain_test.go — package-level test lifecycle hooks.

import (
	"os"
	"testing"
)

// TestMain runs all tests and then closes the shared DB singleton so that
// SQLite can flush the WAL, checkpoint, and remove the -wal / -shm sidecar
// files before the process exits.  Without an explicit Close() the OS merely
// reclaims file handles and SQLite never gets to clean up.
func TestMain(m *testing.M) {
	code := m.Run()

	// dbhandler is the package-level *timeseries.DbHandler set by
	// GetDeviceDB / GetLoggingDB.  Calling Close() on it:
	//   1. Issues PRAGMA journal_mode=DELETE  → SQLite checkpoints the WAL
	//      and removes the -wal / -shm files.
	//   2. Closes the underlying sql.DB connection pool.
	//   3. Resets the timeseries package singleton so future opens are fresh.
	if dbhandler != nil {
		if err := dbhandler.Close(); err != nil {
			// Non-fatal: log and continue so os.Exit still runs.
			_ = err
		}
	}

	os.Exit(code)
}
