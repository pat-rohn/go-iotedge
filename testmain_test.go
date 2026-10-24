package iotedge

// testmain_test.go — package-level test lifecycle hooks.

import (
	"fmt"
	"os"
	"testing"
)

// TestMain picks the database (SQLite, or Postgres with IOTEDGE_TEST_POSTGRES),
// runs all tests and closes the shared test database.
func TestMain(m *testing.M) {
	stopPostgres, err := startTestPostgres()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	code := m.Run()

	// Closing lets SQLite checkpoint the WAL and remove its sidecar files.
	if sharedIoT.DeviceDB != nil {
		sharedIoT.Close()
	}

	if stopPostgres != nil {
		if err := stopPostgres(); err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
	}
	os.Exit(code)
}
