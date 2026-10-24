package iotedge

// testmain_test.go — package-level test lifecycle hooks.

import (
	"fmt"
	"os"
	"testing"

	"github.com/spf13/viper"
)

// TestMain picks the database (SQLite, or Postgres with IOTEDGE_TEST_POSTGRES),
// runs all tests and closes the shared test database.
func TestMain(m *testing.M) {
	stopPostgres, err := startTestPostgres()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	// After Postgres started: embedded-postgres caches its binaries in $HOME.
	sandbox, err := isolateTestRun(stopPostgres == nil)
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
	os.RemoveAll(sandbox)
	os.Exit(code)
}

// isolateTestRun gives the run its own HOME, so GetConfig neither reads nor
// writes the developer's ~/.iotserver/iot.json, and (on SQLite) its own
// database file. Parallel runs then share nothing but free ports.
func isolateTestRun(sqlite bool) (string, error) {
	sandbox, err := os.MkdirTemp("", "iotedge-test-")
	if err != nil {
		return "", err
	}
	if err := os.Setenv("HOME", sandbox); err != nil {
		return "", err
	}
	viper.Set("TimeseriesTable", "measurements")
	if sqlite {
		viper.Set("DBConfig.UsePostgres", false)
		viper.Set("DBConfig.IPOrPath", sandbox+"/")
		viper.Set("DBConfig.Name", "test.db")
	}
	return sandbox, nil
}
