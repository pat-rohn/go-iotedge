package iotedge

// postgres_test.go — runs the whole suite against an embedded Postgres when
// IOTEDGE_TEST_POSTGRES is set:
//
//   IOTEDGE_TEST_POSTGRES=1 go test ./...
//
// The first run downloads the Postgres binaries (~30 MB) into
// ~/.embedded-postgres-go. The package's DB singletons allow one database per
// process, so the dialect is chosen once, before any test runs.

import (
	"fmt"
	"net"
	"os"
	"path/filepath"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/spf13/viper"
)

const testPostgresEnv = "IOTEDGE_TEST_POSTGRES"

// startTestPostgres starts Postgres and points GetConfig at it. It returns a
// stop function, or nil when the suite runs on SQLite.
func startTestPostgres() (func() error, error) {
	if os.Getenv(testPostgresEnv) == "" {
		return nil, nil
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	runtime, err := os.MkdirTemp("", "iotedge-pg-")
	if err != nil {
		return nil, err
	}
	db := embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig().
		Port(uint32(port)).
		Database("iotedge_test").Username("iotedge").Password("iotedge").
		RuntimePath(runtime).DataPath(filepath.Join(runtime, "data")).
		Logger(nil))
	if err := db.Start(); err != nil {
		return nil, fmt.Errorf("start embedded postgres: %w", err)
	}
	viper.Set("DBConfig.UsePostgres", true)
	viper.Set("DBConfig.IPOrPath", "127.0.0.1")
	viper.Set("DBConfig.Port", port)
	viper.Set("DBConfig.Name", "iotedge_test")
	viper.Set("DBConfig.User", "iotedge")
	viper.Set("DBConfig.Password", "iotedge")
	fmt.Printf("tests run against embedded Postgres on port %d\n", port)
	return func() error {
		defer os.RemoveAll(runtime)
		return db.Stop()
	}, nil
}
