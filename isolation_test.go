package iotedge

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// P3: the suite must not read or write the developer's ~/.iotserver config or
// a database in the source tree, so parallel runs cannot disturb each other.
func TestSuiteUsesItsOwnConfigAndDatabase(t *testing.T) {
	home, _ := os.UserHomeDir()
	if !strings.HasPrefix(home, os.TempDir()) {
		t.Errorf("HOME = %s, want a temp directory", home)
	}
	cfg := GetConfig()
	if cfg.DbConfig.UsePostgres {
		return // embedded Postgres, own data directory
	}
	dbPath, _ := filepath.Abs(cfg.DbConfig.IPOrPath + cfg.DbConfig.Name)
	if !strings.HasPrefix(dbPath, os.TempDir()) {
		t.Errorf("test database %s is not in a temp directory", dbPath)
	}
}
