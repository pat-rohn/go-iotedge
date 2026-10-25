package store

// schema_test.go — A4 of doc/EDGE_SERVER_WORKBOOK.md: versioned schema
// migrations.

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pat-rohn/timeseries"
)

func appliedVersions(t *testing.T, db *sql.DB) []int {
	t.Helper()
	rows, err := db.Query(`SELECT version FROM schema_migrations ORDER BY version`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var versions []int
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		versions = append(versions, v)
	}
	return versions
}

func TestMigrateIsIdempotentAndRecordsVersions(t *testing.T) {
	db := openTestDB(t) // migrates once
	if err := db.Migrate(); err != nil {
		t.Fatalf("second migrate: %v", err)
	}
	versions := appliedVersions(t, db.DB)
	if len(versions) != len(migrations) {
		t.Fatalf("applied versions = %v, want one per migration (%d)", versions, len(migrations))
	}
	for i, m := range migrations {
		if versions[i] != m.version {
			t.Errorf("version %d = %d, want %d", i, versions[i], m.version)
		}
	}
}

func columnNames(t *testing.T, db *sql.DB, table string) map[string]bool {
	t.Helper()
	rows, err := db.Query(`SELECT name FROM pragma_table_info(?)`, table)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	names := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		names[name] = true
	}
	return names
}

// A database written by an old release: logs keyed by (timestamp, device),
// devices without the address columns, no monitoring or forwarding tables.
func TestMigrateUpgradesLegacySQLiteSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, stmt := range []string{
		`CREATE TABLE devices (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE,
			description TEXT DEFAULT '', intervall NUMBER DEFAULT 60, buffer INTEGER DEFAULT 2)`,
		`INSERT INTO devices (name, description) VALUES ('old-device', '192.0.2.1')`,
		`CREATE TABLE logs (timestamp DATETIME DEFAULT (datetime('subsec')), device TEXT NOT NULL,
			text TEXT DEFAULT '', level INTEGER DEFAULT 2, PRIMARY KEY (timestamp, device))`,
		`INSERT INTO logs (device, text, level) VALUES ('old-device', 'kept', 1)`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("legacy schema: %v", err)
		}
	}

	legacy := &DeviceDB{DbHandler: &timeseries.DbHandler{DB: db}, conf: timeseries.DBConfig{}}
	if err := legacy.Migrate(); err != nil {
		t.Fatalf("migrate legacy database: %v", err)
	}

	if cols := columnNames(t, db, "logs"); !cols["id"] {
		t.Error("logs has no id column after migration")
	}
	if cols := columnNames(t, db, "devices"); !cols["address"] || !cols["address_seen_at"] {
		t.Errorf("devices columns after migration: %v", cols)
	}
	for _, table := range []string{"sensors", "device_health", "device_restarts", ForwardOutboxTable} {
		if len(columnNames(t, db, table)) == 0 {
			t.Errorf("table %s missing after migration", table)
		}
	}
	var text, description string
	if err := db.QueryRow(`SELECT text FROM logs WHERE device = 'old-device'`).Scan(&text); err != nil || text != "kept" {
		t.Errorf("legacy log row: %q, %v", text, err)
	}
	if err := db.QueryRow(`SELECT description FROM devices WHERE name = 'old-device'`).Scan(&description); err != nil || description != "192.0.2.1" {
		t.Errorf("legacy device row: %q, %v", description, err)
	}
}

// The log pages must read the newest rows by index, not sort the whole table.
func TestLogQueriesUseIndex(t *testing.T) {
	db := openTestDB(t)
	for _, where := range []string{"", ` WHERE device = 'x'`, ` WHERE level = 3`} {
		rows, err := db.DB.Query(`EXPLAIN QUERY PLAN SELECT timestamp, device, text, level FROM logs` +
			where + ` ORDER BY timestamp DESC, id DESC LIMIT 100`)
		if err != nil {
			t.Fatal(err)
		}
		var plan string
		for rows.Next() {
			var id, parent, unused int
			var detail string
			if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
				t.Fatal(err)
			}
			plan += detail + "; "
		}
		rows.Close()
		if !strings.Contains(plan, "USING INDEX idx_logs_") || strings.Contains(plan, "TEMP B-TREE") {
			t.Errorf("query%s plan = %q, want an idx_logs_ index without a sort", where, plan)
		}
	}
}
