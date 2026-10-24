package store

import (
	"database/sql"
	"fmt"
	"time"

	"log/slog"
)

// migration is one schema step. Steps run in order, each in a transaction,
// and are recorded in schema_migrations so they run once per database.
type migration struct {
	version int
	name    string
	up      func(tx *sql.Tx, postgres bool) error
}

// ForwardOutboxTable queues batches for go-iotedge's forwarder.
const ForwardOutboxTable = "forward_outbox"

// migrations lists every schema step. Append new steps; never change or
// reorder released ones. The timeseries table is created by the timeseries
// library, not here.
var migrations = []migration{
	{1, "baseline", baselineSchema},
	{2, "device health history", healthHistorySchema},
}

// healthHistorySchema keeps every health report (pruned by age) for the
// device page's charts; device_health holds only the latest.
func healthHistorySchema(tx *sql.Tx, postgres bool) error {
	id := "id INTEGER PRIMARY KEY AUTOINCREMENT"
	if postgres {
		id = "id BIGSERIAL PRIMARY KEY"
	}
	for _, stmt := range []string{
		`CREATE TABLE IF NOT EXISTS device_health_history (
			` + id + `,
			device_name TEXT NOT NULL,
			recorded_at BIGINT NOT NULL,
			uptime_seconds BIGINT,
			free_heap_bytes BIGINT,
			wifi_rssi INTEGER,
			time_synced BOOLEAN,
			sensor_age_seconds BIGINT
		)`,
		`CREATE INDEX IF NOT EXISTS idx_health_history_device_time ON device_health_history(device_name, recorded_at)`,
	} {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

// Migrate applies the steps the database has not seen yet.
func (devDB *DeviceDB) Migrate() error {
	if _, err := devDB.DB.Exec(`CREATE TABLE IF NOT EXISTS schema_migrations (
		version INTEGER PRIMARY KEY,
		name TEXT NOT NULL,
		applied_at BIGINT NOT NULL
	)`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}
	applied := map[int]bool{}
	rows, err := devDB.DB.Query(`SELECT version FROM schema_migrations`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			rows.Close()
			return err
		}
		applied[v] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	for _, m := range migrations {
		if applied[m.version] {
			continue
		}
		tx, err := devDB.DB.Begin()
		if err != nil {
			return err
		}
		if err := m.up(tx, devDB.conf.UsePostgres); err != nil {
			tx.Rollback()
			return fmt.Errorf("schema migration %d (%s): %w", m.version, m.name, err)
		}
		if _, err := tx.Exec(devDB.Rebind(`INSERT INTO schema_migrations (version, name, applied_at) VALUES (?, ?, ?)`),
			m.version, m.name, time.Now().Unix()); err != nil {
			tx.Rollback()
			return fmt.Errorf("record schema migration %d: %w", m.version, err)
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		slog.Info(fmt.Sprintf("Applied schema migration %d (%s)", m.version, m.name), "fnct", "migrate")
	}
	return nil
}

// baselineSchema creates the schema as releases before versioned migrations
// left it, and upgrades databases from older releases to that state.
func baselineSchema(tx *sql.Tx, postgres bool) error {
	id := "id INTEGER PRIMARY KEY AUTOINCREMENT"
	logsID := id
	bigID := id
	numeric := "NUMBER"
	timestamp := "DATETIME"
	logsTimestamp := "DATETIME DEFAULT (datetime('subsec'))"
	if postgres {
		id = "id SERIAL PRIMARY KEY"
		logsID = id
		bigID = "id BIGSERIAL PRIMARY KEY"
		numeric = "NUMERIC"
		timestamp = "TIMESTAMP WITH TIME ZONE"
		logsTimestamp = "TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP"
	}
	statements := []string{
		`CREATE TABLE IF NOT EXISTS devices (
			` + id + `,
			name        TEXT NOT NULL UNIQUE,
			description TEXT DEFAULT '',
			intervall   ` + numeric + ` DEFAULT 60,
			buffer      INTEGER DEFAULT 2
		)`,
		`CREATE TABLE IF NOT EXISTS sensors (
			` + id + `,
			deviceid      INTEGER NOT NULL,
			name          TEXT NOT NULL,
			description   TEXT DEFAULT '',
			sensor_offset ` + numeric + ` DEFAULT 0,
			UNIQUE (deviceid, name)
		)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_sensors_unique ON sensors(deviceid, name)`,
		`CREATE TABLE IF NOT EXISTS logs (
			` + logsID + `,
			timestamp ` + logsTimestamp + `,
			device    TEXT NOT NULL,
			text      TEXT DEFAULT '',
			level     INTEGER DEFAULT 2
		)`,
		`CREATE TABLE IF NOT EXISTS device_health (
			device_name TEXT PRIMARY KEY,
			last_seen ` + timestamp + ` NOT NULL,
			firmware_version TEXT NOT NULL DEFAULT '',
			uptime_seconds BIGINT,
			free_heap_bytes BIGINT,
			wifi_rssi INTEGER,
			time_synced BOOLEAN,
			sensor_age_seconds BIGINT
		)`,
		`CREATE TABLE IF NOT EXISTS device_restarts (
			` + bigID + `,
			device_name TEXT NOT NULL,
			boot_id TEXT NOT NULL,
			occurred_at ` + timestamp + ` NOT NULL,
			UNIQUE (device_name, boot_id)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_device_restarts_device_time ON device_restarts(device_name, occurred_at)`,
		`CREATE TABLE IF NOT EXISTS ` + ForwardOutboxTable + ` (
			` + bigID + `,
			created_at BIGINT NOT NULL,
			payload TEXT NOT NULL,
			attempts INTEGER NOT NULL DEFAULT 0,
			next_attempt_at BIGINT NOT NULL
		)`,
	}
	for _, statement := range statements {
		if _, err := tx.Exec(statement); err != nil {
			return err
		}
	}
	if err := addLogsID(tx, postgres); err != nil {
		return fmt.Errorf("logs id: %w", err)
	}
	if err := addColumn(tx, postgres, "devices", "address", "TEXT NOT NULL DEFAULT ''"); err != nil {
		return err
	}
	return addColumn(tx, postgres, "devices", "address_seen_at", timestamp)
}

func hasColumn(tx *sql.Tx, table, column string) (bool, error) {
	var n int
	err := tx.QueryRow(`SELECT COUNT(*) FROM pragma_table_info(?) WHERE name = ?`, table, column).Scan(&n)
	return n > 0, err
}

// addColumn adds a column unless it exists (SQLite has no ADD COLUMN IF NOT EXISTS).
func addColumn(tx *sql.Tx, postgres bool, table, column, definition string) error {
	if postgres {
		_, err := tx.Exec(`ALTER TABLE ` + table + ` ADD COLUMN IF NOT EXISTS ` + column + ` ` + definition)
		return err
	}
	exists, err := hasColumn(tx, table, column)
	if err != nil || exists {
		return err
	}
	_, err = tx.Exec(`ALTER TABLE ` + table + ` ADD COLUMN ` + column + ` ` + definition)
	return err
}

// addLogsID gives a logs table from old releases (keyed by timestamp and
// device) its auto-increment id, keeping the rows.
func addLogsID(tx *sql.Tx, postgres bool) error {
	if postgres {
		_, err := tx.Exec(`ALTER TABLE logs ADD COLUMN IF NOT EXISTS id SERIAL`)
		return err
	}
	exists, err := hasColumn(tx, "logs", "id")
	if err != nil || exists {
		return err
	}
	slog.Warn("Old logs table schema detected (no id column) — migrating")
	for _, step := range []string{
		`ALTER TABLE logs RENAME TO logs_old`,
		`CREATE TABLE logs (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			timestamp DATETIME DEFAULT (datetime('subsec')),
			device TEXT NOT NULL,
			text TEXT DEFAULT '',
			level INTEGER DEFAULT 2
		)`,
		`INSERT INTO logs (timestamp, device, text, level) SELECT timestamp, device, text, level FROM logs_old`,
		`DROP TABLE logs_old`,
	} {
		if _, err := tx.Exec(step); err != nil {
			return err
		}
	}
	return nil
}
