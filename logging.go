package iotedge

import (
	"sync"
	"time"

	"github.com/pat-rohn/timeseries"
	log "github.com/sirupsen/logrus"
)

type Loglevel int

const (
	Debug Loglevel = iota
	Info
	Warning
	Error
)

type LogMessage struct {
	Timestamp time.Time
	Device    string
	Text      string
	Level     Loglevel
}

type LoggingDB struct {
	*timeseries.DbHandler
	conf timeseries.DBConfig
}

var onceLoggingDB sync.Once
var loggingDB *LoggingDB

// GetLoggingDB returns the singleton LoggingDB, initialising it the first time.
// The early-return guard is intentionally absent — sync.Once handles concurrent
// first-time initialisation correctly and avoids a data race on loggingDB.
func GetLoggingDB(config timeseries.DBConfig) *LoggingDB {
	logger := log.WithFields(log.Fields{"fnct": "GetLoggingDB", "name": config.Name})
	onceLoggingDB.Do(func() {
		logger.Infoln("init")
		if dbhandler == nil {
			dbhandler = timeseries.DBHandler(config)
		}
		loggingDB = &LoggingDB{conf: config}
		loggingDB.DbHandler = dbhandler

		var idStr string
		timeStampStr := "DATETIME DEFAULT (datetime('subsec'))"
		if config.UsePostgres {
			idStr = "id SERIAL PRIMARY KEY"
			timeStampStr = "TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP"
		} else {
			idStr = "id INTEGER PRIMARY KEY AUTOINCREMENT"
		}
		sqlStr := `CREATE TABLE IF NOT EXISTS logs (
			` + idStr + ` ,
			timestamp ` + timeStampStr + ` ,
			device TEXT NOT NULL,
			text TEXT DEFAULT '',
			level INTEGER DEFAULT 2
);`
		logger.Infoln(sqlStr)
		if err := loggingDB.Execute(sqlStr); err != nil {
			logger.Fatalf("failed to create logging table:%v", err)
		}
		loggingDB.migrateLogsTable()
	})
	// Guard against nil deviceDB before comparing configs (Fix #3).
	if deviceDB != nil && !compareConfigs(deviceDB.conf, config) {
		logger.Fatalf("Config must not change %+v to %+v", deviceDB.conf, config)
	}
	return loggingDB
}

// migrateLogsTable adds the auto-increment `id` primary key to an existing logs
// table that was created under the old schema (PRIMARY KEY (timestamp, device)).
func (l *LoggingDB) migrateLogsTable() {
	logger := log.WithFields(log.Fields{"fnct": "migrateLogsTable"})

	if l.conf.UsePostgres {
		// ALTER TABLE … ADD COLUMN IF NOT EXISTS is idempotent on Postgres 9.6+.
		if err := l.Execute(`ALTER TABLE logs ADD COLUMN IF NOT EXISTS id SERIAL`); err != nil {
			logger.Warnf("Postgres logs migration may need DBA attention: %v", err)
		}
		return
	}

	// SQLite: check whether the 'id' column already exists.
	// Close the cursor before issuing any DDL so we don't hold a read lock
	// that would cause SQLITE_BUSY on ALTER TABLE.
	var count int
	{
		rows, err := l.ExecuteQuery(`SELECT COUNT(*) FROM pragma_table_info('logs') WHERE name='id'`)
		if err != nil {
			logger.Warnf("failed to check logs table schema: %v", err)
			return
		}
		if rows.Next() {
			if err := rows.Scan(&count); err != nil {
				rows.Close()
				logger.Warnf("failed to scan pragma result: %v", err)
				return
			}
		}
		rowsErr := rows.Err()
		rows.Close() // close before any DDL
		if rowsErr != nil {
			logger.Warnf("rows error during schema check: %v", rowsErr)
			return
		}
	}

	if count > 0 {
		return // 'id' column already exists — nothing to do
	}

	// Old schema detected: rename, recreate, copy, drop.
	logger.Warn("Old logs table schema detected (no id column) — performing migration")
	steps := []string{
		`ALTER TABLE logs RENAME TO logs_old`,
		`CREATE TABLE logs (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			timestamp DATETIME DEFAULT (datetime('subsec')),
			device TEXT NOT NULL,
			text TEXT DEFAULT '',
			level INTEGER DEFAULT 2
		)`,
		`INSERT INTO logs (timestamp, device, text, level)
		 SELECT timestamp, device, text, level FROM logs_old`,
		`DROP TABLE logs_old`,
	}
	for _, step := range steps {
		if err := l.Execute(step); err != nil {
			logger.Errorf("migration step failed (%v): %s", err, step)
			return
		}
	}
	logger.Info("Logs table migration complete")
}

func (s *IoTEdge) LogMessage(msg LogMessage) error {
	logger := log.WithFields(log.Fields{"fnct": "LogMessage", "device": msg.Device, "level": msg.Level})
	logger.Infof("Log message")
	switch msg.Level {
	case Debug:
		logger.Debug(msg.Text)
	case Info:
		logger.Info(msg.Text)
	case Warning:
		logger.Warn(msg.Text)
	case Error:
		logger.Error(msg.Text)
	default:
		logger.Info(msg.Text)
	}
	if err := GetLoggingDB(s.IoTConfig.DbConfig).InsertLogMessage(msg); err != nil {
		logger.Errorf("failed to log message to DB:%v", err)
		return err
	}
	return nil
}

func (l *LoggingDB) InsertLogMessage(msg LogMessage) error {
	logger := log.WithFields(log.Fields{"fnct": "InsertLogMessage", "device": msg.Device})
	var sqlStr string
	if l.conf.UsePostgres {
		sqlStr = `INSERT INTO logs (device, text, level) VALUES ($1, $2, $3)`
	} else {
		sqlStr = `INSERT INTO logs (device, text, level) VALUES (?, ?, ?)`
	}
	if err := l.Execute(sqlStr, msg.Device, msg.Text, int(msg.Level)); err != nil {
		logger.Errorf("failed to insert log message:%v", err)
		return err
	}
	return nil
}

func (l *LoggingDB) GetLogMessages(limit int) ([]LogMessage, error) {
	logger := log.WithFields(log.Fields{"fnct": "GetLogMessages", "limit": limit})
	// Select only the columns needed for scanning; id is not projected here.
	var sqlStr string
	if l.conf.UsePostgres {
		sqlStr = `SELECT timestamp, device, text, level FROM logs ORDER BY timestamp DESC, id DESC LIMIT $1;`
	} else {
		sqlStr = `SELECT timestamp, device, text, level FROM logs ORDER BY timestamp DESC, id DESC LIMIT ?;`
	}
	rows, err := l.ExecuteQuery(sqlStr, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var messages []LogMessage
	for rows.Next() {
		var timestamp, device, text string
		var level int
		if err := rows.Scan(&timestamp, &device, &text, &level); err != nil {
			return nil, err
		}
		var t time.Time
		var parseErr error
		if l.conf.UsePostgres {
			t, parseErr = time.Parse(time.RFC3339Nano, timestamp)
			if parseErr != nil {
				t, parseErr = time.Parse(time.RFC3339, timestamp)
			}
		} else {
			// SQLite datetime('subsec') produces "YYYY-MM-DD HH:MM:SS.SSS".
			// Also handle RFC3339 for rows copied from legacy databases.
			for _, layout := range []string{
				"2006-01-02 15:04:05.999999999",
				"2006-01-02 15:04:05",
				time.RFC3339Nano,
				time.RFC3339,
			} {
				t, parseErr = time.Parse(layout, timestamp)
				if parseErr == nil {
					break
				}
			}
		}
		if parseErr != nil {
			logger.Errorf("failed to parse timestamp %q: %v", timestamp, parseErr)
			return nil, parseErr
		}
		messages = append(messages, LogMessage{Timestamp: t, Device: device, Text: text, Level: Loglevel(level)})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return messages, nil
}
