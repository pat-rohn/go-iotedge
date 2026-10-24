package iotedge

import (
	"fmt"
	"sync"
	"sync/atomic"
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

type LogFilter struct {
	Device string
	Level  *Loglevel
}

type LoggingDB struct {
	*timeseries.DbHandler
	conf timeseries.DBConfig
}

var onceLoggingDB sync.Once
var loggingDB *LoggingDB
var loggingHookOnce sync.Once

type databaseLogHook struct {
	db *LoggingDB
}

// logPersistLevel is the lowest log level databaseLogHook stores; set from
// IoTConfig.LogPersistLevel in New. Every stored line is a DB write.
var logPersistLevel atomic.Uint32

func init() {
	setLogPersistLevel(log.WarnLevel)
}

func setLogPersistLevel(level log.Level) {
	logPersistLevel.Store(uint32(level))
}

func (h *databaseLogHook) Levels() []log.Level {
	return log.AllLevels
}

func (h *databaseLogHook) Fire(entry *log.Entry) error {
	if entry.Data["persisted_log"] == true || uint32(entry.Level) > logPersistLevel.Load() {
		return nil
	}
	device := "server"
	if value, ok := entry.Data["device"].(string); ok && value != "" {
		device = value
	}
	level := Info
	switch entry.Level {
	case log.PanicLevel, log.FatalLevel, log.ErrorLevel:
		level = Error
	case log.WarnLevel:
		level = Warning
	case log.DebugLevel, log.TraceLevel:
		level = Debug
	}
	var query string
	if h.db.conf.UsePostgres {
		query = `INSERT INTO logs (device, text, level) VALUES ($1, $2, $3)`
	} else {
		query = `INSERT INTO logs (device, text, level) VALUES (?, ?, ?)`
	}
	if _, err := h.db.DB.Exec(query, device, entry.Message, int(level)); err != nil {
		return fmt.Errorf("persist server log: %w", err)
	}
	return nil
}

// GetLoggingDB returns the singleton LoggingDB, initialising it the first time.
// The early-return guard is intentionally absent — sync.Once handles concurrent
// first-time initialisation correctly and avoids a data race on loggingDB.
func GetLoggingDB(config timeseries.DBConfig) *LoggingDB {
	logger := log.WithFields(log.Fields{"fnct": "GetLoggingDB", "name": config.Name})
	onceLoggingDB.Do(func() {
		logger.Infoln("init")
		// GetDeviceDB opens the shared handler and migrates the schema,
		// including the logs table.
		loggingDB = &LoggingDB{conf: config}
		loggingDB.DbHandler = GetDeviceDB(config).DbHandler
		loggingHookOnce.Do(func() {
			log.AddHook(&databaseLogHook{db: loggingDB})
		})
	})
	// Guard against nil deviceDB before comparing configs (Fix #3).
	if deviceDB != nil && !compareConfigs(deviceDB.conf, config) {
		logger.Fatalf("Config must not change %+v to %+v", deviceDB.conf, config)
	}
	return loggingDB
}

func (s *IoTEdge) LogMessage(msg LogMessage) error {
	// persisted_log: InsertLogMessage below stores the message, the hook must not.
	logger := log.WithFields(log.Fields{"fnct": "LogMessage", "device": msg.Device, "level": msg.Level, "persisted_log": true})
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
	return l.GetLogMessagesPage(limit, 0, LogFilter{})
}

func (l *LoggingDB) GetLogMessagesPage(limit, offset int, filter LogFilter) ([]LogMessage, error) {
	logger := log.WithFields(log.Fields{"fnct": "GetLogMessages", "limit": limit})
	if limit <= 0 || limit > 100 {
		return nil, fmt.Errorf("limit must be between 1 and 100")
	}
	if offset < 0 {
		return nil, fmt.Errorf("offset must not be negative")
	}
	// Select only the columns needed for scanning; id is not projected here.
	var sqlStr string
	args := []any{}
	if l.conf.UsePostgres {
		sqlStr = `SELECT timestamp, device, text, level FROM logs WHERE 1=1`
		if filter.Device != "" {
			sqlStr += ` AND device = $1`
			args = append(args, filter.Device)
		}
		if filter.Level != nil {
			sqlStr += fmt.Sprintf(` AND level = $%d`, len(args)+1)
			args = append(args, int(*filter.Level))
		}
		sqlStr += fmt.Sprintf(` ORDER BY timestamp DESC, id DESC LIMIT $%d OFFSET $%d`, len(args)+1, len(args)+2)
	} else {
		sqlStr = `SELECT timestamp, device, text, level FROM logs WHERE 1=1`
		if filter.Device != "" {
			sqlStr += ` AND device = ?`
			args = append(args, filter.Device)
		}
		if filter.Level != nil {
			sqlStr += ` AND level = ?`
			args = append(args, int(*filter.Level))
		}
		sqlStr += ` ORDER BY timestamp DESC, id DESC LIMIT ? OFFSET ?`
	}
	args = append(args, limit, offset)
	rows, err := l.ExecuteQuery(sqlStr, args...)
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
