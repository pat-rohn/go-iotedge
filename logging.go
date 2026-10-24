package iotedge

import (
	"fmt"
	"sync"
	"sync/atomic"

	log "github.com/sirupsen/logrus"
)

// logTarget is where databaseLogHook stores server log lines. logrus' logger
// is global, so the hook is too; New points it at its database.
var logTarget atomic.Pointer[LoggingDB]
var logHookOnce sync.Once

type databaseLogHook struct{}

// persistServerLogs makes the database log hook write to db.
func persistServerLogs(db *LoggingDB) {
	logTarget.Store(db)
	logHookOnce.Do(func() { log.AddHook(&databaseLogHook{}) })
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
	db := logTarget.Load()
	if db == nil || entry.Data["persisted_log"] == true || uint32(entry.Level) > logPersistLevel.Load() {
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
	if db.Postgres() {
		query = `INSERT INTO logs (device, text, level) VALUES ($1, $2, $3)`
	} else {
		query = `INSERT INTO logs (device, text, level) VALUES (?, ?, ?)`
	}
	if _, err := db.DB.Exec(query, device, entry.Message, int(level)); err != nil {
		return fmt.Errorf("persist server log: %w", err)
	}
	return nil
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
	if err := s.LoggingDB.InsertLogMessage(msg); err != nil {
		logger.Errorf("failed to log message to DB:%v", err)
		return err
	}
	return nil
}
