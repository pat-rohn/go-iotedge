package iotedge

import (
	"sync"

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
	Device string
	Text   string
	Level  Loglevel
}

type LoggingDB struct {
	*timeseries.DbHandler
	conf timeseries.DBConfig
}

var onceLoggingDB sync.Once
var loggingDB *LoggingDB

func GetLoggingDB(config timeseries.DBConfig) *LoggingDB {
	logger := log.WithFields(log.Fields{"fnct": "GetLoggingDB", "name": config.Name})
	if loggingDB != nil {
		return loggingDB
	}
	onceLoggingDB.Do(func() {
		logger.Infoln("init")
		if dbhandler == nil {
			dbhandler = timeseries.DBHandler(config)
		}
		loggingDB = &LoggingDB{conf: config}
		loggingDB.DbHandler = dbhandler
		timeStampStr := "DATETIME"

		if config.UsePostgres {
			timeStampStr = "TIMESTAMP"
		}

		sqlStr := `CREATE TABLE IF NOT EXISTS logs (
			timestamp ` + timeStampStr + ` DEFAULT CURRENT_TIMESTAMP,
			device TEXT NOT NULL,
			text TEXT DEFAULT '',
			level INTEGER DEFAULT 2,
			PRIMARY KEY (timestamp, device)
);`
		if _, err := loggingDB.ExecuteQuery(sqlStr); err != nil {
			logger.Fatalf("failed to create logging table:%v", err)
		}

	})
	if !compareConfigs(deviceDB.conf, config) {
		logger.Fatalf("Config must not change %+v to %+v", deviceDB.conf, config)
		deviceDB.Close()
		deviceDB = nil
	}
	return loggingDB
}

func (s *IoTEdge) LogMessage(msg LogMessage) error {
	logger := log.WithFields(log.Fields{"fnct": "LogMessage",
		"device": msg.Device, "level": msg.Level})
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
	logger := log.WithFields(log.Fields{"fnct": "InsertLogMessage",
		"device": msg.Device, "level": msg.Level})
	logger.Infof("Insert log message into DB")
	sqlStr := `INSERT INTO logs (device, text, level) 
               VALUES (?, ?, ?)`
	if _, err := l.ExecuteQuery(sqlStr, msg.Device, msg.Text, int(msg.Level)); err != nil {
		logger.Errorf("failed to insert log message:%v", err)
		return err
	}
	return nil
}
func (l *LoggingDB) GetLogMessages(limit int) ([]LogMessage, error) {
	logger := log.WithFields(log.Fields{"fnct": "GetLogMessages", "limit": limit})
	logger.Infof("Get log messages from DB")
	sqlStr := `SELECT timestamp, device, text, level FROM logs ORDER BY timestamp DESC LIMIT ?;`
	rows, err := l.ExecuteQuery(sqlStr, limit)
	if err != nil {
		logger.Errorf("failed to get log messages:%v", err)
		return nil, err
	}
	defer rows.Close()

	var messages []LogMessage
	for rows.Next() {
		var msg LogMessage
		var timestamp string // Not used currently
		var level int
		if err := rows.Scan(&timestamp, &msg.Device, &msg.Text, &level); err != nil {
			logger.Errorf("failed to scan log message:%v", err)
			return nil, err
		}
		msg.Level = Loglevel(level)
		messages = append(messages, msg)
	}
	if err := rows.Err(); err != nil {
		logger.Errorf("error iterating over log messages:%v", err)
		return nil, err
	}
	return messages, nil
}
