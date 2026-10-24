package store

import (
	"fmt"
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

// Postgres reports whether the database is Postgres (else SQLite).
func (l *LoggingDB) Postgres() bool { return l.conf.UsePostgres }

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
