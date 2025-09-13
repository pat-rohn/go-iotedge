package iotedge

import log "github.com/sirupsen/logrus"

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
	return nil
}
