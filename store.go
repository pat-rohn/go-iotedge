package iotedge

import (
	"github.com/pat-rohn/go-iotedge/internal/store"
	"github.com/pat-rohn/timeseries"
)

// The database layer lives in internal/store; these aliases keep the
// package's exported names.
type (
	DeviceDB                = store.DeviceDB
	LoggingDB               = store.LoggingDB
	Device                  = store.Device
	DeviceDesc              = store.DeviceDesc
	DeviceConfig            = store.DeviceConfig
	Sensor                  = store.Sensor
	Loglevel                = store.Loglevel
	LogMessage              = store.LogMessage
	LogFilter               = store.LogFilter
	MonitoringConfig        = store.MonitoringConfig
	DeviceHealthReport      = store.DeviceHealthReport
	DeviceMonitoringSummary = store.DeviceMonitoringSummary
	MonitoringSummary       = store.MonitoringSummary
)

const (
	Debug   = store.Debug
	Info    = store.Info
	Warning = store.Warning
	Error   = store.Error
)

// ErrDeviceNotFound is returned by DeviceDB.GetDevice for an unknown name.
var ErrDeviceNotFound = store.ErrDeviceNotFound

// OpenDeviceDB opens the database and migrates its schema.
func OpenDeviceDB(config timeseries.DBConfig) (*DeviceDB, error) {
	return store.OpenDeviceDB(config)
}
