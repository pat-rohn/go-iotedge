package iotedge

import (
	"github.com/pat-rohn/timeseries"
)

type IoTEdge struct {
	Port               int
	DeviceDBConfig     timeseries.DBConfig
	TimeseriesDBConfig timeseries.DBConfig
	Timeseries         *timeseries.DbHandler
	DeviceDB           *DeviceDB
}

const (
	HTTPPort           int    = 3004
	URIInitDevice      string = "/init-device"
	URIUpdateSensor    string = "/update-sensor"
	URIDeviceConfigure string = "/device/configure"
	URISensorConfigure string = "/sensor/configure"
	URIUploadData      string = "/upload-data"
	URISaveTimeseries  string = "/timeseries/save"
	URILogging         string = "/log"
)

type Output struct {
	Status string      `json:"Status"`
	Answer interface{} `json:"Answer"`
}

type Input struct {
	Method     string      `json:"Method"`
	MethodBody interface{} `json:"MethodBody"`
}

type TimeSeriesValue struct {
	Name  string
	Value float32
}

type sensorValues struct {
	Tags []string          `json:"Tags"`
	Data []TimeSeriesValue `json:"Data"`
}

type DeviceDesc struct {
	Name        string
	Sensors     []string
	Description string
}

type Sensor struct {
	ID       int
	DeviceID int
	Name     string
	Offset   float32
}

type Device struct {
	ID          int
	Name        string
	Interval    float32
	Buffer      int
	Description string
}

type ConfigureSensorReq struct {
	Name       string
	SensorName string
	Offset     float32
}

type ConfigureDeviceReq struct {
	Name     string
	Interval float32
	Buffer   int
}

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
