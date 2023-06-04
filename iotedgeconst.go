package iotedge

import (
	"database/sql"

	timeseries "github.com/pat-rohn/timeseries"
	"golang.org/x/sync/semaphore"
)

type IoTEdge struct {
	Port               int
	DeviceDBConfig     timeseries.DBConfig
	TimeseriesDBConfig timeseries.DBConfig
	DB                 *sql.DB
	sem                *semaphore.Weighted
	semTimeseries      *semaphore.Weighted
	Timeseries         timeseries.DbHandler
}

const (
	HTTPPort           int    = 3004
	URIInitDevice      string = "/init-device"
	URIUpdateSensor    string = "/update-sensor"
	URIDeviceConfigure string = "/device/configure"
	URISensorConfigure string = "/sensor/configure"
	URIUploadData      string = "/upload-data"
	URISaveTimeseries  string = "/timeseries/save"
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
