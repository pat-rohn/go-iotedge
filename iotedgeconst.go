package iotedge

import (
	"github.com/jinzhu/gorm"
	timeseries "github.com/pat-rohn/timeseries"
)

type IoTEdge struct {
	Port           int
	DatabaseConfig timeseries.DBConfig `json:"DbConfig"`
}

const (
	HTTPPort           int    = 3004
	URIInitDevice      string = "/init-device"
	URIUpdateSensor    string = "/update-sensor"
	URIUploadData      string = "/upload-data"
	URISaveTimeseries  string = "/timeseries/save"
	URIDeviceConfigure string = "/device/configure"
	URISensorConfigure string = "/sensor/configure"
)

type Output struct {
	Status string      `json:"Status"`
	Answer interface{} `json:"Answer"`
}

type Input struct {
	Method     string      `json:"Method"`
	MethodBody interface{} `json:"MethodBody"`
}

type timeSeriesValue struct {
	Name  string
	Value float32
}

type sensorValues struct {
	Tags []string          `json:"Tags"`
	Data []timeSeriesValue `json:"Data"`
}

type DeviceDesc struct {
	Name        string
	Sensors     []string
	Description string
}

type Sensor struct {
	gorm.Model
	Name     string
	Offset   float32
	DeviceID int
}

type Device struct {
	gorm.Model
	Name        string `gorm:"unique"`
	Sensors     []Sensor
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
