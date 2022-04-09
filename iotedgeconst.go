package iotedge

import (
	"time"

	"github.com/jinzhu/gorm"
	timeseries "github.com/pat-rohn/timeseries"
)

type IoTEdge struct {
	Port           int
	DatabaseConfig timeseries.DBConfig
}

const (
	HTTPPort           int    = 3004
	URIInitDevice      string = "/init-device"
	URIUpdateSensor    string = "/update-sensor"
	URIUploadData      string = "/upload-data"
	URISaveTimeseries  string = "/timeseries/save"
	DeviceDatabaseName string = "devices.db"
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
	Name    string
	Sensors []string
}

type Device struct {
	gorm.Model
	Name    string   `gorm:"unique"`
	Sensors []Sensor `gorm:"foreignKey:DeviceID;references:SensorID;constraint:OnUpdate:CASCADE,OnDelete:SET NULL;"`
}

type Sensor struct {
	gorm.Model
	SensorName         string
	Offset             float64
	AquisitionInterval time.Duration
	DeviceID           uint
}
