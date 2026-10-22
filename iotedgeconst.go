package iotedge

import (
	"strings"
	"time"

	"github.com/gin-contrib/sessions/cookie"
)

type IoTEdge struct {
	Port      int
	IoTConfig IoTConfig
	DeviceDB  *DeviceDB
	store     cookie.Store
	password  string
	// ingestAuth is set by newRouter from IoTConfig.Security.
	ingestAuth *ingestAuth
}

const (
	HTTPPort int = 3004

	URILogin     string = "/login"
	URILogout    string = "/logout"
	URIDashboard string = "/dashboard"
	URIVersion   string = "/version"

	URIInitDevice       string = "/init-device"
	URIUpdateSensor     string = "/update-sensor"
	URIDeviceConfigure  string = "/device/configure"
	URISensorConfigure  string = "/sensor/configure"
	URIUploadData       string = "/upload-data"
	URISaveTimeseries   string = "/timeseries/save"
	URILogging          string = "/api/log"
	URIMonitoringHealth string = "/monitoring/health"
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
	ID           int
	DeviceID     int
	Name         string
	SensorOffset float32
}

type Device struct {
	ID          int
	Name        string
	Interval    float32
	Buffer      int
	Description string
}

type DeviceConfig struct {
	ID          int
	Name        string
	Interval    float32
	Buffer      int
	Description string
	Sensors     []Sensor
	// Address is the IP the device last sent from; AddressSeenAt is when.
	Address       string
	AddressSeenAt *time.Time
}

// URL is the device's own web page, or "" when its address is unknown.
func (d DeviceConfig) URL() string {
	if d.Address == "" {
		return ""
	}
	host := d.Address
	if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	return "http://" + host + "/"
}

type ConfigureSensorReq struct {
	Name         string
	SensorName   string
	SensorOffset float32
}

type ConfigureDeviceReq struct {
	Name     string
	Interval float32
	Buffer   int
}
