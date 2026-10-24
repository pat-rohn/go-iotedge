package iotedge

import (
	"time"

	"github.com/gin-contrib/sessions/cookie"
)

type IoTEdge struct {
	Port      int
	IoTConfig IoTConfig
	DeviceDB  *DeviceDB
	LoggingDB *LoggingDB
	store     cookie.Store
	password  string
	// ingestAuth is set by newRouter from IoTConfig.Security.
	ingestAuth *ingestAuth
	// liveInterval is how often the overview stream checks for changes
	// (defaultLiveInterval when zero).
	liveInterval time.Duration
	// status is what the system page reads from the running parts; shared
	// by copies of the IoTEdge value.
	status *serverStatus
	// loginLimiter slows down password guessing; set by newRouter.
	loginLimiter *loginLimiter
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
