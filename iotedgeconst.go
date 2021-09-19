package iotedge

import timeseries "github.com/pat-rohn/timeseries"

type IoTEdge struct {
	Port           int
	DatabaseConfig timeseries.DBConfig
}

const (
	HTTPPort          int    = 3004
	URIUpdateSensor   string = "/update-sensor"
	URIUploadData     string = "/upload-data"
	URISaveTimeseries string = "/timeseries/save"
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
