package iotedge

import (
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/pat-rohn/timeseries"

	log "github.com/sirupsen/logrus"
)

func (s *IoTEdge) SaveTimeseries(c *gin.Context) {
	logFields := log.Fields{"fnct": "SaveTimeseries"}
	var data []timeseries.TimeseriesImportStruct

	if err := c.BindJSON(&data); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	log.Infof("Received data.%+v", data)
	log.Tracef("%+v", data)

	for _, ts := range data {
		log.Infof("insert %v", ts.Tag)
		if err := s.DeviceDB.InsertTimeseries(ts, true, s.IoTConfig.TimeseriesTable); err != nil {
			log.WithFields(logFields).Errorf("Failed to save timeseries: %+v ", err.Error())
			c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to save timeseries: %v", err)})
			return
		}
	}

	c.Header("Access-Control-Allow-Origin", "*")
	c.JSON(http.StatusOK, gin.H{"success": true})
}

func (s *IoTEdge) UploadDataHandler(c *gin.Context) {
	logFields := log.Fields{"fnct": "UploadDataHandler"}
	log.WithFields(logFields).Infof("Got request: %v", c.Request.URL)

	// Handle OPTIONS request
	if c.Request.Method == http.MethodOptions {
		SetGinHeaders(c)
		return
	}

	if c.Request.Method == http.MethodGet {
		c.JSON(http.StatusOK, Output{Status: "OK", Answer: "Okay"})
		return
	}

	var data []timeseries.TimeseriesImportStruct
	if err := c.BindJSON(&data); err != nil {
		log.WithFields(logFields).Errorf("Input error: %+v ", err.Error())
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Input error: %v", err)})
		return
	}

	log.WithFields(logFields).Infof("Value: %+v ", data)

	for _, val := range data {
		if err := s.DeviceDB.InsertTimeseries(val, true, s.IoTConfig.TimeseriesTable); err != nil {
			log.Errorf("Failed to insert values into database: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to insert values into database: %v", err)})
			return
		}
	}

	c.JSON(http.StatusOK, Output{Status: "OK", Answer: "Success"})
}

func (s *IoTEdge) InitDevice(c *gin.Context) {
	logFields := log.Fields{"fnct": "InitDevice"}
	log.WithFields(logFields).Infof("Got request: %v", c.Request.URL)

	var deviceReq struct {
		DeviceDesc DeviceDesc `json:"Device"`
	}

	if err := c.BindJSON(&deviceReq); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Input error: %v", err)})
		return
	}

	logFields["Name"] = deviceReq.DeviceDesc.Name
	logFields["Description"] = deviceReq.DeviceDesc.Description
	log.WithFields(logFields).Infof("Value: %+v", deviceReq)

	dev, err := s.Init(deviceReq.DeviceDesc)
	if err != nil {
		log.WithFields(logFields).Warnf("init device %s failed: %v", deviceReq.DeviceDesc.Name, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("init device %s failed: %v", deviceReq.DeviceDesc.Name, err)})
		return
	}

	log.WithFields(logFields).Infof("device initialized: %+v", dev)
	SetGinHeaders(c)
	c.JSON(http.StatusOK, dev)
}

func (s *IoTEdge) ConfigureDevice(c *gin.Context) {
	logFields := log.Fields{"fnct": "ConfigureDevice"}
	log.WithFields(logFields).Infof("Got request: %v", c.Request.URL)

	var p ConfigureDeviceReq
	if err := c.BindJSON(&p); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Input error: %v", err)})
		return
	}

	log.WithFields(logFields).Infof("Value: %+v", p)
	dev, err := s.DeviceDB.GetDevice(p.Name)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("getting device failed: %v", err)})
		return
	}

	dev.Interval = p.Interval
	dev.Buffer = p.Buffer
	if err = s.DeviceDB.Configure(dev); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("configuring device failed: %v", err)})
		return
	}

	SetGinHeaders(c)
	c.JSON(http.StatusOK, dev)
}

func (s *IoTEdge) ConfSensor(c *gin.Context) {
	logFields := log.Fields{"fnct": "ConfSensor"}
	log.WithFields(logFields).Infof("Got request: %v", c.Request.URL)

	var p ConfigureSensorReq
	if err := c.BindJSON(&p); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("input error: %v", err)})
		return
	}

	log.WithFields(logFields).Infof("Value: %+v", p)
	dev, err := s.DeviceDB.GetDevice(p.Name)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("getting device failed: %v", err)})
		return
	}

	sensors, err := s.DeviceDB.GetSensors(dev.ID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("getting sensors failed: %v", err)})
		return
	}

	for _, ses := range sensors {
		if ses.Name == p.SensorName {
			updateSensor := ses
			updateSensor.SensorOffset = p.SensorOffset
			updateSensor.DeviceID = dev.ID

			if err = s.DeviceDB.ConfigureSensor(updateSensor); err != nil {
				log.WithFields(logFields).Errorf("configuring sensor failed: %v", err)
				c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("configuring sensor failed: %v", err)})
				return
			}
		}
	}

	SetGinHeaders(c)
	c.JSON(http.StatusOK, dev)
}

func (s *IoTEdge) UpdateSensorHandler(c *gin.Context) {
	logFields := log.Fields{"fnct": "UpdateSensorHandler"}
	log.WithFields(logFields).Infof("Got request: %v", c.Request.URL)

	if c.Request.Method == http.MethodGet {
		SetGinHeaders(c)
		c.JSON(http.StatusOK, Output{Status: "OK", Answer: "Okay"})
		return
	}

	var p sensorValues
	if err := c.BindJSON(&p); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Input error: %v", err)})
		return
	}

	log.WithFields(logFields).Infof("Value: %+v", p)

	for _, val := range p.Data {
		tsVal := timeseries.TimeseriesImportStruct{
			Tag:        val.Name,
			Timestamps: []string{time.Now().UTC().Format("2006-01-02 15:04:05.000")},
			Values:     []string{fmt.Sprintf("%f", val.Value)},
			Comments:   p.Tags,
		}

		if err := s.DeviceDB.InsertTimeseries(tsVal, true, s.IoTConfig.TimeseriesTable); err != nil {
			log.Errorf("Failed to insert values into database: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to insert values into database: %v", err)})
			return
		}
	}

	SetGinHeaders(c)
	c.JSON(http.StatusOK, Output{Status: "OK", Answer: "Success"})
}

func (s *IoTEdge) Log(c *gin.Context) {
	logFields := log.Fields{"fnct": "Log"}
	log.WithFields(logFields).Infof("Got request: %v", c.Request.URL)

	var logMsg LogMessage
	if err := c.BindJSON(&logMsg); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Input error: %v", err)})
		return
	}

	log.WithFields(logFields).Infof("Value: %+v", logMsg)
	SetGinHeaders(c)
	if err := s.LogMessage(logMsg); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to log message: %v", err)})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "success"})
}

// Helper function for CORS headers
func SetGinHeaders(c *gin.Context) {
	origin := c.GetHeader("Origin")
	log.Tracef("origin from header: %+s", origin)
	c.Header("Access-Control-Allow-Origin", origin)
	c.Header("Access-Control-Allow-Credentials", "true")
	c.Header("Access-Control-Allow-Methods", "PUT, POST, PATCH, OPTIONS, GET, DELETE")
	c.Header("Access-Control-Allow-Headers", "content-type")
	c.Header("Access-Control-Max-Age", "240")
}
