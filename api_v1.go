package iotedge

import (
	_ "embed"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

// openAPISpec documents the /api/v1 routes; a test keeps both in sync.
//
//go:embed openapi.yaml
var openAPISpec []byte

// registerAPIv1 adds the versioned JSON API. Device routes take the same
// bodies as the firmware's paths (still served as aliases) and check the
// ingest token; admin routes need a dashboard login.
func (s *IoTEdge) registerAPIv1(router *gin.Engine) {
	v1 := router.Group("/api/v1")
	ingest := s.ingestAuth.handler
	v1.POST("/devices", ingest, s.InitDeviceHandler)
	v1.POST("/measurements", ingest, s.SaveTimeseriesHandler)
	v1.POST("/readings", ingest, s.UpdateSensorHandler)
	v1.POST("/logs", ingest, s.LogHandler)
	v1.POST("/health", ingest, s.MonitoringHealthHandler)
	v1.GET("/version", s.VersionHandler)
	v1.GET("/openapi.yaml", func(c *gin.Context) {
		c.Data(http.StatusOK, "application/yaml", openAPISpec)
	})

	admin := v1.Group("", s.APIAuthenticationHandler)
	admin.GET("/devices", s.apiListDevices)
	admin.PUT("/devices/:name", s.apiConfigureDevice)
	admin.PUT("/devices/:name/sensors/:sensor", s.apiConfigureSensor)
	admin.GET("/logs", s.apiListLogs)
	admin.GET("/monitoring", s.apiMonitoring)
}

func (s *IoTEdge) apiListDevices(c *gin.Context) {
	devices, err := s.DeviceDB.GetDevicesConfigs()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to read devices"})
		return
	}
	if devices == nil {
		devices = []DeviceConfig{}
	}
	c.JSON(http.StatusOK, devices)
}

func (s *IoTEdge) apiConfigureDevice(c *gin.Context) {
	var body struct {
		Interval float32
		Buffer   int
	}
	if err := c.BindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("input error: %v", err)})
		return
	}
	s.configureDevice(c, ConfigureDeviceReq{Name: c.Param("name"), Interval: body.Interval, Buffer: body.Buffer})
}

func (s *IoTEdge) apiConfigureSensor(c *gin.Context) {
	var body struct{ SensorOffset float32 }
	if err := c.BindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("input error: %v", err)})
		return
	}
	s.configureSensor(c, ConfigureSensorReq{Name: c.Param("name"), SensorName: c.Param("sensor"), SensorOffset: body.SensorOffset})
}

// apiListLogs answers ?device=&level=&limit=(1..100, default 100)&offset=.
func (s *IoTEdge) apiListLogs(c *gin.Context) {
	limit, offset := 100, 0
	var err error
	if raw := c.Query("limit"); raw != "" {
		if limit, err = strconv.Atoi(raw); err != nil || limit < 1 || limit > 100 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "limit must be between 1 and 100"})
			return
		}
	}
	if raw := c.Query("offset"); raw != "" {
		if offset, err = strconv.Atoi(raw); err != nil || offset < 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "offset must not be negative"})
			return
		}
	}
	filter := LogFilter{Device: c.Query("device")}
	if raw := c.Query("level"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < int(Debug) || value > int(Error) {
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("level must be between %d and %d", Debug, Error)})
			return
		}
		level := Loglevel(value)
		filter.Level = &level
	}
	logs, err := s.LoggingDB.GetLogMessagesPage(limit, offset, filter)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to read logs"})
		return
	}
	if logs == nil {
		logs = []LogMessage{}
	}
	c.JSON(http.StatusOK, logs)
}

func (s *IoTEdge) apiMonitoring(c *gin.Context) {
	summary, err := s.DeviceDB.GetMonitoringSummary(s.IoTConfig.Monitoring, time.Now())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to read monitoring summary"})
		return
	}
	c.JSON(http.StatusOK, summary)
}
