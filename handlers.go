package iotedge

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-contrib/sessions"
	"github.com/gin-gonic/gin"
	"github.com/pat-rohn/timeseries"
	log "github.com/sirupsen/logrus"
)

type LoginRequest struct {
	Password string `json:"password"`
}

// DashboardResponse carries both log messages and device configs for JSON clients.
type DashboardResponse struct {
	Logs    []LogMessage   `json:"Logs"`
	Devices []DeviceConfig `json:"Devices"`
}

func (s *IoTEdge) LoginPageHandler(c *gin.Context) {
	c.HTML(http.StatusOK, "login.html", gin.H{})
}

func (s *IoTEdge) DashboardHandler(c *gin.Context) {
	logFields := log.Fields{"fnct": "DashboardHandler"}
	logs, err := GetLoggingDB(s.IoTConfig.DbConfig).GetLogMessages(100)
	if err != nil {
		log.WithFields(logFields).Errorf("Failed to get log messages: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to retrieve logs"})
		return
	}
	devices, err := s.DeviceDB.GetDevicesConfigs()
	if err != nil {
		log.WithFields(logFields).Errorf("Failed to get devices: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to retrieve devices"})
		return
	}
	if c.GetHeader("Accept") == "application/json" {
		c.JSON(http.StatusOK, DashboardResponse{Logs: logs, Devices: devices})
		return
	}
	c.HTML(http.StatusOK, "dashboard.html", gin.H{"Logs": logs, "Devices": devices})
}

func (w *IoTEdge) performLogin(c *gin.Context) {
	logFields := log.Fields{"fnct": "LoginHandler"}
	var loginReq LoginRequest
	if err := c.BindJSON(&loginReq); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request format"})
		return
	}
	if loginReq.Password == w.password {
		session := sessions.Default(c)
		session.Set("user", "any")
		if err := session.Save(); err != nil {
			log.WithFields(logFields).Errorf("Failed to save session: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to establish session"})
			return
		}
		c.Redirect(http.StatusFound, "/dashboard")
	} else {
		c.String(http.StatusUnauthorized, "Incorrect credentials")
	}
	log.WithFields(logFields).Info("login attempt")
}

func (s *IoTEdge) AuthenticationHandler(c *gin.Context) {
	session := sessions.Default(c)
	user := session.Get("user")
	if user == nil {
		c.Redirect(http.StatusFound, "/")
		c.Abort()
		return
	}
	c.Next()
}

func (s *IoTEdge) SaveTimeseriesHandler(c *gin.Context) {
	logFields := log.Fields{"fnct": "SaveTimeseries"}
	var data []timeseries.TimeseriesImportStruct
	if err := c.BindJSON(&data); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	for _, ts := range data {
		if err := s.DeviceDB.InsertTimeseries(ts, true, s.IoTConfig.TimeseriesTable); err != nil {
			log.WithFields(logFields).Errorf("Failed to save timeseries: %+v ", err.Error())
			c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to save timeseries: %v", err)})
			return
		}
	}
	s.SetGinHeaders(c)
	c.JSON(http.StatusOK, gin.H{"success": true})
}

func (s *IoTEdge) UploadDataHandler(c *gin.Context) {
	logFields := log.Fields{"fnct": "UploadDataHandler"}
	if c.Request.Method == http.MethodOptions {
		s.SetGinHeaders(c)
		return
	}
	if c.Request.Method == http.MethodGet {
		c.JSON(http.StatusOK, Output{Status: "OK", Answer: "Okay"})
		return
	}
	var data []timeseries.TimeseriesImportStruct
	if err := c.BindJSON(&data); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Input error: %v", err)})
		return
	}
	for _, val := range data {
		if err := s.DeviceDB.InsertTimeseries(val, true, s.IoTConfig.TimeseriesTable); err != nil {
			log.WithFields(logFields).Errorf("Failed to insert: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to insert values: %v", err)})
			return
		}
	}
	c.JSON(http.StatusOK, Output{Status: "OK", Answer: "Success"})
}

func (s *IoTEdge) InitDeviceHandler(c *gin.Context) {
	logFields := log.Fields{"fnct": "InitDevice"}
	var deviceReq struct {
		DeviceDesc DeviceDesc `json:"Device"`
	}
	if err := c.BindJSON(&deviceReq); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Input error: %v", err)})
		return
	}
	dev, err := s.Init(deviceReq.DeviceDesc)
	if err != nil {
		log.WithFields(logFields).Warnf("init device %s failed: %v", deviceReq.DeviceDesc.Name, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("init device %s failed: %v", deviceReq.DeviceDesc.Name, err)})
		return
	}
	s.SetGinHeaders(c)
	c.JSON(http.StatusOK, dev)
}

func (s *IoTEdge) ConfigureDeviceHandler(c *gin.Context) {
	logFields := log.Fields{"fnct": "ConfigureDevice"}
	var p ConfigureDeviceReq
	if err := c.BindJSON(&p); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Input error: %v", err)})
		return
	}
	log.WithFields(logFields).Infof("Value: %+v", p)
	dev, err := s.DeviceDB.GetDevice(p.Name)
	if err != nil {
		if errors.Is(err, ErrDeviceNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": fmt.Sprintf("device '%s' not found", p.Name)})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("getting device failed: %v", err)})
		}
		return
	}
	dev.Interval = p.Interval
	dev.Buffer = p.Buffer
	if err = s.DeviceDB.Configure(dev); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("configuring device failed: %v", err)})
		return
	}
	s.SetGinHeaders(c)
	c.JSON(http.StatusOK, dev)
}

func (s *IoTEdge) ConfSensorHandler(c *gin.Context) {
	logFields := log.Fields{"fnct": "ConfSensor"}
	var p ConfigureSensorReq
	if err := c.BindJSON(&p); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("input error: %v", err)})
		return
	}
	log.WithFields(logFields).Infof("Value: %+v", p)
	dev, err := s.DeviceDB.GetDevice(p.Name)
	if err != nil {
		if errors.Is(err, ErrDeviceNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": fmt.Sprintf("device '%s' not found", p.Name)})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("getting device failed: %v", err)})
		}
		return
	}
	sensors, err := s.DeviceDB.GetSensors(dev.ID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("getting sensors failed: %v", err)})
		return
	}
	found := false
	for _, ses := range sensors {
		if ses.Name == p.SensorName {
			found = true
			updateSensor := ses
			updateSensor.SensorOffset = p.SensorOffset
			updateSensor.DeviceID = dev.ID
			if err = s.DeviceDB.ConfigureSensor(updateSensor); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("configuring sensor failed: %v", err)})
				return
			}
		}
	}
	if !found {
		c.JSON(http.StatusNotFound, gin.H{"error": fmt.Sprintf("sensor '%s' not found on device '%s'", p.SensorName, p.Name)})
		return
	}
	s.SetGinHeaders(c)
	c.JSON(http.StatusOK, dev)
}

func (s *IoTEdge) UpdateSensorHandler(c *gin.Context) {
	logFields := log.Fields{"fnct": "UpdateSensorHandler"}
	if c.Request.Method == http.MethodGet {
		s.SetGinHeaders(c)
		c.JSON(http.StatusOK, Output{Status: "OK", Answer: "Okay"})
		return
	}
	var p sensorValues
	if err := c.BindJSON(&p); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Input error: %v", err)})
		return
	}
	for _, val := range p.Data {
		tsVal := timeseries.TimeseriesImportStruct{
			Tag:        val.Name,
			Timestamps: []string{time.Now().UTC().Format("2006-01-02 15:04:05.000")},
			Values:     []string{fmt.Sprintf("%f", val.Value)},
			Comments:   p.Tags,
		}
		if err := s.DeviceDB.InsertTimeseries(tsVal, true, s.IoTConfig.TimeseriesTable); err != nil {
			log.WithFields(logFields).Errorf("Failed to insert: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to insert values: %v", err)})
			return
		}
	}
	s.SetGinHeaders(c)
	c.JSON(http.StatusOK, Output{Status: "OK", Answer: "Success"})
}

func (s *IoTEdge) LogHandler(c *gin.Context) {
	logFields := log.Fields{"fnct": "Log"}
	var logMsg LogMessage
	if err := c.BindJSON(&logMsg); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Input error: %v", err)})
		return
	}
	log.WithFields(logFields).Infof("Value: %+v", logMsg)
	// Auto-register the device so that logging alone is enough to create a
	// devices-table entry (devices that only call /api/log never hit /init-device).
	if logMsg.Device != "" {
		if _, err := s.DeviceDB.GetOrCreateDevice(DeviceDesc{Name: logMsg.Device}); err != nil {
			log.WithFields(logFields).Warnf("auto-register device %q failed: %v", logMsg.Device, err)
		}
	}
	s.SetGinHeaders(c)
	if err := s.LogMessage(logMsg); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to log message: %v", err)})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "success"})
}

// SetGinHeaders sets CORS headers on the response. The reflected Origin is only
// echoed back when it appears in IoTConfig.AllowedOrigins. If the allow-list is
// empty it defaults to http://localhost and https://localhost.
func (s *IoTEdge) SetGinHeaders(c *gin.Context) {
	origin := c.GetHeader("Origin")
	log.Tracef("origin from header: %+s", origin)
	allowed := s.IoTConfig.AllowedOrigins
	if len(allowed) == 0 {
		allowed = []string{"http://localhost", "https://localhost"}
	}
	for _, a := range allowed {
		if a == origin {
			c.Header("Access-Control-Allow-Origin", origin)
			c.Header("Access-Control-Allow-Credentials", "true")
			break
		}
	}
	c.Header("Access-Control-Allow-Methods", "PUT, POST, PATCH, OPTIONS, GET, DELETE")
	c.Header("Access-Control-Allow-Headers", "content-type")
	c.Header("Access-Control-Max-Age", "240")
}
