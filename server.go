package iotedge

import (
	"context"
	"embed"
	"fmt"
	"html/template"
	"net/http"
	"os"
	"time"

	"github.com/gin-contrib/sessions"
	"github.com/gin-contrib/sessions/cookie"
	"github.com/gin-gonic/gin"
	"github.com/pat-rohn/timeseries"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

const (
	defaultPassword = "123"

	// Shipped config defaults — publicly known, so warn when they are in use.
	defaultConfigPassword      = "changeme"
	defaultConfigSessionSecret = "changeme-session-secret-min32bytes!!"
)

var (
	//go:embed static/*
	staticFS embed.FS
	//go:embed templates/*
	templatesFS embed.FS
	templates   *template.Template
)

type IoTConfig struct {
	Verbosity           string
	Port                int
	MQTTPort            int
	MQTTRedirectAddress string
	DbConfig            timeseries.DBConfig
	TimeseriesTable     string
	UploadInterval      int
	Password            string
	SessionSecret       string
	AllowedOrigins      []string
}

func New(iotConfig IoTConfig) IoTEdge {
	logFields := log.Fields{"fnct": "New"}
	log.WithFields(logFields).Tracef("Config %+v", iotConfig)
	s := IoTEdge{Port: iotConfig.Port, IoTConfig: iotConfig}
	s.DeviceDB = GetDeviceDB(iotConfig.DbConfig)
	if err := s.DeviceDB.CreateTimeseriesTable(iotConfig.TimeseriesTable); err != nil {
		log.Fatalf("failed to create table: %v", err)
	}
	loggerDB := GetLoggingDB(iotConfig.DbConfig)
	if loggerDB == nil {
		log.Fatalf("failed to create logging DB")
	}
	return s
}

func GetConfig() IoTConfig {
	logFields := log.Fields{"fnct": "GetConfig"}
	viper.SetDefault("Verbosity", "info")
	viper.SetDefault("Port", 3004)
	viper.SetDefault("MQTTPort", 1883)
	viper.SetDefault("MQTTRedirectAddress", "")
	viper.SetDefault("UploadInterval", 30)
	viper.SetDefault("DBConfig.Name", "iot.db")
	viper.SetDefault("DBConfig.IPOrPath", "./")
	viper.SetDefault("DBConfig.UsePostgres", false)
	viper.SetDefault("DBConfig.User", "user")
	viper.SetDefault("DBConfig.Password", "password")
	viper.SetDefault("DBConfig.Port", 5432)
	viper.SetDefault("DBConfig.TableName", "configs")
	viper.SetDefault("TimeseriesTable", "measurements")
	viper.SetDefault("Password", defaultConfigPassword)
	viper.SetDefault("SessionSecret", defaultConfigSessionSecret)
	viper.SetDefault("AllowedOrigins", []string{})
	viper.SetConfigName("iot")
	viper.SetConfigType("json")
	dirname, err := os.UserHomeDir()
	if err != nil {
		log.Error(err)
		dirname = "."
	}
	pathToConfig := dirname + "/.iotserver"
	viper.AddConfigPath(pathToConfig)
	viper.AddConfigPath(".")
	log.WithFields(logFields).Infoln("Read Config")
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			log.Warnf("no config file found: %v", err)
			if err := os.MkdirAll(pathToConfig, 0755); err != nil {
				log.Fatal(fmt.Sprintf("Creating config folder failed: %v", err))
			}
			err = viper.SafeWriteConfig()
			if err != nil {
				log.Fatal(fmt.Sprintf("Storing default config failed: %v", err))
			}
		} else {
			log.Fatal(fmt.Sprintf("Loading config failed: %v", err))
		}
	}
	var iotConfig IoTConfig
	err = viper.Unmarshal(&iotConfig)
	if err != nil {
		log.Fatalf("fatal error config file: %v ", err)
	}
	return iotConfig
}

func (s *IoTEdge) StartSensorServer(stopChan chan bool) error {
	logFields := log.Fields{"fnct": "startHTTPListener"}
	staticServer := http.FileServer(http.FS(staticFS))

	sessionSecret := s.IoTConfig.SessionSecret
	switch sessionSecret {
	case "":
		sessionSecret = defaultPassword
		log.Warn("SessionSecret not configured, using insecure default — set SessionSecret in config")
	case defaultConfigSessionSecret:
		log.Warn("SessionSecret is the publicly known shipped default — sessions are forgeable; set a unique SessionSecret in config")
	}
	s.store = cookie.NewStore([]byte(sessionSecret))

	s.password = s.IoTConfig.Password
	switch s.password {
	case "":
		s.password = defaultPassword
		log.Warn("Password not configured, using insecure default — set Password in config")
	case defaultConfigPassword:
		log.Warn("Password is the publicly known shipped default — set a unique Password in config")
	}

	// TODO: set Secure: true when TLS is properly terminated end-to-end.
	s.store.Options(sessions.Options{
		Path: "/", MaxAge: 6000, HttpOnly: true, Secure: false,
	})

	gin.SetMode(gin.ReleaseMode)
	router := gin.Default()
	router.Use(sessions.Sessions("edge-server-session", s.store))
	// CORS headers on every response (including error paths) and a proper
	// preflight answer for OPTIONS requests.
	router.Use(func(c *gin.Context) {
		s.SetGinHeaders(c)
		if c.Request.Method == http.MethodOptions {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}
		c.Next()
	})

	router.GET("/static/css/*filepath", func(c *gin.Context) {
		path := c.Param("filepath")
		c.Request.URL.Path = "/static/css" + path
		staticServer.ServeHTTP(c.Writer, c.Request)
	})
	router.GET("/static/resources/*filepath", func(c *gin.Context) {
		path := c.Param("filepath")
		c.Request.URL.Path = "/static/resources" + path
		staticServer.ServeHTTP(c.Writer, c.Request)
	})

	templates = template.Must(template.ParseFS(templatesFS, "templates/*.html"))
	router.SetHTMLTemplate(templates)

	router.GET("/", s.LoginPageHandler)
	router.POST(URILogin, s.performLogin)
	router.GET(URIDashboard, s.AuthenticationHandler, s.DashboardHandler)
	router.POST(URIUploadData, s.UploadDataHandler)
	router.POST(URISaveTimeseries, s.SaveTimeseriesHandler)
	router.POST(URIInitDevice, s.InitDeviceHandler)
	router.POST(URIUpdateSensor, s.UpdateSensorHandler)
	router.POST(URISensorConfigure, s.ConfSensorHandler)
	router.POST(URIDeviceConfigure, s.ConfigureDeviceHandler)
	router.POST(URILogging, s.LogHandler)

	server := &http.Server{
		Addr:    fmt.Sprintf(":%v", s.Port),
		Handler: router,
	}

	errChan := make(chan error, 1)
	go func() {
		fmt.Printf("Listen on port: %v\n", s.Port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errChan <- err
		}
	}()

	select {
	case <-stopChan:
		log.WithFields(logFields).Info("Shutting down server gracefully...")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	case err := <-errChan:
		log.WithFields(logFields).Errorf("Listen and serve failed: %v.", err)
		return err
	}
}
