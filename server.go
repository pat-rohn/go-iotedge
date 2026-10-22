package iotedge

import (
	"context"
	"embed"
	"fmt"
	"html/template"
	"net/http"
	"os"
	"strings"
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

	sessionCookieName = "edge-server-session"
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
	Monitoring          MonitoringConfig
	Security            SecurityConfig
	// LogPersistLevel is the lowest server log level stored in the logs table
	// (logrus names: "debug", "info", "warning", "error").
	LogPersistLevel string
}

// SecurityConfig holds opt-in hardening. Every switch defaults to false, so a
// server with an old config behaves as before. None of them affects the
// endpoints the firmware uses.
type SecurityConfig struct {
	// RequireCredentials refuses to start while Password or SessionSecret is
	// empty or the shipped default.
	RequireCredentials bool
	// ProtectAdminRoutes requires a dashboard session for /device/configure
	// and /sensor/configure.
	ProtectAdminRoutes bool
}

func New(iotConfig IoTConfig) IoTEdge {
	logFields := log.Fields{"fnct": "New"}
	log.WithFields(logFields).Tracef("Config %+v", iotConfig)
	s := IoTEdge{Port: iotConfig.Port, IoTConfig: iotConfig}
	s.DeviceDB = GetDeviceDB(iotConfig.DbConfig)
	if err := s.DeviceDB.CreateTimeseriesTable(iotConfig.TimeseriesTable); err != nil {
		log.Fatalf("failed to create table: %v", err)
	}
	s.IoTConfig.Monitoring = s.IoTConfig.Monitoring.normalized()
	if err := s.IoTConfig.Monitoring.validate(); err != nil {
		log.Fatalf("invalid monitoring configuration: %v", err)
	}
	if err := s.DeviceDB.InitializeMonitoring(); err != nil {
		log.Fatalf("failed to initialize monitoring tables: %v", err)
	}
	if s.IoTConfig.LogPersistLevel == "" {
		s.IoTConfig.LogPersistLevel = "warning"
	}
	persistLevel, err := log.ParseLevel(s.IoTConfig.LogPersistLevel)
	if err != nil {
		log.Fatalf("invalid LogPersistLevel %q: %v", s.IoTConfig.LogPersistLevel, err)
	}
	setLogPersistLevel(persistLevel)
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
	viper.SetDefault("Monitoring.OnlineWindowMinutes", defaultOnlineWindowMinutes)
	viper.SetDefault("Monitoring.RestartWindowDays", defaultRestartWindowDays)
	viper.SetDefault("Monitoring.HomeAssistantDiscovery", false)
	viper.SetDefault("Security.RequireCredentials", false)
	viper.SetDefault("Security.ProtectAdminRoutes", false)
	viper.SetDefault("LogPersistLevel", "warning")
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
	} else {
		log.WithFields(logFields).Infof("Using config file: %s", viper.ConfigFileUsed())
	}
	var iotConfig IoTConfig
	err = viper.Unmarshal(&iotConfig)
	if err != nil {
		log.Fatalf("fatal error config file: %v ", err)
	}
	return iotConfig
}

// checkCredentials returns an error for an empty or shipped default Password or
// SessionSecret when Security.RequireCredentials is set; otherwise it only warns.
func checkCredentials(cfg IoTConfig) error {
	var problems []string
	switch cfg.SessionSecret {
	case "":
		problems = append(problems, "SessionSecret is not configured")
	case defaultConfigSessionSecret:
		problems = append(problems, "SessionSecret is the publicly known shipped default — sessions are forgeable")
	}
	switch cfg.Password {
	case "":
		problems = append(problems, "Password is not configured")
	case defaultConfigPassword:
		problems = append(problems, "Password is the publicly known shipped default")
	}
	if len(problems) == 0 {
		return nil
	}
	if cfg.Security.RequireCredentials {
		return fmt.Errorf("refusing to start (Security.RequireCredentials): %s", strings.Join(problems, "; "))
	}
	for _, p := range problems {
		log.Warnf("%s — set a unique value in config", p)
	}
	return nil
}

// newRouter checks the credentials and builds the HTTP router with all routes.
func (s *IoTEdge) newRouter() (*gin.Engine, error) {
	if err := checkCredentials(s.IoTConfig); err != nil {
		return nil, err
	}
	staticServer := http.FileServer(http.FS(staticFS))

	sessionSecret := s.IoTConfig.SessionSecret
	if sessionSecret == "" {
		sessionSecret = defaultPassword
	}
	s.store = cookie.NewStore([]byte(sessionSecret))

	s.password = s.IoTConfig.Password
	if s.password == "" {
		s.password = defaultPassword
	}

	// TODO: set Secure: true when TLS is properly terminated end-to-end.
	s.store.Options(sessions.Options{
		Path: "/", MaxAge: 6000, HttpOnly: true, Secure: false,
	})

	gin.SetMode(gin.ReleaseMode)
	router := gin.Default()
	router.Use(sessions.Sessions(sessionCookieName, s.store))
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

	// Admin routes need a session only when Security.ProtectAdminRoutes is set.
	admin := []gin.HandlerFunc{}
	if s.IoTConfig.Security.ProtectAdminRoutes {
		admin = append(admin, s.APIAuthenticationHandler)
	}

	router.GET("/", s.LoginPageHandler)
	router.POST(URILogin, s.performLogin)
	router.POST(URILogout, s.LogoutHandler)
	router.GET(URIVersion, s.VersionHandler)
	router.GET(URIDashboard, s.AuthenticationHandler, s.DashboardHandler)
	router.POST(URIUploadData, s.UploadDataHandler)
	router.POST(URISaveTimeseries, s.SaveTimeseriesHandler)
	router.POST(URIInitDevice, s.InitDeviceHandler)
	router.POST(URIUpdateSensor, s.UpdateSensorHandler)
	router.POST(URISensorConfigure, append(admin, s.ConfSensorHandler)...)
	router.POST(URIDeviceConfigure, append(admin, s.ConfigureDeviceHandler)...)
	router.POST(URILogging, s.LogHandler)
	router.POST(URIMonitoringHealth, s.MonitoringHealthHandler)
	return router, nil
}

func (s *IoTEdge) StartSensorServer(stopChan chan bool) error {
	logFields := log.Fields{"fnct": "startHTTPListener"}
	router, err := s.newRouter()
	if err != nil {
		return err
	}

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
