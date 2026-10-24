package iotedge

import (
	"context"
	"embed"
	"fmt"
	"html/template"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gin-contrib/sessions"
	"github.com/gin-contrib/sessions/cookie"
	"github.com/gin-gonic/gin"
	"github.com/pat-rohn/go-iotedge/internal/store"
	"github.com/pat-rohn/timeseries"
	"github.com/spf13/viper"
	"log/slog"
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
	Verbosity string
	Port      int
	MQTTPort  int
	// Deprecated: use Forwarding.URL.
	MQTTRedirectAddress string
	Forwarding          ForwardingConfig
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
	// IngestAuth checks the shared IngestToken (Authorization: Bearer) on the
	// endpoints the firmware uses: "off" (default), "warn" or "enforce".
	IngestAuth  string
	IngestToken string
}

func New(iotConfig IoTConfig) IoTEdge {
	logFields := []any{"fnct", "New"}
	slog.Log(context.Background(), LevelTrace, fmt.Sprintf("Config %+v", iotConfig), logFields...)
	s := IoTEdge{Port: iotConfig.Port, IoTConfig: iotConfig, status: newServerStatus()}
	deviceDB, err := OpenDeviceDB(iotConfig.DbConfig)
	if err != nil {
		fatal(fmt.Sprintf("failed to open database: %v", err))
	}
	s.DeviceDB = deviceDB
	s.LoggingDB = deviceDB.Logs()
	if err := s.DeviceDB.CreateTimeseriesTable(iotConfig.TimeseriesTable); err != nil {
		fatal(fmt.Sprintf("failed to create table: %v", err))
	}
	s.IoTConfig.Monitoring = s.IoTConfig.Monitoring.Normalized()
	s.DeviceDB.SetHealthHistoryRetention(time.Duration(s.IoTConfig.Monitoring.HealthHistoryDays) * 24 * time.Hour)
	if err := s.IoTConfig.Monitoring.Validate(); err != nil {
		fatal(fmt.Sprintf("invalid monitoring configuration: %v", err))
	}
	if err := s.initIngestAuth(); err != nil {
		fatal(fmt.Sprintf("invalid security configuration: %v", err))
	}
	if s.IoTConfig.MQTTRedirectAddress != "" && s.IoTConfig.Forwarding.URL == "" {
		slog.Warn("MQTTRedirectAddress is deprecated; set Forwarding.URL (and Forwarding.Token) instead")
	}
	if s.IoTConfig.LogPersistLevel == "" {
		s.IoTConfig.LogPersistLevel = "warning"
	}
	level, err := ParseLogLevel(s.IoTConfig.LogPersistLevel)
	if err != nil {
		fatal(fmt.Sprintf("invalid LogPersistLevel %q: %v", s.IoTConfig.LogPersistLevel, err))
	}
	setLogPersistLevel(level)
	persistServerLogs(s.LoggingDB)
	return s
}

// Close closes the database.
func (s *IoTEdge) Close() error {
	return s.DeviceDB.Close()
}

func GetConfig() IoTConfig {
	logFields := []any{"fnct", "GetConfig"}
	viper.SetDefault("Verbosity", "info")
	viper.SetDefault("Port", 3004)
	viper.SetDefault("MQTTPort", 1883)
	viper.SetDefault("MQTTRedirectAddress", "")
	viper.SetDefault("Forwarding.URL", "")
	viper.SetDefault("Forwarding.Token", "")
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
	viper.SetDefault("Monitoring.OnlineWindowMinutes", store.DefaultOnlineWindowMinutes)
	viper.SetDefault("Monitoring.RestartWindowDays", store.DefaultRestartWindowDays)
	viper.SetDefault("Monitoring.HealthHistoryDays", store.DefaultHealthHistoryDays)
	viper.SetDefault("Monitoring.HomeAssistantDiscovery", false)
	viper.SetDefault("Security.RequireCredentials", false)
	viper.SetDefault("Security.ProtectAdminRoutes", false)
	viper.SetDefault("Security.IngestAuth", IngestAuthOff)
	viper.SetDefault("Security.IngestToken", "")
	viper.SetDefault("LogPersistLevel", "warning")
	viper.SetConfigName("iot")
	viper.SetConfigType("json")
	dirname, err := os.UserHomeDir()
	if err != nil {
		slog.Error(fmt.Sprint(err))
		dirname = "."
	}
	pathToConfig := dirname + "/.iotserver"
	viper.AddConfigPath(pathToConfig)
	viper.AddConfigPath(".")
	slog.Info("Read Config", logFields...)
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			slog.Warn(fmt.Sprintf("no config file found: %v", err))
			if err := os.MkdirAll(pathToConfig, 0755); err != nil {
				fatal(fmt.Sprint(fmt.Sprintf("Creating config folder failed: %v", err)))
			}
			err = viper.SafeWriteConfig()
			if err != nil {
				fatal(fmt.Sprint(fmt.Sprintf("Storing default config failed: %v", err)))
			}
		} else {
			fatal(fmt.Sprint(fmt.Sprintf("Loading config failed: %v", err)))
		}
	} else {
		slog.Info(fmt.Sprintf("Using config file: %s", viper.ConfigFileUsed()), logFields...)
	}
	var iotConfig IoTConfig
	err = viper.Unmarshal(&iotConfig)
	if err != nil {
		fatal(fmt.Sprintf("fatal error config file: %v ", err))
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
		slog.Warn(fmt.Sprintf("%s — set a unique value in config", p))
	}
	return nil
}

// newRouter checks the credentials and builds the HTTP router with all routes.
func (s *IoTEdge) newRouter() (*gin.Engine, error) {
	if err := checkCredentials(s.IoTConfig); err != nil {
		return nil, err
	}
	if err := s.initIngestAuth(); err != nil {
		return nil, err
	}
	s.loginLimiter = newLoginLimiter()
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
	// Devices talk to the server directly: take the client IP from the
	// connection, never from X-Forwarded-For.
	if err := router.SetTrustedProxies(nil); err != nil {
		return nil, err
	}
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
	router.GET("/static/js/*filepath", func(c *gin.Context) {
		c.Request.URL.Path = "/static/js" + c.Param("filepath")
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
	router.GET(URIDashboard+"/devices", s.AuthenticationHandler, s.devicesPage)
	router.GET(URIDashboard+"/logs", s.AuthenticationHandler, s.logsPage)
	router.GET(URIDashboard+"/events", s.AuthenticationHandler, s.overviewEvents)
	router.GET(URIDashboard+"/measurements", s.AuthenticationHandler, s.measurementsPage)
	router.GET(URIDashboard+"/system", s.AuthenticationHandler, s.systemPage)
	router.GET(URIDashboard+"/measurements.csv", s.AuthenticationHandler, s.measurementsCSV)
	router.GET(URIDashboard+"/devices/:name", s.AuthenticationHandler, s.devicePage)
	router.PUT(URIDashboard+"/devices/:name", s.AuthenticationHandler, s.csrfProtect, s.updateDeviceSettings)
	router.PUT(URIDashboard+"/devices/:name/sensors/:sensor", s.AuthenticationHandler, s.csrfProtect, s.updateSensorOffset)
	router.POST(URIUploadData, s.ingestAuth.handler, s.UploadDataHandler)
	router.POST(URISaveTimeseries, s.ingestAuth.handler, s.SaveTimeseriesHandler)
	router.POST(URIInitDevice, s.ingestAuth.handler, s.InitDeviceHandler)
	router.POST(URIUpdateSensor, s.ingestAuth.handler, s.UpdateSensorHandler)
	router.POST(URISensorConfigure, append(admin, s.ConfSensorHandler)...)
	router.POST(URIDeviceConfigure, append(admin, s.ConfigureDeviceHandler)...)
	router.POST(URILogging, s.ingestAuth.handler, s.LogHandler)
	router.POST(URIMonitoringHealth, s.ingestAuth.handler, s.MonitoringHealthHandler)
	s.registerAPIv1(router)
	return router, nil
}

// StartSensorServer serves HTTP on s.Port until ctx is cancelled, then shuts
// down gracefully (up to 15 s for running requests).
func (s *IoTEdge) StartSensorServer(ctx context.Context) error {
	logFields := []any{"fnct", "startHTTPListener"}
	router, err := s.newRouter()
	if err != nil {
		return err
	}

	server := &http.Server{
		Addr:    fmt.Sprintf(":%v", s.Port),
		Handler: router,
		// Requests end with ctx, so open event streams do not hold up Shutdown.
		BaseContext: func(net.Listener) context.Context { return ctx },
	}

	errChan := make(chan error, 1)
	go func() {
		fmt.Printf("Listen on port: %v\n", s.Port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errChan <- err
		}
	}()

	select {
	case <-ctx.Done():
		slog.Info("Shutting down server gracefully...", logFields...)
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	case err := <-errChan:
		slog.Error(fmt.Sprintf("Listen and serve failed: %v.", err), logFields...)
		return err
	}
}
