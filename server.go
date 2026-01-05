package iotedge

import (
	"context"
	"embed"
	"fmt"
	"html/template"
	"net/http"
	"os"

	"github.com/gin-contrib/sessions"
	"github.com/gin-contrib/sessions/cookie"
	"github.com/gin-gonic/gin"
	"github.com/pat-rohn/timeseries"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

const (
	defaultPassword = "123"
	sessionToken    = "iot-session-token"
)

var (
	//go:embed static/*
	staticFS embed.FS

	//go:embed templates/*
	templatesFS embed.FS

	templates *template.Template
)

type IoTConfig struct {
	Verbosity           string
	Port                int
	MQTTPort            int
	MQTTRedirectAddress string
	DbConfig            timeseries.DBConfig
	TimeseriesTable     string
	UploadInterval      int // in seconds
}

func New(iotConfig IoTConfig) IoTEdge {
	logFields := log.Fields{"fnct": "New"}
	log.WithFields(logFields).Tracef("Config %+v", iotConfig)
	s := IoTEdge{
		Port:      iotConfig.Port,
		IoTConfig: iotConfig,
	}
	s.DeviceDB = GetDeviceDB(iotConfig.DbConfig)

	if err := s.DeviceDB.CreateTimeseriesTable(iotConfig.TimeseriesTable); err != nil {
		log.Fatalf("failed to create table: %v", err)
	}
	loggerDB := GetLoggingDB(iotConfig.DbConfig)
	if loggerDB == nil {
		log.Fatalf("failed to create logging DB")
	}
	log.WithFields(logFields).Infoln("IoTEdge created")
	return s
}

func GetConfig() IoTConfig {
	logFields := log.Fields{"fnct": "GetConfig"}

	viper.SetDefault("Verbosity", "i")
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

	viper.SetConfigName("iot")
	viper.SetConfigType("json")
	dirname, err := os.UserHomeDir()
	if err != nil {
		log.Fatal(err)
	}
	pathToConfig := dirname + "/.iotserver"
	viper.AddConfigPath(pathToConfig)
	viper.AddConfigPath(".")
	log.WithFields(logFields).Infoln("Read Config")
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			log.Warnf("no config file found: %v", err)
			if err := os.Mkdir(pathToConfig, 0755); err != nil {
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

	// Use the embed FS to serve static files (JS, CSS, etc.)
	staticServer := http.FileServer(http.FS(staticFS))

	s.store = cookie.NewStore([]byte(defaultPassword))
	s.password = defaultPassword
	s.store.Options(sessions.Options{
		Path:     "/",
		MaxAge:   6000, // 10 minutes in seconds
		HttpOnly: true,
		Secure:   false, // Set to true if using HTTPS
	})

	if !fileExists(certFile) || !fileExists(keyFile) || !isCertValid(certFile) {
		log.Println("Certificate or key not found. Generating new self-signed certificate...")
		if err := generateAndSaveCert(certFile, keyFile); err != nil {
			log.Fatalf("Failed to generate certificate: %v", err)
		}
	} else {
		log.Println("Using existing certificate and key.")
	}
	gin.SetMode(gin.ReleaseMode)

	router := gin.Default()
	router.Use(sessions.Sessions("edge-server-session", s.store))

	router.GET("/static/css/*filepath", func(c *gin.Context) {
		path := c.Param("filepath")
		c.Request.URL.Path = "/static/css" + path
		staticServer.ServeHTTP(c.Writer, c.Request)
	})
	/*router.GET("/static/js/*filepath", func(c *gin.Context) {
		path := c.Param("filepath")
		c.Request.URL.Path = "/static/js" + path
		staticServer.ServeHTTP(c.Writer, c.Request)
	})*/
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

	/*tlsConfig := &tls.Config{
		MinVersion:               tls.VersionTLS12,
		CurvePreferences:         []tls.CurveID{tls.CurveP521, tls.CurveP384, tls.CurveP256},
		PreferServerCipherSuites: true,
	}*/
	server := &http.Server{
		Addr:    fmt.Sprintf(":%v", s.Port),
		Handler: router,
		//TLSConfig: tlsConfig,
	}

	errChan := make(chan error, 1)

	go func() {
		fmt.Printf("Listen on port: %v\n", s.Port)
		log.WithFields(logFields).Infof("Starting server on port %v", s.Port)
		/*if err := server.ListenAndServeTLS(certFile, keyFile); err != nil && err != http.ErrServerClosed {
			log.WithFields(logFields).Errorf("Failed to start server: %v", err)
			errChan <- err
		}*/
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.WithFields(logFields).Errorf("Failed to start server: %v", err)
			errChan <- err
		}
	}()

	// Wait for stop signal or error
	select {
	case <-stopChan:
		log.WithFields(logFields).Info("Shutting down server gracefully...")
		return server.Shutdown(context.Background())
	case err := <-errChan:
		log.WithFields(logFields).Fatalf("Listen and serve failed: %v.", err)
		return err
	}
}
