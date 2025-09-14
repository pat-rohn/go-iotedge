package iotedge

import (
	"context"
	"fmt"
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
	"github.com/pat-rohn/timeseries"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type IoTConfig struct {
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
	return s
}

func GetConfig() IoTConfig {
	logFields := log.Fields{"fnct": "GetConfig"}
	viper.SetDefault("DBConfig.Name", "iot.db")
	viper.SetDefault("DBConfig.IPOrPath", "./")
	viper.SetDefault("DBConfig.UsePostgres", false)
	viper.SetDefault("DBConfig.User", "user")
	viper.SetDefault("DBConfig.Password", "password")
	viper.SetDefault("DBConfig.Port", 5432)
	viper.SetDefault("DBConfig.TableName", "configs")
	viper.SetDefault("TimeseriesTable", "measurements")

	viper.SetDefault("Port", 3004)
	viper.SetDefault("MQTTPort", 1883)
	viper.SetDefault("MQTTRedirectAddress", "")
	viper.SetDefault("UploadInterval", 30)

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
	router := gin.Default()

	router.POST(URIUploadData, s.UploadDataHandler)
	router.POST(URISaveTimeseries, s.SaveTimeseries)
	router.POST(URIInitDevice, s.InitDevice)
	router.POST(URIUpdateSensor, s.UpdateSensorHandler)
	router.POST(URISensorConfigure, s.ConfSensor)
	router.POST(URIDeviceConfigure, s.ConfigureDevice)
	router.POST(URILogging, s.Log)

	server := &http.Server{
		Addr:    fmt.Sprintf(":%v", s.Port),
		Handler: router,
	}

	// Channel to handle server errors
	errChan := make(chan error, 1)

	// Start server in goroutine
	go func() {
		fmt.Printf("Listen on port: %v\n", s.Port)
		log.WithFields(logFields).Infof("HTTPListenerPort is %v. ", s.Port)
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
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
