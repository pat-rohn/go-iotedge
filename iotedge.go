package iotedge

import (
	"context"
	"fmt"
	"net/http"
	"os"

	"github.com/pat-rohn/timeseries"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type IoTConfig struct {
	Port                int
	MQTTPort            int
	MQTTRedirectAddress string
	DbConfig            timeseries.DBConfig
	TimeseriesDBConfig  timeseries.DBConfig
}

func New(iotConfig IoTConfig) IoTEdge {
	logFields := log.Fields{"fnct": "New"}
	log.WithFields(logFields).Tracef("Config %+v", iotConfig)
	s := IoTEdge{
		Port:               iotConfig.Port,
		DeviceDBConfig:     iotConfig.DbConfig,
		TimeseriesDBConfig: iotConfig.TimeseriesDBConfig,
	}
	s.DeviceDB = GetDeviceDB(iotConfig.DbConfig)

	tsHandler := timeseries.DBHandler(iotConfig.TimeseriesDBConfig)

	if err := tsHandler.CreateTimeseriesTable(); err != nil {
		log.Fatalf("failed to create table: %v", err)
	}
	s.Timeseries = tsHandler
	return s
}

func GetConfig() IoTConfig {
	logFields := log.Fields{"fnct": "GetConfig"}
	viper.SetDefault("DeviceDBConfig.Name", "config.db")
	viper.SetDefault("DeviceDBConfig.IPOrPath", "./")
	viper.SetDefault("DeviceDBConfig.UsePostgres", false)
	viper.SetDefault("DeviceDBConfig.User", "user")
	viper.SetDefault("DeviceDBConfig.Password", "password")
	viper.SetDefault("DeviceDBConfig.Port", 5432)
	viper.SetDefault("DeviceDBConfig.TableName", "configs")

	viper.SetDefault("TimeseriesDBConfig.Name", "timeseries.db")
	viper.SetDefault("TimeseriesDBConfig.IPOrPath", "./")
	viper.SetDefault("TimeseriesDBConfig.UsePostgres", false)
	viper.SetDefault("TimeseriesDBConfig.User", "user")
	viper.SetDefault("TimeseriesDBConfig.Password", "password")
	viper.SetDefault("TimeseriesDBConfig.Port", 5432)
	viper.SetDefault("TimeseriesDBConfig.TableName", "timeseries")

	viper.SetDefault("Port", 3004)
	viper.SetDefault("MQTTPort", 1883)
	viper.SetDefault("MQTTRedirectAddress", "")

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

	mux := http.NewServeMux()
	mux.HandleFunc(URIUploadData, s.UploadDataHandler)
	mux.HandleFunc(URISaveTimeseries, s.SaveTimeseries)
	mux.HandleFunc(URIInitDevice, s.InitDevice)
	mux.HandleFunc(URIUpdateSensor, s.UpdateSensorHandler)
	mux.HandleFunc(URISensorConfigure, s.ConfSensor)
	mux.HandleFunc(URIDeviceConfigure, s.ConfigureDevice)

	server := &http.Server{
		Addr:    fmt.Sprintf(":%v", s.Port),
		Handler: mux,
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
