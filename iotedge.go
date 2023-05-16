package iotedge

import (
	"fmt"
	"net/http"
	"os"

	_ "github.com/mattn/go-sqlite3"
	"github.com/pat-rohn/timeseries"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"golang.org/x/sync/semaphore"
)

type IoTConfig struct {
	Port               int
	MQTTPort           int
	DbConfig           timeseries.DBConfig
	TimeseriesDBConfig timeseries.DBConfig
}

func New(iotConfig IoTConfig) IoTEdge {
	logFields := log.Fields{"fnct": "New"}
	log.WithFields(logFields).Tracef("Config %+v", iotConfig)
	return IoTEdge{
		Port:               iotConfig.Port,
		DeviceDBConfig:     iotConfig.DbConfig,
		TimeseriesDBConfig: iotConfig.TimeseriesDBConfig,
		sem:                semaphore.NewWeighted(1),
		semTimeseries:      semaphore.NewWeighted(1),
	}
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
			log.Warnf(fmt.Sprintf("no config file found: %v", err))
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

func (s *IoTEdge) StartSensorServer() error {
	logFields := log.Fields{"fnct": "startHTTPListener"}
	if err := s.InitializeDB(); err != nil {
		return err
	}
	tsHandler := timeseries.New(s.TimeseriesDBConfig)
	if err := tsHandler.CreateTimeseriesTable(); err != nil {
		log.Errorf("failed to create table: %v", err)
		return err
	}
	s.Timeseries = tsHandler
	http.HandleFunc(URIUploadData, s.UploadDataHandler)
	http.HandleFunc(URISaveTimeseries, s.SaveTimeseries)

	http.HandleFunc(URIInitDevice, s.InitDevice)
	http.HandleFunc(URIUpdateSensor, s.UpdateSensorHandler)
	http.HandleFunc(URISensorConfigure, s.ConfSensor)
	http.HandleFunc(URIDeviceConfigure, s.ConfigureDevice)

	port := s.Port

	fmt.Printf("Listen on port: %v\n", port)
	log.WithFields(logFields).Infof("HTTPListenerPort is %v. ", port)

	err := http.ListenAndServe(":"+fmt.Sprintf("%v", port), nil)
	if err != nil {
		log.WithFields(logFields).Fatalf("Listen and serve failed: %v.", err)
		return err
	}
	return nil
}
