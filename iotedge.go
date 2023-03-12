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
	Port     int
	MQTTPort int
	DbConfig timeseries.DBConfig
}

func New(iotConfig IoTConfig) IoTEdge {
	logFields := log.Fields{"fnct": "New"}
	log.WithFields(logFields).Tracef("Config %+v", iotConfig)
	return IoTEdge{
		Port:           iotConfig.Port,
		DatabaseConfig: iotConfig.DbConfig,
		sem:            semaphore.NewWeighted(1),
	}
}

func GetConfig() IoTConfig {
	logFields := log.Fields{"fnct": "GetConfig"}
	viper.SetDefault("DbConfig.Name", "plottydb")
	viper.SetDefault("DbConfig.IPOrPath", "localhost")
	viper.SetDefault("DbConfig.UsePostgres", true)
	viper.SetDefault("DbConfig.User", "user")
	viper.SetDefault("DbConfig.Password", "password")
	viper.SetDefault("DbConfig.Port", 5432)
	viper.SetDefault("DbConfig.TableName", "measurements1")
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
	tsHandler := timeseries.New(s.DatabaseConfig)
	if err := tsHandler.CreateTimeseriesTable(); err != nil {
		log.Errorf("failed to create table: %v", err)
		return err
	}
	s.Timeseries = tsHandler
	http.HandleFunc(URIInitDevice, s.InitDevice)
	http.HandleFunc(URIUpdateSensor, s.UpdateSensorHandler)
	http.HandleFunc(URIUploadData, s.UploadDataHandler)
	http.HandleFunc(URISaveTimeseries, s.SaveTimeseries)
	http.HandleFunc(URISensorConfigure, s.ConfigureSensor)
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

func (s *IoTEdge) WriteToDatabase(data []timeseries.TimeseriesImportStruct) {
	db := timeseries.New(s.DatabaseConfig)

	for _, ts := range data {
		log.Infof("insert %v", ts.Tag)
		s.tsMutex.Lock()
		if err := db.InsertTimeseries(ts, true); err != nil {
			if err != nil {
				log.Fatalf("failed to insert TS: %v", err)
			}

		}
		s.tsMutex.Unlock()
	}
}
