package iotedge

import (
	"fmt"
	"net/http"
	"os"

	_ "github.com/mattn/go-sqlite3"
	timeseries "github.com/pat-rohn/timeseries"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

func New(dbConfig timeseries.DBConfig, port int) IoTEdge {

	return IoTEdge{
		DatabaseConfig: dbConfig,
		Port:           port,
	}
}

func (s *IoTEdge) StartSensorServer() error {
	logFields := log.Fields{"fnct": "startHTTPListener"}
	InitializeDB()
	http.HandleFunc(URIInitDevice, s.InitDevice)
	http.HandleFunc(URIUpdateSensor, s.UpdateSensorHandler)
	http.HandleFunc(URIUploadData, s.UploadDataHandler)
	http.HandleFunc(URISaveTimeseries, s.SaveTimeseries)
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
	defer db.CloseDatabase()
	if err := db.CreateDatabase(); err != nil {
		log.Error("failed to create DB: %v", err)
	}
	for _, ts := range data {
		log.Infof("insert %v", ts.Tag)
		if err := db.InsertTimeseries(ts, true); err != nil {
			log.Errorf("failed to insert TS: %v", err)
		}
	}
}

func GetConfig() IoTEdge {
	logFields := log.Fields{"fnct": "GetConfig"}
	viper.SetDefault("DbConfig.Name", "plottydb")
	viper.SetDefault("DbConfig.IPOrPath", "localhost")
	viper.SetDefault("DbConfig.UsePostgres", true)
	viper.SetDefault("DbConfig.User", "user")
	viper.SetDefault("DbConfig.Password", "password")
	viper.SetDefault("DbConfig.Port", 5432)
	viper.SetDefault("DbConfig.TableName", "measurements1")
	viper.SetDefault("Port", 3004)

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
	type config struct {
		Port     int
		DbConfig timeseries.DBConfig
	}

	var iotConfig config

	err = viper.Unmarshal(&iotConfig)
	if err != nil {
		panic(fmt.Errorf("fatal error config file: %w ", err))
	}
	log.Tracef("Config %+v", iotConfig)
	return IoTEdge{
		Port:           iotConfig.Port,
		DatabaseConfig: iotConfig.DbConfig,
	}
}
