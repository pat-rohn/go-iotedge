package iotedge

import (
	"fmt"
	"net/http"

	timeseries "github.com/pat-rohn/timeseries"
	log "github.com/sirupsen/logrus"
)

func New(dbConfig timeseries.DBConfig, port int) IoTEdge {

	return IoTEdge{
		DatabaseConfig: dbConfig,
		Port:           port,
	}
}

func (s *IoTEdge) StartSensorServer() error {
	logFields := log.Fields{"fnct": "startHTTPListener"}

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
		log.Info("insert %v", ts.Tag)
		if err := db.InsertTimeseries(ts, true); err != nil {
			log.Errorf("failed to insert TS: %v", err)
		}
	}
}
