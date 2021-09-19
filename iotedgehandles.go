package iotedge

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	timeseries "github.com/pat-rohn/timeseries"

	log "github.com/sirupsen/logrus"
)

func (s *IoTEdge) SaveTimeseries(w http.ResponseWriter, req *http.Request) {
	var data []timeseries.TimeseriesImportStruct
	switch req.Method {
	case "POST":
		d := json.NewDecoder(req.Body)
		err := d.Decode(&data)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		log.Info("Received data.%+v", data)
		log.Trace("%+v", data)

		//dbh := timeseries.New(s.DatabaseConfig)
		go s.WriteToDatabase(data)

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		log.Errorf("Cant do that.")
	}
}

func (s *IoTEdge) UpdateSensorHandler(w http.ResponseWriter, r *http.Request) {

	logFields := log.Fields{"fnct": "UpdateSensorHandler"}
	log.WithFields(logFields).Infof("Got request: %v ", r.URL)

	switch r.Method {
	case "GET":
		output := Output{Status: "OK", Answer: "Okay"}
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(output)
	case "POST":
		log.WithFields(logFields).Infof("Got post: %+v ", r.URL)
		d := json.NewDecoder(r.Body)
		var p sensorValues
		err := d.Decode(&p)
		if err != nil {
			http.Error(w, fmt.Sprintf(`Input error: %+v.`, err.Error()), http.StatusInternalServerError)
			log.WithFields(logFields).Errorf("Input error: %+v ", err.Error())
			return
		}
		log.WithFields(logFields).Infof("Value: %+v ", p)

		dbh := timeseries.New(s.DatabaseConfig)
		defer dbh.CloseDatabase()
		if err := dbh.CreateDatabase(); err != nil {
			log.Errorf("Failed to open database: %v", err)
			http.Error(w, fmt.Sprintf("Failed to open database: %v", err), http.StatusInternalServerError)
			return
		}
		for _, val := range p.Data {
			tsVal := timeseries.TimeseriesImportStruct{
				Tag:        val.Name,
				Timestamps: []string{time.Now().UTC().Format("2006-01-02 15:04:05.000")},
				Values:     []string{fmt.Sprintf("%f", val.Value)},
				Comments:   p.Tags,
			}

			if err := dbh.InsertTimeseries(tsVal, true); err != nil {
				log.Errorf("Failed to insert values into database: %v", err)
				http.Error(w, fmt.Sprintf("Failed to insert values into database: %v", err), http.StatusInternalServerError)
				return
			}
		}

		output := Output{Status: "OK", Answer: "Success"}
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(output)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		log.WithFields(logFields).Errorln("Only Post is allowed.")
	}
}

func (s *IoTEdge) UploadDataHandler(w http.ResponseWriter, r *http.Request) {
	logFields := log.Fields{"fnct": "UploadDataHandler"}
	log.WithFields(logFields).Infof("Got request: %v ", r.URL)
	switch r.Method {
	case "GET":

		output := Output{Status: "OK", Answer: "Okay"}
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(output)
	case "POST":
		log.WithFields(logFields).Infof("Got post: %+v ", r.URL)
		d := json.NewDecoder(r.Body)
		var data []timeseries.TimeseriesImportStruct
		err := d.Decode(&data)
		if err != nil {
			http.Error(w, fmt.Sprintf(`Input error: %+v.`, err.Error()), http.StatusInternalServerError)
			log.WithFields(logFields).Errorf("Input error: %+v ", err.Error())
			return
		}
		log.WithFields(logFields).Infof("Value: %+v ", data)

		dbh := timeseries.New(s.DatabaseConfig)
		defer dbh.CloseDatabase()
		if err := dbh.CreateDatabase(); err != nil {
			log.Errorf("Failed to open database: %v", err)
			http.Error(w, fmt.Sprintf("Failed to open database: %v", err),
				http.StatusInternalServerError)
			return
		}
		for _, val := range data {
			if err := dbh.InsertTimeseries(val, true); err != nil {
				log.Errorf("Failed to insert values into database: %v", err)
				http.Error(w, fmt.Sprintf("Failed to insert values into database: %v", err),
					http.StatusInternalServerError)
				return
			}
		}

		output := Output{Status: "OK", Answer: "Success"}
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(output)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		log.WithFields(logFields).Errorln("Only Post is allowed.")
	}
}