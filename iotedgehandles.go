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
		log.Infof("Received data.%+v", data)
		log.Tracef("%+v", data)

		//dbh := timeseries.New(s.DatabaseConfig)
		go s.WriteToDatabase(data)

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		log.Errorf("Cant do that.")
	}
}

func (s *IoTEdge) InitDevice(w http.ResponseWriter, r *http.Request) {

	logFields := log.Fields{"fnct": "InitDevice"}
	log.WithFields(logFields).Infof("Got request: %v ", r.URL)

	switch r.Method {
	case "POST":
		log.WithFields(logFields).Infof("Got post: %+v ", r.URL)

		d := json.NewDecoder(r.Body)
		type DeviceReq struct {
			DeviceDesc DeviceDesc `json:"Device"`
		}
		var p DeviceReq
		err := d.Decode(&p)
		if err != nil {
			http.Error(w, fmt.Sprintf(`Input error: %+v.`, err.Error()), http.StatusInternalServerError)
			log.WithFields(logFields).Errorf("Input error: %+v ", err.Error())
			return
		}
		log.WithFields(logFields).Infof("Value: %+v ", p)

		dev, err := Init(p.DeviceDesc)
		if err != nil {
			http.Error(w, fmt.Sprintf(`init device failed: %+v.`, err.Error()), http.StatusInternalServerError)
			log.WithFields(logFields).Errorf("init device failed: %+v ", err.Error())
			return
		}
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")

		log.WithFields(logFields).Infof("device: %+v ", dev)
		json.NewEncoder(w).Encode(dev.Device)
		return

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		log.WithFields(logFields).Errorln("Only Post is allowed.")
	}
}

func (s *IoTEdge) ConfigureDevice(w http.ResponseWriter, r *http.Request) {

	logFields := log.Fields{"fnct": "ConfigureDevice"}
	log.WithFields(logFields).Infof("Got request: %v ", r.URL)

	switch r.Method {
	case "POST":
		log.WithFields(logFields).Infof("Got post: %+v ", r.URL)

		d := json.NewDecoder(r.Body)

		var p ConfigureDeviceReq
		err := d.Decode(&p)
		if err != nil {
			http.Error(w, fmt.Sprintf(`Input error: %+v.`, err.Error()), http.StatusInternalServerError)
			log.WithFields(logFields).Errorf("Input error: %+v ", err.Error())
			return
		}
		log.WithFields(logFields).Infof("Value: %+v ", p)

		dev, err := GetDevice(p.Name)
		if err != nil {
			http.Error(w, fmt.Sprintf(`getting device failed: %+v.`, err.Error()), http.StatusInternalServerError)
			log.WithFields(logFields).Errorf("getting device failed: %+v ", err.Error())
			return
		}

		dev.Configure(p.Interval, p.Buffer)
		if err != nil {
			http.Error(w, fmt.Sprintf(`configuring device failed: %+v.`, err.Error()), http.StatusInternalServerError)
			log.WithFields(logFields).Errorf("configuring device failed: %+v ", err.Error())
			return
		}
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")

		log.WithFields(logFields).Infof("device: %+v ", dev)
		json.NewEncoder(w).Encode(dev.Device)
		return

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		log.WithFields(logFields).Errorln("Only Post is allowed.")
	}
}

func (s *IoTEdge) ConfigureSensor(w http.ResponseWriter, r *http.Request) {

	logFields := log.Fields{"fnct": "ConfigureDevice"}
	log.WithFields(logFields).Infof("Got request: %v ", r.URL)

	switch r.Method {
	case "POST":
		log.WithFields(logFields).Infof("Got post: %+v ", r.URL)

		d := json.NewDecoder(r.Body)

		var p ConfigureSensorReq
		err := d.Decode(&p)
		if err != nil {
			http.Error(w, fmt.Sprintf(`input error: %+v.`, err.Error()), http.StatusInternalServerError)
			log.WithFields(logFields).Errorf("input error: %+v ", err.Error())
			return
		}
		log.WithFields(logFields).Infof("Value: %+v ", p)

		dev, err := GetDevice(p.Name)
		if err != nil {
			http.Error(w, fmt.Sprintf(`getting device failed: %+v.`, err.Error()), http.StatusInternalServerError)
			log.WithFields(logFields).Errorf("getting device failed: %+v ", err.Error())
			return
		}

		if err = dev.ConfigureSensor(p.Offset, p.SensorName); err != nil {
			http.Error(w, fmt.Sprintf(`configuring device failed: %+v.`, err.Error()), http.StatusInternalServerError)
			log.WithFields(logFields).Errorf("configuring device failed: %+v ", err.Error())
			return
		}
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")

		log.WithFields(logFields).Infof("device: %+v ", dev)
		json.NewEncoder(w).Encode(dev.Device)
		return

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		log.WithFields(logFields).Errorln("Only Post is allowed.")
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
