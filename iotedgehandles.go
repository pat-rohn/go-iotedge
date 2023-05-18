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

	logFields := log.Fields{"fnct": "SaveTimeseries"}
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
		if !s.semTimeseries.TryAcquire(1) {
			http.Error(w, "busy saving timseries.", http.StatusInternalServerError)
			log.WithFields(logFields).Errorf("busy saving timseries.")
			return
		}
		defer s.semTimeseries.Release(1)

		//dbh := timeseries.New(s.DatabaseConfig)
		db := timeseries.New(s.TimeseriesDBConfig)

		for _, ts := range data {
			log.Infof("insert %v", ts.Tag)

			if err := db.InsertTimeseries(ts, true); err != nil {
				http.Error(w, fmt.Sprintf(`Failed to save timeseries: %+v.`, err.Error()), http.StatusInternalServerError)
				log.WithFields(logFields).Errorf("Failed to save timeseries: %+v ", err.Error())
				return

			}
		}
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")

		json.NewEncoder(w).Encode(`{"success": true}`)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		log.Errorf("Cant do that.")
	}
}

func (s *IoTEdge) UploadDataHandler(w http.ResponseWriter, r *http.Request) {
	logFields := log.Fields{"fnct": "UploadDataHandler"}
	log.WithFields(logFields).Infof("Got request: %v ", r.URL)
	SetHeaders(w, r.Header.Get("Origin"))
	switch r.Method {
	case http.MethodOptions:
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
		if !s.semTimeseries.TryAcquire(1) {

			http.Error(w, "busy uploading data.", http.StatusInternalServerError)
			log.WithFields(logFields).Errorf("busy uploading data.")
			return
		}
		defer s.semTimeseries.Release(1)

		dbh := timeseries.New(s.TimeseriesDBConfig)
		defer dbh.CloseDatabase()
		if err := dbh.OpenDatabase(); err != nil {
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
		logFields["Name"] = p.DeviceDesc.Name
		logFields["Description"] = p.DeviceDesc.Description
		log.WithFields(logFields).Infof("Value: %+v ", p)

		if !s.sem.TryAcquire(1) {
			http.Error(w, fmt.Sprintf(`too busy to init device %v.`, p.DeviceDesc.Name), http.StatusInternalServerError)
			log.WithFields(logFields).Errorf("too busy to init device %s ", p.DeviceDesc.Name)
			return
		}
		defer s.sem.Release(1)

		dev, err := s.Init(p.DeviceDesc)
		if err != nil {
			http.Error(w, fmt.Sprintf(`init device %s failed: %+v.`, p.DeviceDesc.Name, err.Error()), http.StatusInternalServerError)
			log.WithFields(logFields).Errorf("init device %s failed: %+v ", p.DeviceDesc.Name, err.Error())
			return
		}

		log.WithFields(logFields).Infof("device initialized: %+v ", dev)
		json.NewEncoder(w).Encode(dev)
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")

		return

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		log.WithFields(logFields).Errorln("Only Post is allowed.")
	}
}

func (s *IoTEdge) ConfigureDevice(w http.ResponseWriter, r *http.Request) {

	logFields := log.Fields{"fnct": "ConfigureDevice"}
	log.WithFields(logFields).Infof("Got request: %v ", r.URL)
	SetHeaders(w, r.Header.Get("Origin"))
	switch r.Method {
	case http.MethodOptions:

	case http.MethodPost:
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

		if !s.sem.TryAcquire(1) {
			http.Error(w, fmt.Sprintf(`too busy to configure device %v.`, p.Name), http.StatusInternalServerError)
			log.WithFields(logFields).Errorf("too busy to configure device %s ", p.Name)
			return
		}
		defer s.sem.Release(1)

		dev, err := s.GetDevice(p.Name)
		if err != nil {
			http.Error(w, fmt.Sprintf(`getting device failed: %+v.`, err.Error()), http.StatusInternalServerError)
			log.WithFields(logFields).Errorf("getting device failed: %+v ", err.Error())
			return
		}
		dev.Interval = p.Interval
		dev.Buffer = p.Buffer
		err = s.Configure(dev)
		if err != nil {
			http.Error(w, fmt.Sprintf(`configuring device failed: %+v.`, err.Error()), http.StatusInternalServerError)
			log.WithFields(logFields).Errorf("configuring device failed: %+v ", err.Error())
			return
		}
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")

		log.WithFields(logFields).Infof("device: %+v ", dev)
		json.NewEncoder(w).Encode(dev)
		return

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		log.WithFields(logFields).Errorln("Only Post is allowed.")
	}
}

func (s *IoTEdge) ConfSensor(w http.ResponseWriter, r *http.Request) {

	logFields := log.Fields{"fnct": "ConfSensor"}
	log.WithFields(logFields).Infof("Got request: %v ", r.URL)
	SetHeaders(w, r.Header.Get("Origin"))
	switch r.Method {
	case http.MethodOptions:
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

		if !s.sem.TryAcquire(1) {
			http.Error(w, fmt.Sprintf(`too busy to configure sensor %v.`, p.Name), http.StatusInternalServerError)
			log.WithFields(logFields).Errorf("too busy to configure sensor %s ", p.Name)
			return
		}
		defer s.sem.Release(1)

		dev, err := s.GetDevice(p.Name)
		if err != nil {
			http.Error(w, fmt.Sprintf(`getting device failed: %+v.`, err.Error()), http.StatusInternalServerError)
			log.WithFields(logFields).Errorf("getting device failed: %+v ", err.Error())
			return
		}
		sensors, err := s.GetSensors(dev.ID)
		if err != nil {
			http.Error(w, fmt.Sprintf(`configuring device failed: %+v.`, err.Error()), http.StatusInternalServerError)
			log.WithFields(logFields).Errorf("configuring device failed: %+v ", err.Error())
			return
		}

		for _, ses := range sensors {
			if ses.Name == p.SensorName {
				updateSensor := ses
				updateSensor.Offset = p.Offset
				updateSensor.DeviceID = dev.ID

				if err = s.ConfigureSensor(updateSensor); err != nil {
					log.WithFields(logFields).Errorf("configuring sensor failed: %+v ", err.Error())
				}

			}
		}
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")

		log.WithFields(logFields).Infof("sensor: %+v ", dev)
		json.NewEncoder(w).Encode(dev)
		return

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		log.WithFields(logFields).Errorln("Only Post is allowed.")
	}
}

func (s *IoTEdge) UpdateSensorHandler(w http.ResponseWriter, r *http.Request) {

	logFields := log.Fields{"fnct": "UpdateSensorHandler"}
	log.WithFields(logFields).Infof("Got request: %v ", r.URL)
	SetHeaders(w, r.Header.Get("Origin"))
	switch r.Method {
	case http.MethodOptions:

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
		if !s.sem.TryAcquire(1) {
			http.Error(w, fmt.Sprintf(`too busy to update sensor %s.`, p.Tags), http.StatusInternalServerError)
			log.WithFields(logFields).Errorf("too busy to update sensor  %s ", p.Tags)
			return
		}
		defer s.sem.Release(1)

		dbh := timeseries.New(s.TimeseriesDBConfig)
		defer dbh.CloseDatabase()
		if err := dbh.OpenDatabase(); err != nil {
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

func SetHeaders(w http.ResponseWriter, origin string) {
	log.Tracef("origin from header: %+s", origin)
	w.Header().Set("Access-Control-Allow-Origin", origin)
	w.Header().Set("Access-Control-Allow-Credentials", "true")
	w.Header().Set("Access-Control-Allow-Methods", "PUT, POST, PATCH, OPTIONS, GET, DELETE")
	w.Header().Set("Access-Control-Allow-Headers", "content-type")
	w.Header().Set("Access-Control-Max-Age", "240")
}
