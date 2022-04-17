package iotedge

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"time"

	"github.com/pat-rohn/timeseries"
	log "github.com/sirupsen/logrus"

	"testing"
)

type DummyDevice struct {
	Url        string
	DeviceDesc DeviceDesc
}

func TestMain(t *testing.T) {

	conf := GetConfig()
	conf.DatabaseConfig.TableName = "measurements_test"
	iot := New(conf.DatabaseConfig, 3006)
	db := timeseries.New(conf.DatabaseConfig)
	defer db.CloseDatabase()
	if err := db.CreateDatabase(); err != nil {
		t.Fatalf("failed to create DB: %v", err)
	}
	if err := db.CreateTimeseriesTable(); err != nil {
		log.Fatalf("failed to create table: %v", err)
	}

	go iot.StartSensorServer()
	time.Sleep(time.Second * 2)
	name := "Dummy"
	dummy := DummyDevice{
		Url: "http://localhost:3006",
		DeviceDesc: DeviceDesc{
			Name:        name,
			Description: fmt.Sprintf("%s1.0;DummyTemp", name),
			Sensors:     []string{fmt.Sprintf("%sTemperature", name)},
		},
	}

	dummy.simulate(t)
	dummy.DeviceDesc.Sensors = append(dummy.DeviceDesc.Sensors,
		fmt.Sprintf("%sHumidity", name))
	dummy.simulate(t)
}

func (d *DummyDevice) simulate(t *testing.T) {
	type Request struct {
		Device DeviceDesc
	}
	req := Request{d.DeviceDesc}
	json_data, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}

	log.Infof("%+v", req)

	resp, err := http.Post(d.Url+URIInitDevice, "application/json",
		bytes.NewBuffer(json_data))

	if err != nil {
		t.Fatal(err)
	}

	var res map[string]interface{}

	json.NewDecoder(resp.Body).Decode(&res)

	fmt.Println(res["json"])
	for j := 0; j < 10; j++ {

		var data []timeseries.TimeseriesImportStruct

		val := timeseries.TimeseriesImportStruct{
			Tag: fmt.Sprintf("%sTemperature", d.DeviceDesc.Name),
		}
		for i := 0; i < 10; i++ {
			time.Sleep(time.Millisecond * 50)
			val.Timestamps = append(val.Timestamps, time.Now().Format("2006-01-02 15:04:05.000"))
			val.Values = append(val.Values, fmt.Sprintf("%f", 283.0+(rand.Float32()*15)))
			val.Comments = append(val.Values, "dummy")
		}
		data = append(data, val)
		go d.sendData(t, &data)
	}
	time.Sleep(time.Second * 2)
}

func (d *DummyDevice) sendData(t *testing.T, data *[]timeseries.TimeseriesImportStruct) {
	jsonData, err := json.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := http.Post(d.Url+URISaveTimeseries, "application/json",
		bytes.NewBuffer(jsonData))

	if err != nil {
		t.Fatal(err)
	}

	var res map[string]interface{}

	json.NewDecoder(resp.Body).Decode(&res)

	fmt.Println(res["json"])
}
