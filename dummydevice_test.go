package iotedge

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sync"
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

	iot := GetConfig()
	iot.Port = 3006
	iot.DatabaseConfig.TableName = "measurements_test"
	db := timeseries.New(iot.DatabaseConfig)
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

	dummy.init(t)

	dummy.sendSensorData(t)

	dummy.DeviceDesc.Sensors = append(dummy.DeviceDesc.Sensors,
		fmt.Sprintf("%sHumidity", name))
	dummy.init(t)
	configureDeviceReq := ConfigureDeviceReq{
		Name:     dummy.DeviceDesc.Name,
		Interval: 10,
		Buffer:   1,
	}
	dummy.ConfigureDevice(t, configureDeviceReq)

	configureSensorReq := ConfigureSensorReq{
		Name:       dummy.DeviceDesc.Name,
		SensorName: dummy.DeviceDesc.Sensors[0],
		Offset:     -4.0,
	}
	dummy.configureSensor(t, configureSensorReq)
}

func TestInitDevices(t *testing.T) {
	log.SetLevel(log.WarnLevel)
	iot := GetConfig()
	iot.Port = 3006
	iot.DatabaseConfig.TableName = "measurements_test"
	db := timeseries.New(iot.DatabaseConfig)
	if err := db.CreateDatabase(); err != nil {
		t.Fatalf("failed to create DB: %v", err)
	}
	if err := db.CreateTimeseriesTable(); err != nil {
		log.Fatalf("failed to create table: %v", err)
	}
	db.CloseDatabase()

	go iot.StartSensorServer()
	time.Sleep(2 * time.Second)
	//iot.GormDB.CreateBatchSize = 100
	var wg sync.WaitGroup
	for i := 0; i < 10000; i++ {
		name := fmt.Sprintf("DummyOnlyDev%d", i)
		dummy := DummyDevice{
			Url: "http://localhost:3006",
			DeviceDesc: DeviceDesc{
				Name:        name,
				Description: fmt.Sprintf("%s1.0;DummyTemp", name),
				Sensors:     []string{fmt.Sprintf("%sTemperature", name)},
			},
		}

		time.Sleep(100 * time.Nanosecond)
		wg.Add(1)
		go func(t *testing.T) {
			dummy.init(t)
			dummy.sendSensorData(t)
			wg.Done()
			fmt.Printf("--> %s\n", dummy.DeviceDesc.Name)
		}(t)

	}
	fmt.Println("Wait till ready")
	wg.Wait()
	//t.Error("Test")
}

func (d *DummyDevice) init(t *testing.T) {
	type Request struct {
		Device DeviceDesc
	}
	req := Request{d.DeviceDesc}
	json_data, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}

	//log.Infof("%+v", req)

	_, err = http.Post(d.Url+URIInitDevice, "application/json",
		bytes.NewBuffer(json_data))

	if err != nil {
		t.Fatal(err)
	}

}

func (d *DummyDevice) sendSensorData(t *testing.T) {
	var data []timeseries.TimeseriesImportStruct

	val := timeseries.TimeseriesImportStruct{
		Tag: d.DeviceDesc.Sensors[0],
	}
	for i := 0; i < 10; i++ {
		time.Sleep(time.Millisecond * 2)
		val.Timestamps = append(val.Timestamps, time.Now().Format("2006-01-02 15:04:05.000"))
		val.Values = append(val.Values, fmt.Sprintf("%f", 283.0+(rand.Float32()*15)))
		val.Comments = append(val.Values, "dummy")
	}
	data = append(data, val)

	d.sendData(t, &data)
	fmt.Printf("Send data: %v\n", d.DeviceDesc.Sensors[0])

}

func (d *DummyDevice) sendData(t *testing.T, data *[]timeseries.TimeseriesImportStruct) {
	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	//fmt.Printf(string(jsonData))
	client := http.Client{
		Timeout: 40 * time.Second,
	}
	resp, err := client.Post(d.Url+URISaveTimeseries, "application/json",
		bytes.NewBuffer(jsonData))

	if err != nil {
		t.Error(err)
		return
	}

	if resp.StatusCode != http.StatusOK {
		if err != nil {
			t.Errorf("Failed with status: %s", resp.Status)
		}
	}

	_, err = io.ReadAll(resp.Body)
	// b, err := ioutil.ReadAll(resp.Body)  Go.1.15 and earlier
	if err != nil {
		t.Error(err)
	}
	//fmt.Println(string(b))
}

func (d *DummyDevice) ConfigureDevice(t *testing.T, configureDeviceReq ConfigureDeviceReq) {
	jsonData, err := json.Marshal(configureDeviceReq)
	if err != nil {
		t.Fatal(err)
	}
	client := http.Client{
		Timeout: 40 * time.Second,
	}
	resp, err := client.Post(d.Url+URIDeviceConfigure, "application/json",
		bytes.NewBuffer(jsonData))

	if err != nil {
		t.Fatal(err)
	}

	var res map[string]interface{}

	json.NewDecoder(resp.Body).Decode(&res)

	fmt.Println(res["json"])
}

func (d *DummyDevice) configureSensor(t *testing.T, configureSensorReq ConfigureSensorReq) {
	jsonData, err := json.Marshal(configureSensorReq)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := http.Post(d.Url+URISensorConfigure, "application/json",
		bytes.NewBuffer(jsonData))

	if err != nil {
		t.Fatal(err)
	}

	var res map[string]interface{}

	json.NewDecoder(resp.Body).Decode(&res)

	fmt.Println(res["json"])
}
