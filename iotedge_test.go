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

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"github.com/pat-rohn/timeseries"
	log "github.com/sirupsen/logrus"

	"testing"
)

type DummyDevice struct {
	Url        string
	DeviceDesc DeviceDesc
}

func TestAutomated(t *testing.T) {
	log.SetLevel(log.FatalLevel)
	startTime := time.Now()
	defer func() {
		fmt.Printf("Finished after %s\n", time.Since(startTime).String())
	}()

	testMain(t)
	testDBInit(t)
	testInitDevices(t)

	db := timeseries.DBHandler(GetConfig().DbConfig)
	defer db.Close()
	dbTimeseries := timeseries.DBHandler(GetConfig().TimeseriesDBConfig)
	defer dbTimeseries.Close()
}

func testMain(t *testing.T) {
	config := GetConfig()
	iot := New(config)
	iot.Port = 3006
	db := timeseries.DBHandler(config.TimeseriesDBConfig)
	if err := db.CreateTimeseriesTable(); err != nil {
		log.Fatalf("failed to create table: %v", err)
	}
	stopper := make(chan bool)
	go func() {
		iot.StartSensorServer(stopper)
	}()

	time.Sleep(time.Second * 2)
	name := "Dummy" + uuid.NewString()
	dummy := DummyDevice{
		Url: fmt.Sprintf("http://localhost:%d", iot.Port),
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
	dummy.configureDevice(t, configureDeviceReq)

	configureSensorReq := ConfigureSensorReq{
		Name:       dummy.DeviceDesc.Name,
		SensorName: dummy.DeviceDesc.Sensors[0],
		Offset:     -4.0,
	}
	dummy.configureSensor(t, configureSensorReq)
	stopper <- true
	time.Sleep(time.Second * 2)
}

func testDBInit(t *testing.T) {
	iot := New(GetConfig())
	iot.Port = 3006
	stopper := make(chan bool)
	go func() {
		iot.StartSensorServer(stopper)
	}()
	time.Sleep(time.Second * 2)
	for i := range 500 {
		time.Sleep(100 * time.Nanosecond)
		name := fmt.Sprintf("DummyOnlyDev%d-%s", i, uuid.New())
		dummy := DeviceDesc{
			Name:        name,
			Description: fmt.Sprintf("%s1.0;DummyTemp", name),
			Sensors:     []string{fmt.Sprintf("%sTemperature", name)},
		}
		log.Warnf("Init device %s", name)
		iot.Init(dummy)
	}
	stopper <- true
	time.Sleep(time.Second * 2)
}

func testInitDevices(t *testing.T) {
	iot := New(GetConfig())
	iot.Port = 3006

	stopper := make(chan bool)
	go func() {
		iot.StartSensorServer(stopper)
	}()
	time.Sleep(2 * time.Second)
	devicesNr := 500
	fmt.Printf("Start creating devices %d\n", devicesNr)
	counter := make(chan int)
	var wg sync.WaitGroup
	for i := range devicesNr {

		time.Sleep(100 * time.Nanosecond)
		wg.Add(1)
		go func(t *testing.T, i int, counter chan int) {
			name := fmt.Sprintf("DummyOnlyDev%d-%s", i, uuid.New())
			dummy := DummyDevice{
				Url: fmt.Sprintf("http://localhost:%d", iot.Port),
				DeviceDesc: DeviceDesc{
					Name:        name,
					Description: fmt.Sprintf("%s1.0;DummyTemp", name),
					Sensors:     []string{fmt.Sprintf("%sTemperature", name)},
				},
			}
			log.Warnf("Init device %s", name)
			dummy.init(t)

			log.Warnf("Send Data %s", name)
			dummy.sendSensorData(t)
			fmt.Printf("--> %s\n", dummy.DeviceDesc.Name)

			wg.Done()
			counter <- 1
		}(t, i, counter)

	}
	fmt.Println("Wait till ready")
	go func(counter chan int) {
		c := 0
		for {
			<-counter
			c += 1
			fmt.Printf("..%d..", c)
		}

	}(counter)
	wg.Wait()
	fmt.Println("Finished")
	stopper <- true
	time.Sleep(time.Second * 2)
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

	log.Infof("%+v", req)
	endTime := time.Now().Add(time.Second * 60)
	for time.Now().Before(endTime) {
		client := http.Client{
			Timeout: 2 * time.Second,
		}
		resp, err := client.Post(d.Url+URIInitDevice, "application/json",
			bytes.NewBuffer(json_data))
		{
			if err != nil {
				log.Warnf("Failed to create device %s: %v", d.DeviceDesc.Name, err)
				time.Sleep(time.Second * 2)
				continue
			}
			{
				defer resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					fmt.Printf("Device %s created", d.DeviceDesc.Name)
					return
				}
			}
		}

		time.Sleep(time.Millisecond * 50)

	}
	t.Errorf("Failed to create device %s", d.DeviceDesc.Name)

}

func (d *DummyDevice) sendSensorData(t *testing.T) {
	var data []timeseries.TimeseriesImportStruct

	val := timeseries.TimeseriesImportStruct{
		Tag: d.DeviceDesc.Sensors[0],
	}
	for range 10 {
		time.Sleep(time.Millisecond * 2)
		val.Timestamps = append(val.Timestamps, time.Now().Format("2006-01-02 15:04:05.000"))
		val.Values = append(val.Values, fmt.Sprintf("%f", 283.0+(rand.Float32()*15)))
		val.Comments = append(val.Values, "dummy")
	}
	data = append(data, val)

	d.sendData(t, &data)
}

func (d *DummyDevice) sendData(t *testing.T, data *[]timeseries.TimeseriesImportStruct) {
	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	nextTryTime := time.Now().Add(time.Second * 5)
	//fmt.Printf(string(jsonData))
	client := http.Client{
		Timeout: 5 * time.Second,
	}
	resp, err := client.Post(d.Url+URISaveTimeseries, "application/json",
		bytes.NewBuffer(jsonData))

	if err != nil {
		t.Error(err)
		return
	}

	if resp.StatusCode != http.StatusOK {
		log.Errorf("Failed with status: %s", resp.Status)
		resp.Body.Close()
		time.Sleep(time.Until(nextTryTime))
		d.sendData(t, data)
	} else {
		_, err = io.ReadAll(resp.Body)
		if err != nil {
			t.Error(err)
		}
		resp.Body.Close()
	}

	//fmt.Println(string(b))
}

func (d *DummyDevice) configureDevice(t *testing.T, configureDeviceReq ConfigureDeviceReq) {
	jsonData, err := json.Marshal(configureDeviceReq)
	if err != nil {
		t.Fatal(err)
	}
	client := http.Client{
		Timeout: 40 * time.Second,
	}
	resp, err := client.Post(d.Url+URIDeviceConfigure, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

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
	defer resp.Body.Close()

	var res map[string]any

	json.NewDecoder(resp.Body).Decode(&res)

	fmt.Println(res["json"])
}

func TestMQTT(t *testing.T) {
	log.SetLevel(log.WarnLevel)
	config := GetConfig()
	config.UploadInterval = 5
	edge := New(config)
	go StartMQTTBroker(1884, config)
	time.Sleep(time.Second * 2)
	for i := range 1000 {
		time.Sleep(time.Millisecond * 2)
		go pubMQTTPaho(i)
	}
	<-time.After(time.Second * 45)
	edge.Timeseries.Close()
	time.Sleep(time.Second * 5) // would fail if data is not written
}

type TestHandler struct{}

func (h *TestHandler) handleConnected(client mqtt.Client) {
	log.Infoln("TestHandler Connected")

}

func (h *TestHandler) handleConnectionLost(client mqtt.Client, err error) {
	log.Error("TestHandler handleConnectionLost")
}

func (h *TestHandler) handleMessage(client mqtt.Client, msg mqtt.Message) {
	//payload := msg.Payload()
	//fmt.Printf("TestHandler handleMessage %s", string(payload))
}

func pubMQTTPaho(id int) {
	log.Infof("<-- %d ", id)
	h := TestHandler{}
	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://%s:%d", "localhost", 1884))
	opts.SetClientID("test-client " + fmt.Sprintf("%d", id))
	//opts.SetUsername("user")
	//opts.SetPassword("pw")

	opts.SetDefaultPublishHandler(h.handleMessage)
	opts.OnConnect = h.handleConnected
	opts.OnConnectionLost = h.handleConnectionLost
	sensorsClient := mqtt.NewClient(opts)
	time.Sleep(time.Second * 2)
	if token := sensorsClient.Connect(); token.Wait() && token.Error() != nil {
		log.Fatal(token.Error())
	}

	endTime := time.Now().Add(time.Second * 10)
	nextUploadTime := time.Now()
	for time.Now().Before(endTime) {
		topic := fmt.Sprintf("pahoClient/test/Temperature%d/data", id)
		token := sensorsClient.Publish(topic, 0, false, fmt.Sprintf("%f", rand.Float32()*100))
		token.Wait()
		time.Sleep(time.Until(nextUploadTime))

		nextUploadTime = time.Now().Add(time.Millisecond * 100)
	}
	log.Infof("Disconnecting %d\n", id)
	sensorsClient.Disconnect(250)

}
