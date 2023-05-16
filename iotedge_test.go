package iotedge

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
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

func TestMain(t *testing.T) {
	logFile := "TestMain.log"
	f, err := os.OpenFile(logFile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		fmt.Println("Failed to create logfile" + logFile)
		panic(err)
	}
	defer f.Close()
	log.SetOutput(f)
	log.SetLevel(log.TraceLevel)
	config := GetConfig()
	iot := New(config)
	iot.Port = 3006
	db := timeseries.New(config.TimeseriesDBConfig)
	defer db.CloseDatabase()
	if err := db.OpenDatabase(); err != nil {
		t.Fatalf("failed to create DB: %v", err)
	}
	if err := db.CreateTimeseriesTable(); err != nil {
		log.Fatalf("failed to create table: %v", err)
	}

	go func() {
		if err := iot.StartSensorServer(); err != nil {
			t.Fatalf("Failed to start server %s", err)
		}
	}()

	time.Sleep(time.Second * 2)
	name := "Dummy" + uuid.NewString()
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
	dummy.configureDevice(t, configureDeviceReq)

	configureSensorReq := ConfigureSensorReq{
		Name:       dummy.DeviceDesc.Name,
		SensorName: dummy.DeviceDesc.Sensors[0],
		Offset:     -4.0,
	}
	dummy.configureSensor(t, configureSensorReq)
}

func TestDBInit(t *testing.T) {
	logFile := "TestDBInit.log"
	f, err := os.OpenFile(logFile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		fmt.Println("Failed to create logfile" + logFile)
		panic(err)
	}
	defer f.Close()
	log.SetOutput(f)
	log.SetLevel(log.InfoLevel)
	iot := New(GetConfig())
	iot.Port = 3006
	go iot.StartSensorServer()
	time.Sleep(time.Second * 2)
	for i := 0; i < 500; i++ { // 500 seems to be the limit here (to investigate)
		time.Sleep(100 * time.Nanosecond)
		name := fmt.Sprintf("DummyOnlyDev%d-%s", i, uuid.New())
		dummy := DeviceDesc{
			Name:        name,
			Description: fmt.Sprintf("%s1.0;DummyTemp", name),
			Sensors:     []string{fmt.Sprintf("%sTemperature", name)},
		}
		iot.Init(dummy)
	}

}

func TestInitDevices(t *testing.T) {
	logFile := "TestInitDevices.log"
	f, err := os.OpenFile(logFile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		fmt.Println("Failed to create logfile" + logFile)
		panic(err)
	}
	defer f.Close()
	log.SetOutput(f)
	log.SetLevel(log.ErrorLevel)
	iot := New(GetConfig())
	iot.Port = 3006

	go iot.StartSensorServer()
	time.Sleep(2 * time.Second)

	counter := make(chan int)
	var wg sync.WaitGroup
	for i := 0; i < 450; i++ { // 500 seems to be the limit here (to investigate)

		time.Sleep(100 * time.Nanosecond)
		wg.Add(1)
		go func(t *testing.T, i int, counter chan int) {
			name := fmt.Sprintf("DummyOnlyDev%d-%s", i, uuid.New())
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
			wg.Done()
			fmt.Printf("--> %s\n", dummy.DeviceDesc.Name)
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

	log.Infof("%+v", req)
	endTime := time.Now().Add(time.Second * 15)
	for time.Now().Before(endTime) {
		client := http.Client{
			Timeout: 2 * time.Second,
		}
		resp, err := client.Post(d.Url+URIInitDevice, "application/json",
			bytes.NewBuffer(json_data))
		if err == nil && resp.StatusCode == http.StatusOK {
			return
		}
		time.Sleep(time.Millisecond * 50)

	}
	t.Fatalf("Failed to create device %s", d.DeviceDesc.Name)

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
		d.sendData(t, data)
	}

	_, err = io.ReadAll(resp.Body)
	if err != nil {
		t.Error(err)
	}
	defer resp.Body.Close()
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

	var res map[string]interface{}

	json.NewDecoder(resp.Body).Decode(&res)

	fmt.Println(res["json"])
}

func TestMQTT(t *testing.T) {
	logFile := "TestMQTT.log"
	f, err := os.OpenFile(logFile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		fmt.Println("Failed to create logfile" + logFile)
		panic(err)
	}
	defer f.Close()
	log.SetOutput(f)
	log.SetLevel(log.WarnLevel)
	dbConfig := GetConfig().TimeseriesDBConfig

	go StartMQTTBroker(1884, dbConfig)
	time.Sleep(time.Second * 5)
	for i := 0; i < 5000; i++ {
		time.Sleep(time.Millisecond * 2)
		fmt.Printf("<-- %d ", i)
		go pubMQTTPaho(t, i)
	}
	<-time.After(time.Second * 40)
}

type TestHandler struct {
}

func (h *TestHandler) handleConnected(client mqtt.Client) {
	fmt.Println("TestHandler Connected")

}

func (h *TestHandler) handleConnectionLost(client mqtt.Client, err error) {
	fmt.Println("TestHandler handleConnectionLost")

}

func (h *TestHandler) handleMessage(client mqtt.Client, msg mqtt.Message) {
	//payload := msg.Payload()
	//fmt.Printf("TestHandler handleMessage %s", string(payload))
}

func pubMQTTPaho(t *testing.T, id int) {
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
		panic(token.Error())
	}
	for {
		topic := fmt.Sprintf("pahoClient/test/Temperature%d/data", id)
		token := sensorsClient.Publish(topic, 0, false, fmt.Sprintf("%f", rand.Float32()*100))
		token.Wait()
		time.Sleep(time.Millisecond * 500)
	}

}
