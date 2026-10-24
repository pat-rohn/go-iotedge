package iotedge

import (
	"bytes"
	"context"
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
	"log/slog"

	"testing"
)

type DummyDevice struct {
	Url        string
	DeviceDesc DeviceDesc
}

func TestAutomated(t *testing.T) {
	startTime := time.Now()
	defer func() {
		fmt.Printf("Finished after %s\n", time.Since(startTime).String())
	}()

	testMain(t)
	testDBInit(t)
	testInitDevices(t)
}

func testMain(t *testing.T) {
	iot := regressionIoT()
	iot.Port = freePort(t)
	if err := iot.DeviceDB.CreateTimeseriesTable("measurements"); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	stop := startTestServer(t, &iot)
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
		Name:         dummy.DeviceDesc.Name,
		SensorName:   dummy.DeviceDesc.Sensors[0],
		SensorOffset: -4.0,
	}
	dummy.configureSensor(t, configureSensorReq)
	stop()
}

func testDBInit(t *testing.T) {
	iot := regressionIoT()
	iot.Port = freePort(t)
	stop := startTestServer(t, &iot)
	for i := range 500 {
		time.Sleep(100 * time.Nanosecond)
		name := fmt.Sprintf("DummyOnlyDev%d-%s", i, uuid.New())
		dummy := DeviceDesc{
			Name:        name,
			Description: fmt.Sprintf("%s1.0;DummyTemp", name),
			Sensors:     []string{fmt.Sprintf("%sTemperature", name)},
		}
		slog.Warn(fmt.Sprintf("Init device %s", name))
		if _, err := iot.Init(dummy); err != nil {
			t.Errorf("Init device %s: %v", name, err)
		}
	}
	stop()
}

func testInitDevices(t *testing.T) {
	iot := regressionIoT()
	iot.Port = freePort(t)

	stop := startTestServer(t, &iot)
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
			slog.Warn(fmt.Sprintf("Init device %s", name))
			dummy.init(t)

			slog.Warn(fmt.Sprintf("Send Data %s", name))
			dummy.sendSensorData(t)
			fmt.Printf("--> %s\n", dummy.DeviceDesc.Name)

			counter <- 1
			wg.Done()
		}(t, i, counter)

	}
	fmt.Println("Wait till ready")
	go func(counter chan int) {
		c := 0
		for range counter {
			c += 1
			fmt.Printf("..%d..", c)
		}
	}(counter)
	wg.Wait()
	close(counter)
	fmt.Println("Finished")
	stop()
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

	slog.Info(fmt.Sprintf("%+v", req))
	endTime := time.Now().Add(time.Second * 60)
	for time.Now().Before(endTime) {
		client := http.Client{
			Timeout: 2 * time.Second,
		}
		resp, err := client.Post(d.Url+URIInitDevice, "application/json",
			bytes.NewBuffer(json_data))
		if err != nil {
			slog.Warn(fmt.Sprintf("Failed to create device %s: %v", d.DeviceDesc.Name, err))
			time.Sleep(time.Second * 2)
			continue
		}
		// Fix #8: close body immediately in the loop body, not via defer
		ok := resp.StatusCode == http.StatusOK
		resp.Body.Close()
		if ok {
			fmt.Printf("Device %s created", d.DeviceDesc.Name)
			return
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
		val.Comments = append(val.Comments, "dummy") // Fix #7: was append(val.Values, "dummy")
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
		slog.Error(fmt.Sprintf("Failed with status: %s", resp.Status))
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

// TestMQTT is a load test: 1000 clients publish every 100 ms for
// loadTestDuration.
func TestMQTT(t *testing.T) {
	_, broker, port := testBroker(t, SecurityConfig{}, false)
	var wg sync.WaitGroup
	for i := range 1000 {
		time.Sleep(time.Millisecond * 2)
		wg.Add(1)
		go func() {
			defer wg.Done()
			pubMQTTPaho(port, i)
		}()
	}
	wg.Wait()
	// Close waits until the queued readings are processed, so this test does
	// not keep the database busy for the tests after it.
	if err := broker.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	data, _ := broker.handler.getAndClearData()
	if len(data) == 0 {
		t.Fatal("no MQTT readings were buffered")
	}
}

type TestHandler struct{}

func (h *TestHandler) handleConnected(client mqtt.Client) {
	slog.Info("TestHandler Connected")

}

func (h *TestHandler) handleConnectionLost(client mqtt.Client, err error) {
	slog.Error("TestHandler handleConnectionLost")
}

func (h *TestHandler) handleMessage(client mqtt.Client, msg mqtt.Message) {
	//payload := msg.Payload()
	//fmt.Printf("TestHandler handleMessage %s", string(payload))
}

func pubMQTTPaho(port, id int) {
	slog.Info(fmt.Sprintf("<-- %d ", id))
	h := TestHandler{}
	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://%s:%d", "localhost", port))
	opts.SetClientID("test-client " + fmt.Sprintf("%d", id))
	//opts.SetUsername("user")
	//opts.SetPassword("pw")

	opts.SetDefaultPublishHandler(h.handleMessage)
	opts.OnConnect = h.handleConnected
	opts.OnConnectionLost = h.handleConnectionLost
	sensorsClient := mqtt.NewClient(opts)
	if token := sensorsClient.Connect(); token.Wait() && token.Error() != nil {
		fatal(fmt.Sprint(token.Error()))
	}

	endTime := time.Now().Add(loadTestDuration())
	nextUploadTime := time.Now()
	for time.Now().Before(endTime) {
		topic := fmt.Sprintf("pahoClient/test/Temperature%d/data", id)
		token := sensorsClient.Publish(topic, 0, false, fmt.Sprintf("%f", rand.Float32()*100))
		token.Wait()
		time.Sleep(time.Until(nextUploadTime))

		nextUploadTime = time.Now().Add(time.Millisecond * 100)
	}
	slog.Info(fmt.Sprintf("Disconnecting %d\n", id))
	sensorsClient.Disconnect(250)
}

func TestLogging(t *testing.T) {
	iot := regressionIoT()
	iot.Port = freePort(t)

	stop := startTestServer(t, &iot)
	client := http.Client{
		Timeout: 40 * time.Second,
	}
	wg := sync.WaitGroup{}
	for i := range 10 {
		time.Sleep(100 * time.Nanosecond)
		name := fmt.Sprintf("DummyOnlyDev%d-%s", i, uuid.New())

		wg.Go(func() {
			for j := range 10 {

				logMessage := LogMessage{
					Level:  Warning,
					Text:   "test" + fmt.Sprintf(" %d-%d", i, j),
					Device: name,
				}
				jsonData, err := json.Marshal(logMessage)
				if err != nil {
					t.Error(err)
				}

				resp, err := client.Post(fmt.Sprintf("http://localhost:%d%s",
					iot.Port, URILogging), "application/json",
					bytes.NewBuffer(jsonData))
				if err != nil {
					t.Error(err)
					continue
				}
				if resp.StatusCode != http.StatusOK {
					t.Errorf("Failed with status: %s", resp.Status)
				}
				resp.Body.Close()
				time.Sleep(time.Millisecond * 50)
			}

		})

	}
	wg.Wait()
	resp, err := client.Get(fmt.Sprintf("http://localhost:%d%s",
		iot.Port, URIDashboard))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Failed with status: %s", resp.Status)
	}

	stop()
}

// loadTestDuration is how long the load tests publish: 2 s, or 10 s with
// IOTEDGE_LOAD_TESTS set.
func loadTestDuration() time.Duration {
	if os.Getenv("IOTEDGE_LOAD_TESTS") != "" {
		return 10 * time.Second
	}
	return 2 * time.Second
}

// startTestServer serves iot over HTTP on iot.Port until the returned stop
// function is called; both wait until the server is up or down.
func startTestServer(t *testing.T, iot *IoTEdge) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- iot.StartSensorServer(ctx) }()
	url := fmt.Sprintf("http://127.0.0.1:%d%s", iot.Port, URIVersion)
	eventually(t, "test server up", func() bool {
		resp, err := http.Get(url)
		if err != nil {
			return false
		}
		resp.Body.Close()
		return true
	})
	return func() {
		cancel()
		if err := waitReturn(t, done, "StartSensorServer"); err != nil {
			t.Errorf("StartSensorServer: %v", err)
		}
	}
}
