package iotedge

// shutdown_test.go — Phase 1 of doc/EDGE_SERVER_WORKBOOK.md: graceful
// shutdown via context (T5) and flushing buffered MQTT readings (T4).

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
)

func waitReturn(t *testing.T, done <-chan error, what string) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(10 * time.Second):
		t.Fatalf("%s did not return after cancel", what)
		return nil
	}
}

func TestSensorServerStopsOnCancel(t *testing.T) {
	iot := regressionIoT()
	iot.Port = freePort(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- iot.StartSensorServer(ctx) }()

	url := fmt.Sprintf("http://127.0.0.1:%d%s", iot.Port, URIVersion)
	eventually(t, "server up", func() bool {
		resp, err := http.Get(url)
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	})

	cancel()
	if err := waitReturn(t, done, "StartSensorServer"); err != nil {
		t.Fatalf("StartSensorServer: %v", err)
	}
	if _, err := http.Get(url); err == nil {
		t.Fatal("server still answers after cancel")
	}
}

func TestMQTTBrokerFlushesReadingsOnCancel(t *testing.T) {
	iot := regressionIoT()
	iot.IoTConfig.MQTTPort = freePort(t)
	iot.IoTConfig.UploadInterval = 3600 // only the shutdown flush can store it
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- iot.StartMQTTBroker(ctx) }()

	var client mqtt.Client
	eventually(t, "broker up", func() bool {
		c, err := connectClient(iot.IoTConfig.MQTTPort, "")
		client = c
		return err == nil
	})
	tag := "flush-" + uuid.NewString()
	publish(t, client, "frick/test/dev/"+tag+"/data", "42")
	client.Disconnect(100)
	time.Sleep(100 * time.Millisecond) // let the broker hand the reading to its workers

	cancel()
	if err := waitReturn(t, done, "StartMQTTBroker"); err != nil {
		t.Fatalf("StartMQTTBroker: %v", err)
	}
	var n int
	row := iot.DeviceDB.DB.QueryRow(iot.DeviceDB.rebind(
		"SELECT COUNT(*) FROM "+iot.IoTConfig.TimeseriesTable+" WHERE tag = ?"), tag)
	if err := row.Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("stored readings for %s = %d, want 1 after shutdown flush", tag, n)
	}
}
