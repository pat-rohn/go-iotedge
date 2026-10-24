package iotedge

// mqtt_broker_test.go — Phase 1 of doc/EDGE_SERVER_WORKBOOK.md: embedded
// mochi-mqtt v2 broker consumed through hooks (A1) with the shared ingest
// token as MQTT password (S3).

import (
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
)

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// testBroker starts the broker of a test IoTEdge on a free port.
func testBroker(t *testing.T, sec SecurityConfig, homeAssistant bool) (*IoTEdge, *mqttBroker, int) {
	t.Helper()
	iot := regressionIoT()
	iot.status = newServerStatus()
	iot.IoTConfig.Security = sec
	iot.IoTConfig.Monitoring.HomeAssistantDiscovery = homeAssistant
	port := freePort(t)
	broker, err := iot.startMQTTBroker(port)
	if err != nil {
		t.Fatalf("startMQTTBroker: %v", err)
	}
	t.Cleanup(func() { broker.Close() })
	return &iot, broker, port
}

func connectClient(port int, password string) (mqtt.Client, error) {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://127.0.0.1:%d", port))
	opts.SetClientID("test-" + uuid.NewString())
	opts.SetConnectRetry(false)
	opts.SetAutoReconnect(false)
	if password != "" {
		opts.SetUsername("device")
		opts.SetPassword(password)
	}
	client := mqtt.NewClient(opts)
	token := client.Connect()
	if !token.WaitTimeout(5 * time.Second) {
		return nil, fmt.Errorf("connect timed out")
	}
	return client, token.Error()
}

func publish(t *testing.T, client mqtt.Client, topic, payload string) {
	t.Helper()
	token := client.Publish(topic, 1, false, payload)
	if !token.WaitTimeout(5*time.Second) || token.Error() != nil {
		t.Fatalf("publish %s: %v", topic, token.Error())
	}
}

// eventually polls cond for up to 5 s.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func bufferedValues(h *TimeseriesHandler, tag string) []string {
	h.dataMutex.Lock()
	defer h.dataMutex.Unlock()
	for _, ts := range h.data {
		if ts.Tag == tag {
			return append([]string(nil), ts.Values...)
		}
	}
	return nil
}

func TestMQTTBrokerBuffersMeasurementsAtAnyDepth(t *testing.T) {
	_, broker, port := testBroker(t, SecurityConfig{}, false)
	client, err := connectClient(port, "")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Disconnect(100)

	device := "mqtt-dev-" + uuid.NewString()
	shallow := "co2-" + uuid.NewString()
	deep := "temp-" + uuid.NewString()
	publish(t, client, "frick/bedroom/"+device+"/"+shallow+"/data", "591")
	publish(t, client, "a/b/c/d/e/f/g/"+device+"/"+deep+"/data", "21.5")
	publish(t, client, "frick/bedroom/"+device+"/"+shallow+"/json/set", `{"x":1}`)

	eventually(t, "both measurements buffered", func() bool {
		return len(bufferedValues(broker.handler, shallow)) == 1 && len(bufferedValues(broker.handler, deep)) == 1
	})
	if got := bufferedValues(broker.handler, shallow); got[0] != "591" {
		t.Errorf("value = %v, want [591]", got)
	}
}

func TestMQTTBrokerRecordsHealthAndPublishesHomeAssistant(t *testing.T) {
	iot, _, port := testBroker(t, SecurityConfig{}, true)
	device := "mqtt-health-" + uuid.NewString()

	subscriber, err := connectClient(port, "")
	if err != nil {
		t.Fatalf("connect subscriber: %v", err)
	}
	defer subscriber.Disconnect(100)
	var mu sync.Mutex
	received := map[string]string{}
	token := subscriber.Subscribe("homeassistant/sensor/"+device+"_last_seen/config", 1, func(_ mqtt.Client, m mqtt.Message) {
		mu.Lock()
		received[m.Topic()] = string(m.Payload())
		mu.Unlock()
	})
	if !token.WaitTimeout(5*time.Second) || token.Error() != nil {
		t.Fatalf("subscribe: %v", token.Error())
	}

	publisher, err := connectClient(port, "")
	if err != nil {
		t.Fatalf("connect publisher: %v", err)
	}
	defer publisher.Disconnect(100)
	publish(t, publisher, "iotedge/"+device+"/health",
		fmt.Sprintf(`{"device":%q,"boot_id":"b1","firmware_version":"fw1"}`, device))

	eventually(t, "health recorded", func() bool {
		summary, err := iot.DeviceDB.GetMonitoringSummary(iot.IoTConfig.Monitoring, time.Now())
		if err != nil {
			return false
		}
		for _, d := range summary.Devices {
			if d.Device == device && d.FirmwareVersion == "fw1" {
				return true
			}
		}
		return false
	})
	eventually(t, "home assistant discovery", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return strings.Contains(received["homeassistant/sensor/"+device+"_last_seen/config"], "enlightened:"+device)
	})
}

// Firmware (ArduinoMqttClient) connects with MQTT 3.1.1, no credentials and a
// retained "offline" will on iotedge/<device>/availability for Home Assistant.
func TestMQTTBrokerDeliversFirmwareLastWill(t *testing.T) {
	_, _, port := testBroker(t, SecurityConfig{}, false)
	device := "mqtt-will-" + uuid.NewString()
	availability := "iotedge/" + device + "/availability"

	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(mqttConnectWithWill("Arduino-"+uuid.NewString()[:8], availability, "offline")); err != nil {
		t.Fatal(err)
	}
	connack := make([]byte, 4)
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.ReadFull(conn, connack); err != nil || connack[0] != 0x20 || connack[3] != 0 {
		t.Fatalf("CONNACK = %x, %v; want accepted", connack, err)
	}
	conn.Close() // no DISCONNECT, like a power loss

	subscriber, err := connectClient(port, "")
	if err != nil {
		t.Fatal(err)
	}
	defer subscriber.Disconnect(100)
	got := make(chan string, 4)
	subscriber.Subscribe(availability, 1, func(_ mqtt.Client, m mqtt.Message) { got <- string(m.Payload()) })
	deadline := time.After(5 * time.Second)
	for {
		select {
		case payload := <-got:
			if payload == "offline" {
				return
			}
		case <-deadline:
			t.Fatal("retained last will 'offline' not delivered")
		}
	}
}

// mqttConnectWithWill builds an MQTT 3.1.1 CONNECT packet as ArduinoMqttClient
// sends it: clean session, no credentials, retained QoS 1 will.
func mqttConnectWithWill(clientID, willTopic, willMessage string) []byte {
	str := func(v string) []byte { return append([]byte{byte(len(v) >> 8), byte(len(v))}, v...) }
	body := append(str("MQTT"), 4, 0x02|0x04|0x08|0x20, 0, 60) // level 4; clean, will, QoS 1, retain; keep-alive 60 s
	body = append(body, str(clientID)...)
	body = append(body, str(willTopic)...)
	body = append(body, str(willMessage)...)
	packet := []byte{0x10}
	for n := len(body); ; {
		b := byte(n % 128)
		n /= 128
		if n > 0 {
			b |= 0x80
		}
		packet = append(packet, b)
		if n == 0 {
			break
		}
	}
	return append(packet, body...)
}

// Close stops the broker and waits until the queued messages are processed,
// so nothing keeps writing to the database afterwards.
func TestMQTTBrokerCloseDrainsQueue(t *testing.T) {
	_, broker, _ := testBroker(t, SecurityConfig{}, false)
	tag := "drain-" + uuid.NewString()
	for i := 0; i < 50; i++ {
		broker.handler.enqueue("dev/"+tag+"/data", "1")
	}
	if err := broker.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := len(bufferedValues(broker.handler, tag)); got != 50 {
		t.Fatalf("buffered after Close = %d, want 50", got)
	}
	broker.handler.enqueue("dev/"+tag+"/data", "1") // must not panic
	if err := broker.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestMQTTBrokerAuthOffAcceptsAnyClient(t *testing.T) {
	_, _, port := testBroker(t, SecurityConfig{IngestToken: testIngestToken}, false)
	client, err := connectClient(port, "")
	if err != nil {
		t.Fatalf("connect without password, auth off: %v", err)
	}
	client.Disconnect(100)
}

func TestMQTTBrokerAuthEnforceRequiresToken(t *testing.T) {
	_, _, port := testBroker(t, SecurityConfig{IngestAuth: IngestAuthEnforce, IngestToken: testIngestToken}, false)
	if client, err := connectClient(port, ""); err == nil {
		client.Disconnect(100)
		t.Error("connect without password accepted")
	}
	if client, err := connectClient(port, "wrong-token"); err == nil {
		client.Disconnect(100)
		t.Error("connect with wrong password accepted")
	}
	client, err := connectClient(port, testIngestToken)
	if err != nil {
		t.Fatalf("connect with token: %v", err)
	}
	client.Disconnect(100)
}

func TestMQTTBrokerAuthWarnReportsClient(t *testing.T) {
	iot, _, port := testBroker(t, SecurityConfig{IngestAuth: IngestAuthWarn, IngestToken: testIngestToken}, false)
	client, err := connectClient(port, "")
	if err != nil {
		t.Fatalf("connect without password, warn: %v", err)
	}
	client.Disconnect(100)
	clients := iot.ingestAuth.unauthenticated()
	if len(clients) != 1 || clients[0].IP != "127.0.0.1" {
		t.Fatalf("unauthenticated = %+v, want 127.0.0.1", clients)
	}
}
