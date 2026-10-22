package iotedge

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	mqttserver "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/pat-rohn/timeseries"
	log "github.com/sirupsen/logrus"
)

type mqttMsg struct {
	topic   string
	payload string
}

type TimeseriesHandler struct {
	data            []*timeseries.TimeseriesImportStruct
	dataMutex       *sync.Mutex
	msgCh           chan mqttMsg
	recordHealth    func(DeviceHealthReport) error
	recordActivity  func(device string) error
	publishRetained func(topic, payload string)
	summarize       func(device string) (DeviceMonitoringSummary, bool)

	queueMu sync.RWMutex // guards closed and sending on msgCh
	closed  bool
	workers sync.WaitGroup
}

func (h *TimeseriesHandler) startWorkers(n int) {
	for i := 0; i < n; i++ {
		h.workers.Add(1)
		go func() {
			defer h.workers.Done()
			for msg := range h.msgCh {
				h.processData(msg.topic, msg.payload)
			}
		}()
	}
}

// enqueue hands a published message to the workers without blocking the broker.
func (h *TimeseriesHandler) enqueue(topic, payload string) {
	h.queueMu.RLock()
	defer h.queueMu.RUnlock()
	if h.closed {
		return
	}
	select {
	case h.msgCh <- mqttMsg{topic: topic, payload: payload}:
	default:
		log.Warn("MQTT message queue full, dropping message")
	}
}

// stopWorkers lets the workers finish the queued messages and waits for them.
func (h *TimeseriesHandler) stopWorkers() {
	h.queueMu.Lock()
	if !h.closed {
		h.closed = true
		close(h.msgCh)
	}
	h.queueMu.Unlock()
	h.workers.Wait()
}

func (h *TimeseriesHandler) processData(topic string, payload string) {
	logger := log.WithFields(log.Fields{"topic": topic})
	splittedTopic := strings.Split(topic, "/")
	if len(splittedTopic) <= 2 {
		logger.Tracef("Ignoring topic without device and sensor: %v", splittedTopic)
		return
	}
	uniqueID := splittedTopic[len(splittedTopic)-2]
	logger = logger.WithField("uniqueID", uniqueID)
	logger.Tracef("Received message: %s from topic: %s (%s)\n", string(payload), uniqueID, splittedTopic)
	if splittedTopic[len(splittedTopic)-1] == "health" {
		if h.recordHealth == nil {
			logger.Warn("Received health report without a monitoring recorder")
			return
		}
		var report DeviceHealthReport
		if err := json.Unmarshal([]byte(payload), &report); err != nil {
			logger.Errorf("Invalid health payload: %v", err)
			return
		}
		if report.Device != uniqueID {
			logger.Errorf("Health payload device %q does not match topic device %q", report.Device, uniqueID)
			return
		}
		if err := h.recordHealth(report); err != nil {
			logger.Errorf("Failed to record health report: %v", err)
			return
		}
		h.publishHomeAssistant(report.Device)
		return
	}
	if splittedTopic[len(splittedTopic)-1] != "data" {
		// Availability, Home Assistant discovery, json/set, json/status, …:
		// not measurements.
		logger.Tracef("Ignoring topic: %v", payload)
		return
	}
	_, err := strconv.ParseFloat(string(payload), 32)
	if err != nil {
		logger.Errorf("Not a valid number: %v", payload)
		return
	}
	if h.recordActivity != nil && len(splittedTopic) >= 4 {
		if err := h.recordActivity(splittedTopic[len(splittedTopic)-3]); err != nil {
			logger.Errorf("Failed to record measurement activity: %v", err)
		}
	}

	timestamp := time.Now().UTC().Format("2006-01-02 15:04:05.000")
	h.dataMutex.Lock()
	defer h.dataMutex.Unlock()

	for _, ts := range h.data {
		if ts.Tag == uniqueID {
			ts.Values = append(ts.Values, string(payload))
			ts.Timestamps = append(ts.Timestamps, timestamp)
			return
		}
	}

	h.data = append(h.data, &timeseries.TimeseriesImportStruct{
		Tag:        uniqueID,
		Values:     []string{string(payload)},
		Timestamps: []string{timestamp},
	})
}

func (h *TimeseriesHandler) publishHomeAssistant(device string) {
	if h.publishRetained == nil || h.summarize == nil {
		return
	}
	summary, ok := h.summarize(device)
	if !ok {
		return
	}
	for _, m := range homeAssistantDiscovery(device) {
		h.publishRetained(m.Topic, m.Payload)
	}
	for _, m := range homeAssistantState(summary) {
		h.publishRetained(m.Topic, m.Payload)
	}
}

func (h *TimeseriesHandler) getAndClearData() ([]timeseries.TimeseriesImportStruct, error) {
	h.dataMutex.Lock()
	defer h.dataMutex.Unlock()
	var returnData []timeseries.TimeseriesImportStruct
	for _, impstr := range h.data {
		var timestamps []string
		timestamps = append(timestamps, impstr.Timestamps...)
		var values []string
		values = append(values, impstr.Values...)
		log.Tracef("copied %d/%d entries", len(impstr.Values), len(impstr.Timestamps))
		returnData = append(returnData, timeseries.TimeseriesImportStruct{
			Tag:        impstr.Tag,
			Timestamps: timestamps,
			Values:     values,
		})
	}
	h.data = h.data[:0]
	return returnData, nil
}

// brokerHook authenticates MQTT clients with the shared ingest token and hands
// every client publication to the TimeseriesHandler.
type brokerHook struct {
	mqttserver.HookBase
	auth    *ingestAuth
	handler *TimeseriesHandler
}

func (h *brokerHook) ID() string { return "iotedge" }

func (h *brokerHook) Provides(b byte) bool {
	return b == mqttserver.OnConnectAuthenticate || b == mqttserver.OnACLCheck || b == mqttserver.OnPublished
}

func (h *brokerHook) OnConnectAuthenticate(cl *mqttserver.Client, pk packets.Packet) bool {
	return h.auth.allow(string(pk.Connect.Password), remoteIP(cl.Net.Remote), "mqtt", time.Now())
}

func (h *brokerHook) OnACLCheck(cl *mqttserver.Client, topic string, write bool) bool {
	return true
}

func (h *brokerHook) OnPublished(cl *mqttserver.Client, pk packets.Packet) {
	if cl.Net.Inline { // the server's own Home Assistant messages
		return
	}
	h.handler.enqueue(pk.TopicName, string(pk.Payload))
}

func remoteIP(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	return host
}

// mqttBroker is the embedded broker and the measurement buffer it fills.
type mqttBroker struct {
	server    *mqttserver.Server
	handler   *TimeseriesHandler
	closeOnce sync.Once
	closeErr  error
}

// Close stops the broker and waits until the queued messages are buffered.
// Further calls do nothing.
func (b *mqttBroker) Close() error {
	b.closeOnce.Do(func() {
		b.closeErr = b.server.Close()
		b.handler.stopWorkers()
	})
	return b.closeErr
}

// startMQTTBroker starts the broker on port and returns once it listens.
func (s *IoTEdge) startMQTTBroker(port int) (*mqttBroker, error) {
	logFields := log.Fields{"tech": "mqtt", "fnct": "startMQTTBroker"}
	if err := s.initIngestAuth(); err != nil {
		return nil, err
	}
	config := s.IoTConfig
	deviceDB := s.DeviceDB
	handler := &TimeseriesHandler{
		data:      []*timeseries.TimeseriesImportStruct{},
		dataMutex: &sync.Mutex{},
		msgCh:     make(chan mqttMsg, 1024),
		recordHealth: func(report DeviceHealthReport) error {
			return deviceDB.RecordDeviceHealth(report, time.Now())
		},
		recordActivity: func(device string) error {
			return deviceDB.RecordDeviceActivity(device, time.Now())
		},
	}
	handler.startWorkers(runtime.NumCPU())

	server := mqttserver.New(&mqttserver.Options{
		InlineClient: true,
		Logger:       slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})),
	})
	if err := server.AddHook(&brokerHook{auth: s.ingestAuth, handler: handler}, nil); err != nil {
		return nil, err
	}
	tcp := listeners.NewTCP(listeners.Config{ID: "mqtt-broker", Address: fmt.Sprintf(":%d", port)})
	if err := server.AddListener(tcp); err != nil {
		return nil, fmt.Errorf("MQTT listener on port %d: %w", port, err)
	}
	if err := server.Serve(); err != nil {
		return nil, err
	}

	if config.Monitoring.HomeAssistantDiscovery {
		handler.publishRetained = func(topic, payload string) {
			if err := server.Publish(topic, []byte(payload), true, 1); err != nil {
				log.WithFields(logFields).Warnf("Failed to publish %s: %v", topic, err)
			}
		}
		handler.summarize = func(device string) (DeviceMonitoringSummary, bool) {
			summary, err := deviceDB.GetMonitoringSummary(config.Monitoring, time.Now())
			if err != nil {
				log.WithFields(logFields).Errorf("Failed to summarize monitoring: %v", err)
				return DeviceMonitoringSummary{}, false
			}
			for _, d := range summary.Devices {
				if d.Device == device {
					return d, true
				}
			}
			return DeviceMonitoringSummary{}, false
		}
	}
	log.WithFields(logFields).Infof("MQTT broker listening on port %d", port)
	return &mqttBroker{server: server, handler: handler}, nil
}

// mqttShutdownBudget bounds the final flush when the broker stops.
const mqttShutdownBudget = 10 * time.Second

// StartMQTTBroker runs the broker on IoTConfig.MQTTPort and stores (or
// forwards) the buffered measurements every UploadInterval seconds. When ctx
// is cancelled it stops the broker and flushes what is still buffered.
func (s *IoTEdge) StartMQTTBroker(ctx context.Context) error {
	logFields := log.Fields{"tech": "mqtt", "fnct": "StartMQTTBroker"}
	config := s.IoTConfig
	broker, err := s.startMQTTBroker(config.MQTTPort)
	if err != nil {
		return err
	}
	defer broker.Close()

	interval := time.Second * time.Duration(config.UploadInterval)
	if interval <= 0 {
		log.WithFields(logFields).Warnf("UploadInterval %d is not positive, using 30 s", config.UploadInterval)
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.WithFields(logFields).Info("Stopping MQTT broker, flushing buffered readings")
			if err := broker.Close(); err != nil {
				log.WithFields(logFields).Warnf("Closing MQTT broker: %v", err)
			}
			s.storeMQTTData(broker.handler, time.Now().Add(mqttShutdownBudget), false)
			return nil
		case <-ticker.C:
			s.storeMQTTData(broker.handler, time.Now().Add(interval), true)
		}
	}
}

// storeMQTTData writes (or forwards) the buffered readings; inserts must
// finish before deadline.
func (s *IoTEdge) storeMQTTData(handler *TimeseriesHandler, deadline time.Time, forwardAsync bool) {
	logFields := log.Fields{"tech": "mqtt", "fnct": "storeMQTTData"}
	config := s.IoTConfig
	data, err := handler.getAndClearData()
	if err != nil {
		log.WithFields(logFields).Errorf("Failed to get and clear data %v", err)
		return
	}
	if len(data) == 0 {
		return
	}
	log.WithFields(logFields).Debugf("Got data %d", len(data))
	if len(config.MQTTRedirectAddress) > 0 {
		log.WithFields(logFields).Debugf("Redirect data to %s", config.MQTTRedirectAddress)
		if forwardAsync {
			go sendData(&data, config.MQTTRedirectAddress)
		} else if err := sendData(&data, config.MQTTRedirectAddress); err != nil {
			log.WithFields(logFields).Errorf("Failed to forward MQTT data: %v", err)
		}
		return
	}
	if err := insertData(s.DeviceDB.DbHandler, data, deadline, config.TimeseriesTable); err != nil {
		log.WithFields(logFields).Errorf("Failed to insert MQTT data: %v", err)
	}
}

func insertData(dbh *timeseries.DbHandler, data []timeseries.TimeseriesImportStruct, nextUploadTime time.Time, table string) error {
	logger := log.WithFields(log.Fields{"tech": "mqtt", "fnct": "insertData"})
	failed := 0
	for i, tsVal := range data {
		timeTillNextIncome := time.Until(nextUploadTime)
		if timeTillNextIncome <= time.Second*2 {
			logger.Errorln("Too much data, abort")
			return fmt.Errorf("aborted after %d/%d series: time budget exceeded", i, len(data))
		}
		logger.Tracef("insert %d/%d entries for %s ", len(tsVal.Timestamps), len(tsVal.Values), tsVal.Tag)
		inserted := false
		timeOut := time.Now().Add(time.Second * 2)
		for time.Now().Before(timeOut) {
			err := dbh.InsertTimeseries(tsVal, true, table)
			if err != nil {
				logger.Warnf("Failed to insert values into database: %v", err)
				time.Sleep(time.Millisecond * 50)
			} else {
				inserted = true
				break
			}
		}
		if !inserted {
			logger.Errorf("Giving up on series %s after retries", tsVal.Tag)
			failed++
		}
	}
	if failed > 0 {
		return fmt.Errorf("failed to insert %d/%d series", failed, len(data))
	}
	return nil
}

func sendData(data *[]timeseries.TimeseriesImportStruct, url string) error {
	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}
	client := http.Client{Timeout: 5 * time.Second}
	resp, err := client.Post(url+URISaveTimeseries, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		log.Errorf("Failed to send data: %v", err)
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		log.Errorf("Failed with status: %s", resp.Status)
		return fmt.Errorf("failed with status: %s", resp.Status)
	}
	respStr, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Error(err)
	}
	log.Info(string(respStr))
	return nil
}
