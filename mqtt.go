package iotedge

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	mqttserver "github.com/mochi-co/mqtt/server"
	"github.com/mochi-co/mqtt/server/listeners"
	"github.com/pat-rohn/timeseries"
	log "github.com/sirupsen/logrus"
)

type TimeseriesHandler struct {
	DataMessageHandler *mqtt.MessageHandler
	data               []*timeseries.TimeseriesImportStruct
	dataMutex          *sync.Mutex
}

type MQTTEdge struct {
	MQTTserver        *mqttserver.Server
	timeseriesHandler *TimeseriesHandler
}

func (h *TimeseriesHandler) handleConnected(client mqtt.Client) {
	fmt.Println("TimeseriesHandler Connected")
	logFields := log.Fields{"Handler": "TimeseriesHandler", "fnct": "handleConnected"}
	log.WithFields(logFields).Dup().Logger.Infoln("New Client")

}

func (h *TimeseriesHandler) handleConnectionLost(client mqtt.Client, err error) {
	logFields := log.Fields{"Handler": "TimeseriesHandler", "fnct": "handleConnectionLost"}
	log.WithFields(logFields).Errorf("Connection lost: %v", err)
	fmt.Println("TimeseriesHandler handleConnectionLost")

}

func (h *TimeseriesHandler) handleMessage(client mqtt.Client, msg mqtt.Message) {
	payload := msg.Payload()
	logFields := log.Fields{"fnct": "handleMessage"}

	go h.processData(msg.Topic(), string(payload))
	log.WithFields(logFields).Tracef("Connection lost: %s", payload)
	//fmt.Printf("TestHandler handleMessage %s", string(payload))

}

func (h *TimeseriesHandler) processData(topic string, payload string) {
	splittedTopic := strings.Split(topic, "/")
	if len(splittedTopic) <= 2 {
		log.Errorf("No valid data to store into the dabase: %v", splittedTopic)
		return
	}
	uniqueID := splittedTopic[len(splittedTopic)-2]
	log.Tracef("Received message: %s from topic: %s (%s)\n", string(payload), uniqueID, splittedTopic)
	_, err := strconv.ParseFloat(string(payload), 32)
	if err != nil {
		log.Errorf("Not a valid number: %v", payload)
		return
	}
	timestamp := time.Now().UTC().Format("2006-01-02 15:04:05.000")
	h.dataMutex.Lock()
	defer h.dataMutex.Unlock()

	for _, ts := range h.data {
		if ts.Tag == uniqueID {
			ts.Values = append(ts.Values, string(payload))
			ts.Timestamps = append(ts.Timestamps, timestamp)
			log.Tracef("exists %s (%v) %v", uniqueID, len(ts.Values), ts.Values)
			//log.Tracef("exists %s (%v) %v", uniqueID, ts.Values, ts.Timestamps)

			return
		}
	}

	log.Tracef("new %s", uniqueID)
	h.data = append(h.data, &timeseries.TimeseriesImportStruct{
		Tag:        uniqueID,
		Values:     []string{string(payload)},
		Timestamps: []string{timestamp},
	})
}

func (h *TimeseriesHandler) getAndClearData() ([]timeseries.TimeseriesImportStruct, error) {
	h.dataMutex.Lock()
	defer h.dataMutex.Unlock()
	// deep copy
	var returnData []timeseries.TimeseriesImportStruct
	for _, impstr := range h.data {
		var timestamps []string
		//lenTS := copy(timestamps, impstr.Timestamps)
		timestamps = append(timestamps, impstr.Timestamps...)

		var values []string
		values = append(values, impstr.Values...)
		log.Infof("copied %d/%d entries", len(impstr.Values), len(impstr.Timestamps))

		returnData = append(returnData, timeseries.TimeseriesImportStruct{
			Tag:        impstr.Tag,
			Timestamps: timestamps,
			Values:     values,
		})
	}
	log.Info("Clear slice")
	h.data = nil
	h.data = []*timeseries.TimeseriesImportStruct{}
	return returnData, nil
}

func sub(client mqtt.Client, topic string) {
	token := client.Subscribe(topic, 1, nil)
	for !token.Wait() {
		time.Sleep(time.Second * 1)
		fmt.Printf("Waiting for topic: %s", topic)
	}
}

func StartMQTTBroker(port int, dbConfig timeseries.DBConfig) {
	log.Infof("start mqtt broker on port %d", port)
	fmt.Printf("start mqtt broker on port %d\n", port)
	handler := TimeseriesHandler{
		data:      []*timeseries.TimeseriesImportStruct{},
		dataMutex: &sync.Mutex{},
	}
	mqttEdge := MQTTEdge{
		MQTTserver:        mqttserver.NewServer(nil),
		timeseriesHandler: &handler,
	}
	go func() {

		tcp := listeners.NewTCP("mqtt-broker", fmt.Sprintf(":%d", port))

		err := mqttEdge.MQTTserver.AddListener(tcp, nil)
		if err != nil {
			log.Fatal(err)
		}

		err = mqttEdge.MQTTserver.Serve()
		if err != nil {
			log.Fatal(err)
		}
	}()

	var broker = "localhost"
	fmt.Printf("tcp://%s:%d\n", broker, port)
	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://%s:%d", broker, port))
	opts.SetClientID("mqtt-pinger")
	//opts.SetUsername("user")
	//opts.SetPassword("pw")

	opts.SetDefaultPublishHandler(mqttEdge.timeseriesHandler.handleMessage)
	opts.OnConnect = mqttEdge.timeseriesHandler.handleConnected
	opts.OnConnectionLost = mqttEdge.timeseriesHandler.handleConnectionLost
	databaseClient := mqtt.NewClient(opts)
	time.Sleep(time.Second * 2)
	if token := databaseClient.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
	// subscribes to everything
	sub(databaseClient, "+/+/+/+/+/+/data")
	sub(databaseClient, "+/+/+/+/+/data")
	sub(databaseClient, "+/+/+/+/data")
	sub(databaseClient, "+/+/+/data")
	sub(databaseClient, "+/+/data")
	sub(databaseClient, "+/data")
	go publishPing(databaseClient, "/server/ping/data")

	nextUploadTime := time.Now().Add(time.Second * 30)
	dbh := timeseries.New(dbConfig)
	dbh.CloseDatabase()
	for {
		dur := time.Until(nextUploadTime)
		<-time.After(dur)
		nextUploadTime = time.Now().Add(time.Second * 30)
		data, err := handler.getAndClearData()
		if err != nil {
			log.Errorf("Failed to get and clear data %v", err)
			continue
		}
		log.Infof("Got data %d", len(data))

		insertData(&dbh, data, nextUploadTime)

		time.Sleep(time.Second * 5)
	}
}

func insertData(dbh *timeseries.DbHandler, data []timeseries.TimeseriesImportStruct, nextUploadTime time.Time) error {
	if err := dbh.OpenDatabase(); err != nil {
		return err
	}
	defer dbh.CloseDatabase()
	for _, tsVal := range data {
		timeTillNextIncome := time.Until(nextUploadTime)
		log.Warnf("timeTillNextIncome: %v", timeTillNextIncome.String())
		if timeTillNextIncome <= time.Second*2 {
			log.Errorln("Too much data, abort")
			break

		}
		log.Tracef("insert %d/%d entries for %s ",
			len(tsVal.Timestamps), len(tsVal.Values), tsVal.Tag)
		timeOut := time.Now().Add(time.Second * 2)
		err := dbh.InsertTimeseries(tsVal, true)
		for time.Now().Before(timeOut) && err != nil {

			if err != nil {
				log.Warnf("Failed to insert values into database: %v", err)
				time.Sleep(time.Millisecond * 50)
			}
		}
		if err != nil {
			log.Errorf("Failed to insert values into database: %v", err)
		}

	}
	return nil
}

func publishPing(client mqtt.Client, topic string) {
	for {
		token := client.Publish(topic, 0, false, "-10")
		token.Wait()
		//test
		//time.Sleep(time.Millisecond * 100)
		time.Sleep(time.Second * 30)
	}
}
