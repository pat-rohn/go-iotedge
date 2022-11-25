package main

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	mqttserver "github.com/mochi-co/mqtt/server"
	"github.com/mochi-co/mqtt/server/listeners"
	iotedge "github.com/pat-rohn/go-iotedge"
	"github.com/pat-rohn/timeseries"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"gopkg.in/natefinch/lumberjack.v2"
)

var loglevel string
var logPath string

func initGlobalFlags() {
	switch loglevel {
	case "--trace", "t":
		log.SetLevel(log.TraceLevel)
	case "--info", "i":
		log.SetLevel(log.InfoLevel)
	case "--warn", "w":
		log.SetLevel(log.WarnLevel)
	case "--error", "e":
		log.SetLevel(log.ErrorLevel)
	default:
		fmt.Printf("Invalid log level '%s'\n", loglevel)
	}
	fmt.Printf("LogLevel is set to %s\n", loglevel)
}

func main() {
	name := "IoT-Server"
	fmt.Println(name)
	var rootCmd = &cobra.Command{
		Use:   "IoT-Server",
		Short: "IoT-Server receives and stores timeseries",
	}

	var startServerCmd = &cobra.Command{
		Use:   "start",
		Args:  cobra.MinimumNArgs(0),
		Short: "",
		Long:  ``,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(logPath) > 0 {
				SetLogfile(name + "-start")
			}
			go startMQTTBroker()
			if err := startServer(); err != nil {
				return err
			}
			return nil
		},
	}

	var simMqttDeviceCmd = &cobra.Command{
		Use:   "mqtt-sim",
		Args:  cobra.MinimumNArgs(0),
		Short: "",
		Long:  ``,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(logPath) > 0 {
				SetLogfile(name + "-mqtt-sim")
			}
			go startMQTTBroker()
			time.Sleep(time.Second * 3)
			if err := mqttSim(); err != nil {
				return err
			}
			return nil
		},
	}

	var createTableCmd = &cobra.Command{
		Use:   "create-table",
		Args:  cobra.MinimumNArgs(0),
		Short: "",
		Long:  ``,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := createTable(); err != nil {
				return err
			}
			return nil
		},
	}

	var ConfigureDeviceCmd = &cobra.Command{
		Use:   "conf-device devicename interval buffer",
		Args:  cobra.MinimumNArgs(3),
		Short: "",
		Long:  ``,
		RunE: func(cmd *cobra.Command, args []string) error {
			interval, err := strconv.ParseFloat(args[1], 32)
			if err != nil {
				return err
			}
			buffer, err := strconv.ParseInt(args[2], 10, 32)
			if err != nil {
				return err
			}
			edge := iotedge.GetConfig()
			err = edge.InitializeDB()
			if err != nil {
				return err
			}
			iotDevice, err := edge.GetDevice(args[0])
			if err != nil {
				return err
			}
			if err = iotDevice.Configure(float32(interval), int(buffer), edge.GormDB); err != nil {
				return err
			}
			return nil
		},
	}

	var ConfigureSensorCmd = &cobra.Command{
		Use:   "conf-sensor sensorname offset",
		Args:  cobra.MinimumNArgs(3),
		Short: "",
		Long:  `e.g IoTServer conf-sensor -v i Basel3 Basel3Humidity -- -2 `,
		RunE: func(cmd *cobra.Command, args []string) error {
			offset, err := strconv.ParseFloat(args[2], 32)
			if err != nil {
				return err
			}
			sensor := args[1]
			edge := iotedge.GetConfig()
			err = edge.InitializeDB()
			if err != nil {
				return err
			}
			iotDevice, err := edge.GetDevice(args[0])
			if err != nil {
				return err
			}
			if err = iotDevice.ConfigureSensor(float32(offset), sensor, edge.GormDB); err != nil {
				return err
			}
			return nil
		},
	}

	rootCmd.PersistentFlags().StringVarP(&loglevel, "verbose", "v", "w", "verbosity")
	rootCmd.PersistentFlags().StringVarP(&logPath, "logfile", "l", "", "activate and create logfile")

	rootCmd.AddCommand(startServerCmd)
	rootCmd.AddCommand(simMqttDeviceCmd)
	rootCmd.AddCommand(createTableCmd)
	rootCmd.AddCommand(ConfigureDeviceCmd)
	rootCmd.AddCommand(ConfigureSensorCmd)

	cobra.OnInitialize(initGlobalFlags)
	rootCmd.Execute()

}

func createTable() error {
	iotConfig := iotedge.GetConfig()
	db := timeseries.New(iotConfig.DatabaseConfig)
	defer db.CloseDatabase()
	if err := db.CreateDatabase(); err != nil {
		log.Error("failed to create DB: %v", err)
		return err
	}
	if err := db.CreateTimeseriesTable(); err != nil {
		log.Error("failed to create DB: %v", err)
		return err
	}
	return nil
}

func startServer() error {
	iot := iotedge.GetConfig()
	return iot.StartSensorServer()
}

func SetLogfile(filename string) {
	var path string
	operatingSystem := runtime.GOOS
	switch operatingSystem {
	case "windows":
		path = "C:/ProgramData/"
	case "linux":
		os.Mkdir("./log", 0644)
		path = "./log/"
	default:
		os.Mkdir("./log", 0644)
		path = "./log/"
	}
	fmt.Printf("%s: Logging path set to '%v'\n", operatingSystem, path)
	log.SetOutput(&lumberjack.Logger{
		Filename:   fmt.Sprintf("%s%s.log", path, filename),
		MaxSize:    500, // megabytes
		MaxBackups: 3,
		MaxAge:     28,   //days
		Compress:   true, // disabled by default
	})
}

var messagePubHandler mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
	fmt.Printf("Received message: %s from topic: %s\n", msg.Payload(), msg.Topic())
}

var connectHandler mqtt.OnConnectHandler = func(client mqtt.Client) {
	fmt.Println("Connected")
}

var connectLostHandler mqtt.ConnectionLostHandler = func(client mqtt.Client, err error) {
	fmt.Printf("Connect lost: %v", err)
}

func mqttSim() error {
	var broker = "192.168.1.101"
	var port = 1883
	fmt.Printf("tcp://%s:%d\n", broker, port)
	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://%s:%d", broker, port))
	opts.SetClientID("mqtt-test")
	opts.SetUsername("chropfi")
	opts.SetPassword("chropfi-test")
	opts.SetDefaultPublishHandler(messagePubHandler)
	opts.OnConnect = connectHandler
	opts.OnConnectionLost = connectLostHandler
	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	sub(client)
	publish(client)

	client.Disconnect(250)
	return nil
}

func publish(client mqtt.Client) {
	num := 10000
	for i := 0; i < num; i++ {
		text := fmt.Sprintf("Message %d", i)
		token := client.Publish("livingroom/env", 0, false, text)
		token.Wait()
		time.Sleep(time.Second)
		msg := `  {
			"temperature": ` + fmt.Sprintf("%f", 23.20) + ` ,
			"humidity": ` + fmt.Sprintf("%f", 53.20) + ` 
		  }`
		fmt.Println(msg)
	}
}

func sub(client mqtt.Client) {
	topic := "topic/test"
	token := client.Subscribe(topic, 1, nil)
	token.Wait()
	fmt.Printf("Subscribed to topic: %s", topic)
}

func startMQTTBroker() {
	server := mqttserver.NewServer(nil)

	tcp := listeners.NewTCP("t1", ":9333")

	err := server.AddListener(tcp, nil)
	if err != nil {
		log.Fatal(err)
	}

	err = server.Serve()
	if err != nil {
		log.Fatal(err)
	}
}
