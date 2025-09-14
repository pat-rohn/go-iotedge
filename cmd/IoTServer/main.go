package main

import (
	"fmt"
	"strconv"

	iotedge "github.com/pat-rohn/go-iotedge"
	"github.com/pat-rohn/timeseries"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var loglevel string

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

			if err := startServer(); err != nil {
				return err
			}
			return nil
		},
	}

	var mqttServerCmd = &cobra.Command{
		Use:   "mqtt",
		Args:  cobra.MinimumNArgs(0),
		Short: "",
		Long:  ``,
		RunE: func(cmd *cobra.Command, args []string) error {
			conf := iotedge.GetConfig()
			iotedge.StartMQTTBroker(conf.MQTTPort, conf)

			return nil
		},
	}

	var createTableCmd = &cobra.Command{
		Use:   "create-table",
		Args:  cobra.MinimumNArgs(0),
		Short: "",
		Long:  ``,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := CreateTimeseriesTable(); err != nil {
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
			edge := iotedge.New(iotedge.GetConfig())

			dev, err := edge.DeviceDB.GetDevice(args[0])
			if err != nil {
				return err
			}
			dev.Interval = float32(interval)
			dev.Buffer = int(buffer)
			if err = edge.DeviceDB.Configure(dev); err != nil {
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
			sensorName := args[1]
			edge := iotedge.New(iotedge.GetConfig())

			iotDevice, err := edge.DeviceDB.GetDevice(args[0])
			if err != nil {
				return err
			}
			sensor := iotedge.Sensor{
				Name:         sensorName,
				SensorOffset: float32(offset),
				DeviceID:     iotDevice.ID,
			}
			if err = edge.DeviceDB.ConfigureSensor(sensor); err != nil {
				return err
			}
			return nil
		},
	}

	rootCmd.PersistentFlags().StringVarP(&loglevel, "verbose", "v", "w", "verbosity")

	rootCmd.AddCommand(startServerCmd)
	rootCmd.AddCommand(mqttServerCmd)
	rootCmd.AddCommand(createTableCmd)
	rootCmd.AddCommand(ConfigureDeviceCmd)
	rootCmd.AddCommand(ConfigureSensorCmd)

	cobra.OnInitialize(initGlobalFlags)
	rootCmd.Execute()

}

func CreateTimeseriesTable() error {
	iotConfig := iotedge.New(iotedge.GetConfig())
	db := timeseries.DBHandler(iotConfig.IoTConfig.DbConfig)
	if err := db.CreateTimeseriesTable(iotConfig.IoTConfig.TimeseriesTable); err != nil {
		log.Errorf("failed to create DB: %v", err)
		return err
	}
	return nil
}

func startServer() error {
	config := iotedge.GetConfig()
	iot := iotedge.New(config)
	go iotedge.StartMQTTBroker(config.MQTTPort, config)
	return iot.StartSensorServer(nil)
}
