package main

import (
	"fmt"
	"strconv"

	iotedge "github.com/pat-rohn/go-iotedge"
	startup "github.com/pat-rohn/go-startup"
	"github.com/pat-rohn/timeseries"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

const (
	logPkg string = "main"
)

var loglevel string

func initGlobalFlags() {
	startup.SetLogLevel(fmt.Sprintf("-%s", loglevel), "iotserver.log")
}

func main() {
	fmt.Println("IoT-Server")
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
			iotDevice, err := iotedge.GetDevice(args[0])
			if err != nil {
				return err
			}
			if err = iotDevice.Configure(float32(interval), int(buffer)); err != nil {
				return err
			}
			return nil
		},
	}

	var ConfigureSensorCmd = &cobra.Command{
		Use:   "conf-sensor sensorname offset",
		Args:  cobra.MinimumNArgs(2),
		Short: "",
		Long:  `e.g IoTServer conf-sensor -v i Basel3Humidity -- -2 `,
		RunE: func(cmd *cobra.Command, args []string) error {
			offset, err := strconv.ParseFloat(args[1], 32)
			if err != nil {
				return err
			}
			iotDevice, err := iotedge.GetDevice(args[0])
			if err != nil {
				return err
			}
			if err = iotDevice.ConfigureSensor(float32(offset)); err != nil {
				return err
			}
			return nil
		},
	}
	rootCmd.PersistentFlags().StringVarP(&loglevel, "verbose", "v", "w", "verbosity")

	rootCmd.AddCommand(startServerCmd)
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
	iotConfig := iotedge.GetConfig()
	iot := iotedge.New(iotConfig.DatabaseConfig, iotConfig.Port)
	return iot.StartSensorServer()
}
