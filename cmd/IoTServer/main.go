package main

import (
	"fmt"

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

	var TestCmd = &cobra.Command{
		Use:   "test",
		Args:  cobra.MinimumNArgs(0),
		Short: "",
		Long:  ``,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := iotedge.Test(); err != nil {
				return err
			}
			return nil
		},
	}
	rootCmd.PersistentFlags().StringVarP(&loglevel, "verbose", "v", "w", "verbosity")

	rootCmd.AddCommand(startServerCmd)
	rootCmd.AddCommand(createTableCmd)
	rootCmd.AddCommand(TestCmd)

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
	if err := db.CreateTimeseriesTable(iotConfig.DatabaseConfig.TableName); err != nil {
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
