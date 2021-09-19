package main

import (
	"fmt"
	"os"

	iotedge "github.com/pat-rohn/go-iotedge"
	startup "github.com/pat-rohn/go-startup"
	"github.com/pat-rohn/timeseries"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
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

	var setAllDOCmd = &cobra.Command{
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

	rootCmd.PersistentFlags().StringVarP(&loglevel, "verbose", "v", "w", "verbosity")

	rootCmd.AddCommand(setAllDOCmd)
	cobra.OnInitialize(initGlobalFlags)
	rootCmd.Execute()

}

func startServer() error {

	viper.SetDefault("DbConfig.Name", "plottydb")
	viper.SetDefault("DbConfig.IPOrPath", "localhost")
	viper.SetDefault("DbConfig.UsePostgres", true)
	viper.SetDefault("DbConfig.User", "user")
	viper.SetDefault("DbConfig.Password", "password")
	viper.SetDefault("DbConfig.Port", 5432)
	viper.SetDefault("DbConfig.TableName", "measurements1")
	viper.SetDefault("Port", 3004)

	viper.SetConfigName("iot")
	viper.SetConfigType("json")
	dirname, err := os.UserHomeDir()
	if err != nil {
		log.Fatal(err)
	}
	pathToConfig := dirname + "/.iotserver"
	viper.AddConfigPath(pathToConfig)
	viper.AddConfigPath(".")
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			log.Warnf(fmt.Sprintf("no config file found: %v", err))
			if err := os.Mkdir(pathToConfig, 0755); err != nil {
				log.Fatal(fmt.Sprintf("Creating config folder failed: %v", err))
			}
			err = viper.SafeWriteConfig()
			if err != nil {
				log.Fatal(fmt.Sprintf("Storing default config failed: %v", err))
			}
		} else {
			log.Fatal(fmt.Sprintf("Loading config failed: %v", err))
		}
	}

	type config struct {
		Port     int
		DbConfig timeseries.DBConfig
	}

	var iotConfig config

	err = viper.Unmarshal(&iotConfig)
	if err != nil {
		panic(fmt.Errorf("fatal error config file: %w ", err))
	}
	fmt.Printf("%+v", iotConfig)
	iot := iotedge.New(iotConfig.DbConfig, iotConfig.Port)
	return iot.StartSensorServer()
}
