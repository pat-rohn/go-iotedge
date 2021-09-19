package main

import (
	"fmt"
	"os"

	startup "github.com/pat-rohn/go-startup"
	timeseries "github.com/pat-rohn/timeseries"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"picloud.ch/schusti/pkg/iotedge"
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

	viper.SetDefault("db.Name", "plottydb")
	viper.SetDefault("db.IPOrPath", "localhost")
	viper.SetDefault("db.UsePostgres", true)
	viper.SetDefault("db.User", "user")
	viper.SetDefault("db.Password", "password")
	viper.SetDefault("db.Port", 5432)
	viper.SetDefault("db.TableName", "measurements1")
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
	if err = viper.ReadInConfig(); err != nil {
		log.Warnf(fmt.Sprintf("no config file found: %w", err))

		if err := os.Mkdir(pathToConfig, 0755); err != nil {
			log.Fatal(fmt.Sprintf("Creating config folder failed: %w", err))
		}
		err = viper.SafeWriteConfig()
		if err != nil {
			log.Fatal(fmt.Sprintf("Storing default config failed: %w", err))
		}
	}

	type config struct {
		Port int
		db   timeseries.DBConfig
	}

	var iotConfig config

	err = viper.Unmarshal(&iotConfig)
	if err != nil {
		panic(fmt.Errorf("fatal error config file: %w ", err))
	}
	iot := iotedge.New(iotConfig.db, iotConfig.Port)
	return iot.StartSensorServer()
}
