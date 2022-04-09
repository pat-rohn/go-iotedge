package iotedge

import (
	"fmt"
	"time"

	"github.com/jinzhu/gorm"
	log "github.com/sirupsen/logrus"
)

func initialMigration() error {
	logFields := log.Fields{"fnct": "initialMigration"}
	log.WithFields(logFields).Infoln("Test")
	db, err := gorm.Open("sqlite3", DeviceDatabaseName)
	if err != nil {
		fmt.Println(err.Error())
		return err
	}
	defer db.Close()

	// Migrate the schema
	result := db.AutoMigrate(&Sensor{})
	if err = result.Error; err != nil {
		return err
	}
	result = db.AutoMigrate(&Device{})
	if err = result.Error; err != nil {
		return err
	}
	return nil
}

func saveDefaultDeviceConfig(deviceDesc DeviceDesc) error {
	logFields := log.Fields{"fnct": "saveDefaultDeviceConfig"}
	log.WithFields(logFields).Infoln("Test")
	db, err := gorm.Open("sqlite3", DeviceDatabaseName)
	if err != nil {
		return err
	}
	defer db.Close()

	var sensorSettings []Sensor
	for _, sensor := range deviceDesc.Sensors {
		sensorSetting := Sensor{
			SensorName:         sensor,
			Offset:             0.0,
			AquisitionInterval: time.Duration(time.Second * 60),
		}
		sensorSettings = append(sensorSettings, sensorSetting)
	}
	dev := Device{
		Name:    deviceDesc.Name,
		Sensors: sensorSettings,
	}

	result := db.Create(&dev)
	if err = result.Error; err != nil || result.RowsAffected <= 0 {
		log.WithFields(logFields).Errorf("Failed to save device %v", err)
		return err
	}

	return nil
}

func getDeviceConfig(deviceName string) (Device, error) {
	logFields := log.Fields{"fnct": "getDeviceConfig"}
	log.WithFields(logFields).Infoln("Test")
	db, err := gorm.Open("sqlite3", DeviceDatabaseName)
	if err != nil {
		log.WithFields(logFields).Error(err)
		return Device{}, err
	}
	defer db.Close()
	var dev Device
	result := db.Preload("Sensors").First(&dev, "Name = ?", deviceName)
	if err = result.Error; err != nil {
		log.WithFields(logFields).Error(err)
		return Device{}, err
	}
	if result.RowsAffected > 0 {
		log.WithFields(logFields).Infof("Found device %s", dev.Name)
		return dev, nil
	}

	log.WithFields(logFields).Error("No device Found device %+v", dev)
	return Device{}, err
}

func hasDevice(deviceName string) bool {
	logFields := log.Fields{"fnct": "hasDevice"}
	log.WithFields(logFields).Infof("Check for %s", deviceName)
	db, err := gorm.Open("sqlite3", DeviceDatabaseName)
	if err != nil {
		return false
	}
	defer db.Close()
	var dev Device
	res := db.First(&dev, "Name = ?", deviceName)
	if res.Error != nil {
		log.WithFields(logFields).Infof("failed to find device: %v", res.Error)
		return false
	}
	if res.RowsAffected > 0 {
		log.WithFields(logFields).Infof("Found device %s", dev.Name)
		return true
	}
	log.WithFields(logFields).Infof("No device with name %s", dev.Name)
	return false
}

func UpdateSensors(name string, sensors []Sensor) error {
	logFields := log.Fields{"fnct": "UpdateDevice"}
	log.WithFields(logFields).Infoln("Test")
	db, err := gorm.Open("sqlite3", DeviceDatabaseName)
	if err != nil {
		return err
	}
	defer db.Close()
	var oldDev Device
	db.First(&oldDev, "Name= ?", name)
	oldDev.Sensors = sensors
	res := db.Save(&oldDev)
	if res.Error != nil {
		log.WithFields(logFields).Error(res.Error)
		return res.Error
	}
	if res.RowsAffected <= 0 {
		log.WithFields(logFields).Error("not updated")
		return fmt.Errorf("not updated")
	}
	return nil
}

func Test() error {
	initialMigration()

	for i := 0; i < 10; i++ {
		devDesc := DeviceDesc{
			Name:    fmt.Sprintf("BaselTest%d", i),
			Sensors: []string{"dht", "mhz", "bme"},
		}
		if !hasDevice(devDesc.Name) {
			saveDefaultDeviceConfig(devDesc)
		}
	}

	/*saveDefaultDeviceConfig(DeviceDesc{
		Name:    "BaselTest1",
		Sensors: []string{"dht", "mhz", "bme"},
	})*/

	dev, err := getDeviceConfig("BaselTest0")
	if err != nil {
		return err
	}

	var sensors []Sensor
	for _, sensor := range dev.Sensors {
		s := sensor
		if sensor.SensorName == "bme" {
			s.Offset = -2.0
			s.AquisitionInterval = time.Second * 100
			fmt.Printf("Change Offset of %+v\n", sensor.SensorName)
		}
		sensors = append(sensors, s)
	}

	UpdateSensors("BaselTest0", sensors)

	return nil
}
