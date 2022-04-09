package iotedge

import (
	"fmt"

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

func createDefaultDevice(deviceDesc DeviceDesc) (Device, error) {
	logFields := log.Fields{"fnct": "saveDefaultDeviceConfig"}
	log.WithFields(logFields).Infoln("Test")
	db, err := gorm.Open("sqlite3", DeviceDatabaseName)
	if err != nil {
		return Device{}, err
	}
	defer db.Close()

	var sensorSettings []Sensor
	for _, sensor := range deviceDesc.Sensors {
		sensorSetting := Sensor{
			Name:   sensor,
			Offset: 0.0,
		}
		sensorSettings = append(sensorSettings, sensorSetting)
	}
	dev := Device{
		Name:     deviceDesc.Name,
		Sensors:  sensorSettings,
		Interval: 60.0,
		Buffer:   3,
	}

	result := db.Create(&dev)
	if err = result.Error; err != nil || result.RowsAffected <= 0 {
		log.WithFields(logFields).Errorf("Failed to save device %v", err)
		return Device{}, err
	}

	return dev, err
}

func getDevice(deviceName string) (Device, error) {
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
	res := db.First(&oldDev, "Name= ?", name)
	if res.Error != nil {
		log.WithFields(logFields).Error(res.Error)
		return err
	}
	oldDev.Sensors = sensors
	res = db.Save(&oldDev)
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

func ConfigureDevice(name string, interval float32, buffer int) error {
	logFields := log.Fields{"fnct": "ConfigureDevice"}
	log.WithFields(logFields).Infof("Configure device %s with interval/buffer: %v/%v ",
		name, interval, buffer)
	db, err := gorm.Open("sqlite3", DeviceDatabaseName)
	if err != nil {
		log.WithFields(logFields).Error(err)
		return err
	}
	defer db.Close()
	var devToUpdate Device
	res := db.First(&devToUpdate, "Name= ?", name)
	if res.Error != nil {
		log.WithFields(logFields).Error(res.Error)
		return err
	}
	devToUpdate.Interval = interval
	devToUpdate.Buffer = buffer
	res = db.Save(&devToUpdate)
	if res.Error != nil {
		log.WithFields(logFields).Error(res.Error)
		return res.Error
	}
	if res.RowsAffected <= 0 {
		log.WithFields(logFields).Error("not updated")
		return fmt.Errorf("not updated")
	}

	log.WithFields(logFields).Infof("Succefully updated device %v", name)
	return nil
}

func ConfigureSensor(name string, offset float32) error {
	logFields := log.Fields{"fnct": "ConfigureSensor"}
	log.WithFields(logFields).Infof("Configure sensor %s with offset: %v ", name, offset)
	db, err := gorm.Open("sqlite3", DeviceDatabaseName)
	if err != nil {
		log.WithFields(logFields).Error(err)
		return err
	}
	defer db.Close()
	var sensorToUpdate Sensor
	res := db.First(&sensorToUpdate, "Name= ?", name)
	if res.Error != nil {

		log.WithFields(logFields).Errorf("Failed to get sensor %s: %v", name, res.Error)
		return res.Error
	}
	sensorToUpdate.Offset = offset
	res = db.Save(&sensorToUpdate)
	if res.Error != nil {
		log.WithFields(logFields).Error(res.Error)
		return res.Error
	}
	if res.RowsAffected <= 0 {
		log.WithFields(logFields).Error("not updated")
		return fmt.Errorf("not updated")
	}
	log.WithFields(logFields).Infof("Succefully updated sensor %v", name)
	return nil
}
