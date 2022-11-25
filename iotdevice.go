package iotedge

import (
	"fmt"

	"github.com/pat-rohn/timeseries"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type IoTDevice struct {
	DatabaseConfig timeseries.DBConfig
	Device         Device
}

func (e *IoTEdge) InitializeDB() error {
	logFields := log.Fields{"fnct": "InitializeDB"}
	log.WithFields(logFields).Infoln("GORM init")
	if e.DatabaseConfig.UsePostgres {
		dsn := fmt.Sprintf("host=localhost user=%s password=%s dbname=%s port=%v sslmode=disable TimeZone=Europe/Zurich",
			e.DatabaseConfig.User, e.DatabaseConfig.Password, e.DatabaseConfig.Name, e.DatabaseConfig.Port,
		)
		var err error
		e.GormDB, err = gorm.Open(postgres.Open(dsn), &gorm.Config{})
		if err != nil {
			fmt.Println(err.Error())
			return errors.Wrap(err, "open failed")
		}
		//dsn := "host=localhost user=gorm password=gorm dbname=gorm port=9920 sslmode=disable TimeZone=Asia/Shanghai"

	} else {

		var err error
		e.GormDB, err = gorm.Open(sqlite.Open("devices.db"), &gorm.Config{})
		if err != nil {
			fmt.Println(err.Error())
			return errors.Wrap(err, "open failed")
		}
	}

	err := e.GormDB.AutoMigrate(&Sensor{})
	if err != nil {
		return errors.Wrap(err, "AutoMigrate Sensor failed")
	}
	err = e.GormDB.AutoMigrate(&Device{})
	if err != nil {
		return errors.Wrap(err, "AutoMigrate Device failed")
	}
	return nil
}

func (e *IoTEdge) Init(deviceDesc DeviceDesc) (IoTDevice, error) {
	logFields := log.Fields{"fnct": "Init"}
	log.WithFields(logFields).Infof("Init %s.", deviceDesc.Name)
	if e.hasDevice(deviceDesc.Name) {

		log.WithFields(logFields).Infoln("Device exists already.")
		hasUpdate := false
		dev, err := e.GetDevice(deviceDesc.Name)
		if err != nil {
			return IoTDevice{}, errors.Wrap(err, "AutoMigrate Device failed")
		}
		if dev.Device.Description != deviceDesc.Description {
			log.WithFields(logFields).Infof("New Description: %s", deviceDesc.Description)
			dev.Device.Description = deviceDesc.Description
			hasUpdate = true

		}
		for _, s := range deviceDesc.Sensors {
			if !dev.hasSensor(s, e.GormDB) {
				hasUpdate = true
				log.WithFields(logFields).Infof("New sensor found: %s", s)
				dev.Device.Sensors = append(dev.Device.Sensors, Sensor{
					Name:   s,
					Offset: 0.0,
				})
			}
		}
		if hasUpdate {
			result := e.GormDB.Save(&dev.Device)
			if err = result.Error; err != nil || result.RowsAffected <= 0 {
				log.WithFields(logFields).Error(errors.Wrap(err, "Failed to save device"))
				return IoTDevice{}, errors.Wrap(err, "Failed to save device")
			}
		}
		return dev, nil
	}

	var sensors []Sensor
	for _, sensor := range deviceDesc.Sensors {
		sensor := Sensor{
			Name:   sensor,
			Offset: 0.0,
		}
		sensors = append(sensors, sensor)
	}

	dev := IoTDevice{
		Device: Device{
			Name:        deviceDesc.Name,
			Sensors:     sensors,
			Interval:    60.0,
			Buffer:      3,
			Description: deviceDesc.Description,
		},
		DatabaseConfig: e.DatabaseConfig,
	}

	_ = e.GormDB.Create(&dev.Device)

	return dev, nil
}

func (e *IoTEdge) GetDevice(deviceName string) (IoTDevice, error) {
	logFields := log.Fields{"fnct": "GetDevice"}
	log.WithFields(logFields).Infof("%s", deviceName)

	var dev Device
	result := e.GormDB.Preload("Sensors").First(&dev, "Name = ?", deviceName)

	if result.RowsAffected > 0 {
		log.WithFields(logFields).Infof("Found device %s", dev.Name)
		return IoTDevice{
			Device:         dev,
			DatabaseConfig: e.DatabaseConfig,
		}, nil
	}

	log.WithFields(logFields).Errorf("No device found %+v", dev)
	return IoTDevice{}, nil
}

func (d *IoTDevice) Configure(interval float32, buffer int, gormDB *gorm.DB) error {
	logFields := log.Fields{"fnct": "ConfigureDevice"}
	log.WithFields(logFields).Infof("Configure device %s with interval/buffer: %v/%v ",
		d.Device.Name, interval, buffer)

	var devToUpdate Device
	//res := gormDB.First(&devToUpdate, "Name= ?", d.Device.Name)

	devToUpdate.Interval = interval
	devToUpdate.Buffer = buffer
	res := gormDB.Save(&devToUpdate)
	if res.Error != nil {
		log.WithFields(logFields).Error(res.Error)
		return res.Error
	}
	if res.RowsAffected <= 0 {
		log.WithFields(logFields).Error("not updated")
		return fmt.Errorf("not updated")
	}

	log.WithFields(logFields).Infof("Succefully updated device %v", d.Device.Name)
	return nil
}

func (d *IoTDevice) ConfigureSensor(offset float32, sensorName string, gormDB *gorm.DB) error {
	logFields := log.Fields{"fnct": "ConfigureSensor"}
	log.WithFields(logFields).Infof("Configure sensor %s with offset: %v ",
		d.Device.Name, offset)

	var sensorToUpdate Sensor
	res := gormDB.First(&sensorToUpdate, "Name= ?", sensorName)
	if res.Error != nil {

		log.WithFields(logFields).Errorf("Failed to get sensor %s: %v", sensorName, res.Error)
		return res.Error
	}
	sensorToUpdate.Offset = offset
	res = gormDB.Save(&sensorToUpdate)
	if res.Error != nil {
		log.WithFields(logFields).Error(res.Error)
		return res.Error
	}
	if res.RowsAffected <= 0 {
		log.WithFields(logFields).Error("not updated")
		return fmt.Errorf("not updated")
	}
	log.WithFields(logFields).Infof("Succefully updated sensor %v", d.Device.Name)
	return nil
}

func (e *IoTEdge) hasDevice(name string) bool {
	logFields := log.Fields{"fnct": "hasDevice"}
	log.WithFields(logFields).Infof("Check for %s", name)
	var exists bool
	var dev Device
	err := e.GormDB.Model(dev).Select("count(*) > 0").
		Where("Name = ?", name).
		Find(&exists).
		Error
	if err != nil {
		log.WithFields(logFields).Warnf("Failed to check if record exist: %v", err)
		return false
	}
	if exists {

		log.WithFields(logFields).Warnf("Has found sensor: %v", name)
		return true
	}

	log.WithFields(logFields).Warnf("Sensor %v does not exist", name)
	return false
}

func (d *IoTDevice) hasSensor(name string, gormDB *gorm.DB) bool {
	logFields := log.Fields{"fnct": "hasSensor"}
	log.WithFields(logFields).Infof("Check for %s", name)
	var sensor Sensor
	var exists bool
	err := gormDB.Model(sensor).Select("count(*) > 0").
		Where("Name = ?", name).
		Find(&exists).
		Error
	if err != nil {
		log.WithFields(logFields).Infof("failed to find sensor: %v", err)
		return false
	}
	if exists {
		log.WithFields(logFields).Infof("Has sensor with name %s", name)
		return true
	}
	log.WithFields(logFields).Infof("No sensor with name %s", name)
	return false
}
