package iotedge

import (
	"fmt"

	"github.com/pat-rohn/timeseries"
	log "github.com/sirupsen/logrus"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type IoTDevice struct {
	DatabaseConfig timeseries.DBConfig
	Device         Device
}

func InitializeDB() error {
	logFields := log.Fields{"fnct": "InitializeDB"}
	log.WithFields(logFields).Infoln("GORM init")
	db, err := getORMConn(GetConfig().DatabaseConfig)
	if err != nil {
		return err
	}

	err = db.AutoMigrate(&Sensor{})
	if err != nil {
		return err
	}
	err = db.AutoMigrate(&Device{})
	if err != nil {
		return err
	}
	return nil
}

func Init(deviceDesc DeviceDesc) (IoTDevice, error) {
	logFields := log.Fields{"fnct": "Init"}
	log.WithFields(logFields).Infof("Init %s.", deviceDesc.Name)
	if hasDevice(deviceDesc.Name) {
		// todo: update
		log.WithFields(logFields).Infoln("Device exists already.")
		return GetDevice(deviceDesc.Name)
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
		DatabaseConfig: GetConfig().DatabaseConfig,
	}

	db, err := getORMConn(dev.DatabaseConfig)
	if err != nil {
		log.WithFields(logFields).Error(err)
		return IoTDevice{}, err
	}

	result := db.Create(&dev.Device)
	if err = result.Error; err != nil || result.RowsAffected <= 0 {
		log.WithFields(logFields).Errorf("Failed to save device %v", err)
		return IoTDevice{}, err
	}

	return dev, err
}

func GetDevice(deviceName string) (IoTDevice, error) {
	logFields := log.Fields{"fnct": "GetDevice"}
	log.WithFields(logFields).Infof("%s", deviceName)
	config := GetConfig()
	db, err := getORMConn(config.DatabaseConfig)
	if err != nil {
		log.WithFields(logFields).Error(err)
		return IoTDevice{}, err
	}

	var dev Device
	result := db.Preload("Sensors").First(&dev, "Name = ?", deviceName)
	if err = result.Error; err != nil {
		log.WithFields(logFields).Error(err)
		return IoTDevice{}, err
	}
	if result.RowsAffected > 0 {
		log.WithFields(logFields).Infof("Found device %s", dev.Name)
		return IoTDevice{
			Device:         dev,
			DatabaseConfig: config.DatabaseConfig,
		}, nil
	}

	log.WithFields(logFields).Errorf("No device found %+v", dev)
	return IoTDevice{}, err
}

func (d *IoTDevice) UpdateSensors(sensors []Sensor) error {
	logFields := log.Fields{"fnct": "UpdateSensors"}
	log.WithFields(logFields).Infoln("Update")
	db, err := getORMConn(d.DatabaseConfig)
	if err != nil {
		return err
	}

	var oldDev Device
	res := db.First(&oldDev, "Name= ?", d.Device.Name)
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

func (d *IoTDevice) Configure(interval float32, buffer int) error {
	logFields := log.Fields{"fnct": "ConfigureDevice"}
	log.WithFields(logFields).Infof("Configure device %s with interval/buffer: %v/%v ",
		d.Device.Name, interval, buffer)
	db, err := getORMConn(d.DatabaseConfig)
	if err != nil {
		log.WithFields(logFields).Error(err)
		return err
	}
	var devToUpdate Device
	res := db.First(&devToUpdate, "Name= ?", d.Device.Name)
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

	log.WithFields(logFields).Infof("Succefully updated device %v", d.Device.Name)
	return nil
}

func (d *IoTDevice) ConfigureSensor(offset float32) error {
	logFields := log.Fields{"fnct": "ConfigureSensor"}
	log.WithFields(logFields).Infof("Configure sensor %s with offset: %v ",
		d.Device.Name, offset)

	db, err := getORMConn(d.DatabaseConfig)
	if err != nil {
		log.WithFields(logFields).Error(err)
		return err
	}
	var sensorToUpdate Sensor
	res := db.First(&sensorToUpdate, "Name= ?", d.Device.Name)
	if res.Error != nil {

		log.WithFields(logFields).Errorf("Failed to get sensor %s: %v", d.Device.Name, res.Error)
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
	log.WithFields(logFields).Infof("Succefully updated sensor %v", d.Device.Name)
	return nil
}

func hasDevice(name string) bool {
	logFields := log.Fields{"fnct": "hasDevice"}
	log.WithFields(logFields).Infof("Check for %s", name)
	config := GetConfig()
	db, err := getORMConn(config.DatabaseConfig)
	if err != nil {
		return false
	}

	var dev Device
	res := db.First(&dev, "Name = ?", name)
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
func getORMConn(conf timeseries.DBConfig) (db *gorm.DB, err error) {

	if conf.UsePostgres {
		dsn := fmt.Sprintf("host=localhost user=%s password=%s dbname=%s port=%v sslmode=disable TimeZone=Europe/Zurich",
			conf.User, conf.Password, conf.Name, conf.Port,
		)

		//dsn := "host=localhost user=gorm password=gorm dbname=gorm port=9920 sslmode=disable TimeZone=Asia/Shanghai"
		db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
		if err != nil {
			fmt.Println(err.Error())
			return nil, err
		}
		return db, nil

	}

	database, err := gorm.Open(sqlite.Open(DeviceDatabaseName), &gorm.Config{})
	if err != nil {
		fmt.Println(err.Error())
		return nil, err
	}

	return database, nil

}
