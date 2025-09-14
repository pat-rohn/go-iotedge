package iotedge

import (
	"fmt"
	"sync"
	"time"

	"github.com/pat-rohn/timeseries"
	log "github.com/sirupsen/logrus"
)

type DeviceDB struct {
	*timeseries.DbHandler
	conf timeseries.DBConfig
}

var onceDeviceDB sync.Once
var deviceDB *DeviceDB
var dbhandler *timeseries.DbHandler

func CreateDbHandler(config timeseries.DBConfig) *timeseries.DbHandler {

	dbhandler = timeseries.DBHandler(config)
	return dbhandler
}

func GetDeviceDB(config timeseries.DBConfig) *DeviceDB {
	logger := log.WithFields(log.Fields{"fnct": "InitializeDB", "name": config.Name})
	if dbhandler == nil {
		dbhandler = CreateDbHandler(config)
	} else {
		logger.Infof("Reusing existing DBHandler for deviceDB")
	}
	onceDeviceDB.Do(func() {
		logger.Infoln("init")
		deviceDB = &DeviceDB{conf: config}
		deviceDB.DbHandler = dbhandler
		var idStr, numericType string
		if config.UsePostgres {
			// Define type strings based on database type
			idStr = "id SERIAL PRIMARY KEY"
			numericType = "NUMERIC"
		} else {
			idStr = "id INTEGER PRIMARY KEY AUTOINCREMENT"
			numericType = "NUMBER"
		}

		sqlStr := `CREATE TABLE IF NOT EXISTS devices (
			` + idStr + ` ,
			name        TEXT NOT NULL UNIQUE,
			description TEXT DEFAULT '',
			intervall	 ` + numericType + ` DEFAULT 60,
			buffer 		INTEGER DEFAULT 2
		   );
		 `
		if _, err := deviceDB.ExecuteQuery(sqlStr); err != nil {
			logger.Fatalf("failed to create devices table:%v", err)
		}
		sqlStr = `CREATE TABLE IF NOT EXISTS sensors (
		` + idStr + ` ,
		deviceid        INTEGER NOT NULL,
		name			TEXT NOT NULL,
		description 	TEXT DEFAULT '',
		sensor_offset			` + numericType + ` DEFAULT 0
	   );
	 `
		if _, err := deviceDB.ExecuteQuery(sqlStr); err != nil {
			logger.Fatalf("failed to create sensors table:%v", err)
		}
	})
	if !compareConfigs(deviceDB.conf, config) {
		logger.Fatalf("Config must not change %+v to %+v", deviceDB.conf, config)
		deviceDB.Close()
		deviceDB = nil
	}
	return deviceDB
}

func compareConfigs(oldConf, newConf timeseries.DBConfig) bool {
	if oldConf.Name != newConf.Name {
		return false
	}
	if oldConf.IPOrPath != newConf.IPOrPath {
		return false
	}
	if oldConf.User != newConf.User {
		return false
	}
	if oldConf.Password != newConf.Password {
		return false
	}
	if oldConf.Port != newConf.Port {
		return false
	}
	if oldConf.UsePostgres != newConf.UsePostgres {
		return false
	}

	return true
}

func (devDB *DeviceDB) GetOrCreateDevice(descr DeviceDesc) (Device, error) {
	logFields := log.Fields{"fnct": "GetOrCreateDevice", "device": descr.Name}
	log.WithFields(logFields).Infoln("Look for device")
	startTime := time.Now()
	deviceRows, err := devDB.ExecuteQuery("SELECT * FROM devices WHERE name = ?", descr.Name)
	if err != nil {
		return Device{}, err
	}
	defer deviceRows.Close()
	var dev Device
	hasDevice := deviceRows.Next() // is unique
	if hasDevice {
		log.WithFields(logFields).Infoln("Device already initialized")
		if err := deviceRows.Scan(&dev.ID, &dev.Name, &dev.Description, &dev.Buffer, &dev.Interval); err != nil {
			return Device{}, err
		}
		if err = deviceRows.Err(); err != nil {
			log.WithFields(logFields).Errorf("Scan failed failed: %v", err)
			return Device{}, err
		}
		log.WithFields(logFields).Infof("Device has ID %d", dev.ID)
		return dev, nil
	}
	log.WithFields(logFields).Infof("Create new device %v", descr.Name)
	dev.Description = descr.Description
	dev.Name = descr.Name
	if err := devDB.insertDevice(dev); err != nil {
		log.WithFields(logFields).Errorf("Insert device failed: %v", err)
		return dev, err
	}
	rows, err := devDB.ExecuteQuery("SELECT * FROM devices WHERE name = ?", descr.Name)
	if err != nil {
		log.WithFields(logFields).Errorf("Reading device after inserting failed: %v", err)
		return dev, err
	}
	for rows.Next() {
		if err := rows.Scan(&dev.ID, &dev.Name, &dev.Description, &dev.Buffer, &dev.Interval); err != nil {
			log.WithFields(logFields).Errorf("Scan failed: %v", err)
			return dev, err
		}
	}
	if err = rows.Err(); err != nil {
		return dev, err
	}

	log.WithFields(logFields).Infof("Device has ID %d", dev.ID)
	log.WithFields(logFields).Warnf("GetOrCreateDevice took %v", time.Since(startTime))
	return dev, nil
}

func (devDB *DeviceDB) GetDevice(name string) (Device, error) {
	logFields := log.Fields{"fnct": "GetDevice", "name": name}
	log.WithFields(logFields).Infof("Find device with name %v", name)

	rows, err := devDB.ExecuteQuery("SELECT * FROM devices WHERE name = ?", name)
	if err != nil {
		return Device{}, err
	}
	defer rows.Close()

	var dev Device
	for rows.Next() {
		err := rows.Scan(&dev.ID, &dev.Name, &dev.Description, &dev.Buffer, &dev.Interval)
		if err != nil {
			log.WithFields(logFields).Errorf("Failed to scan device %v", err)
			return Device{}, fmt.Errorf("failed to scan device %v", err)
		}
		log.WithFields(logFields).Infof("Device found %+v", dev)
	}
	if err = rows.Err(); err != nil {
		return Device{}, err
	}
	log.WithFields(logFields).Errorf("Device '%s' not found", name)
	return dev, err
}

func (devDB *DeviceDB) GetSensors(deviceID int) ([]Sensor, error) {
	logFields := log.Fields{"fnct": "GetSensors"}
	log.WithFields(logFields).Infof("%d", deviceID)
	var sensors []Sensor
	rows, err := devDB.ExecuteQuery("SELECT id, deviceid, name, sensor_offset FROM sensors WHERE deviceid = ?", deviceID)
	if err != nil {
		return sensors, err
	}
	defer rows.Close()
	for rows.Next() {
		var sensor Sensor
		if err := rows.Scan(&sensor.ID, &sensor.DeviceID, &sensor.Name, &sensor.SensorOffset); err != nil {
			return sensors, err
		}
		sensors = append(sensors, sensor)
	}
	if err = rows.Err(); err != nil {
		return sensors, err
	}

	return sensors, err
}

func (devDB *DeviceDB) Configure(dev Device) error {
	logFields := log.Fields{"fnct": "Configure", "device": dev.Name}
	log.WithFields(logFields).Infof("Configure device '%s' with interval/buffer: %v/%v ",
		dev.Name, dev.Interval, dev.Buffer)
	_, err := devDB.ExecuteQuery("UPDATE devices SET description = ? , buffer = ? , intervall = ? WHERE id = ?", dev.Description, dev.Buffer, dev.Interval, dev.ID)
	if err != nil {
		log.WithFields(logFields).Errorf("exec failed: %v)", err)
		return err
	}
	log.WithFields(logFields).Infof("Succefully updated device %v", dev.Name)
	return nil
}

func (devDB *DeviceDB) ConfigureSensor(sensor Sensor) error {
	logFields := log.Fields{"fnct": "ConfigureSensor"}
	log.WithFields(logFields).Infof("Configure sensor %s with offset: %v ",
		sensor.Name, sensor.SensorOffset)
	_, err := devDB.ExecuteQuery("UPDATE sensors SET name = ? , sensor_offset = ?  WHERE deviceid = ?",
		sensor.Name, sensor.SensorOffset, sensor.DeviceID)
	if err != nil {
		log.WithFields(logFields).Errorf("exec failed: %v", err)
		return err
	}
	log.WithFields(logFields).Infof("Succefully updated sensor %s", sensor.Name)
	return nil

}

func (devDB *DeviceDB) insertDevice(device Device) error {
	logFields := log.Fields{"fnct": "insertDevice", "device": device.Name}
	log.WithFields(logFields).Infof("%s", device.Name)

	_, err := devDB.ExecuteQuery("INSERT INTO devices (name) VALUES (?)", device.Name)
	if err != nil {
		log.WithFields(logFields).Error(err)
		return err
	}
	return nil
}

func (devDB *DeviceDB) InsertSensor(sensor Sensor) error {
	logFields := log.Fields{"fnct": "insertSensor", "sensor": sensor.Name}
	log.WithFields(logFields).Infof("%s", sensor.Name)
	_, err := devDB.ExecuteQuery("INSERT INTO sensors (name,deviceid) VALUES (?,?)", sensor.Name, sensor.DeviceID)
	if err != nil {
		log.WithFields(logFields).Error(err)
		return err
	}
	return err
}
