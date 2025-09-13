package iotedge

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/pat-rohn/timeseries"
	log "github.com/sirupsen/logrus"
)

type DeviceDB struct {
	db        *sql.DB
	semaphore chan struct{} // limit number of concurrent operations
	mutex     sync.Mutex
	timeout   time.Duration
}

var deviceDB *DeviceDB
var once sync.Once

func GetDeviceDB(config timeseries.DBConfig) *DeviceDB {
	logFields := log.Fields{"fnct": "InitializeDB", "name": config.Name}
	log.WithFields(logFields).Infoln("init")
	once.Do(func() {
		deviceDB = &DeviceDB{
			semaphore: make(chan struct{}, 10),
			mutex:     sync.Mutex{},
		}
		log.WithFields(logFields).Infoln("init")
		if config.UsePostgres {
			psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
				"password=%s dbname=%s sslmode=disable",
				config.IPOrPath,
				config.Port,
				config.User,
				config.Password,
				config.Name)
			log.WithFields(logFields).Tracef(
				"Open database: %v", psqlInfo)
			database, err := sql.Open("postgres", psqlInfo)
			if err != nil {
				log.WithFields(logFields).Fatalf(
					"Failed to open db %v", err)

			}
			deviceDB.db = database
		} else {
			if len(config.IPOrPath) > 0 {
				log.WithFields(logFields).Tracef("Create Folder: %v", config.IPOrPath)
				if _, err := os.Stat(config.IPOrPath); err != nil {
					if os.IsNotExist(err) {
						err := os.MkdirAll(config.IPOrPath, 0644)
						if err != nil {
							log.WithFields(logFields).Errorf("Failed to create path %v", err)
						}
					}
				}
			}

			database, err := sql.Open("sqlite", config.IPOrPath+config.Name)
			if err != nil {
				log.WithFields(logFields).Fatalf("Failed to open db %v", err)
			}
			deviceDB.db = database
		}
		log.WithFields(logFields).Infof("Opened database with name %s ",
			config.Name)
		err := deviceDB.db.Ping()
		if err != nil {
			log.Fatal(err)
		}
		idStr := "id integer primary key autoincrement"
		if config.UsePostgres {
			idStr = "id  			SERIAL PRIMARY KEY UNIQUE"
		}
		sqlStr := `CREATE TABLE IF NOT EXISTS devices (
			` + idStr + ` ,
			name        TEXT NOT NULL UNIQUE,
			description TEDeviceDBConfigXT DEFAULT '',
			intervall	NUMBER DEFAULT 60,
			buffer 		INTEGER DEFAULT 2
		   );
		 `
		if err := deviceDB.executeQuery(sqlStr); err != nil {
			log.WithFields(logFields).Fatalf("failed to create devices table:%v", err)
		}
		sqlStr = `CREATE TABLE IF NOT EXISTS sensors (
		` + idStr + ` ,
		deviceid        SERIAL NOT NULL,
		name			TEXT NOT NULL,
		description 	TEXT DEFAULT '',
		offset			NUMBER DEFAULT 0
	   );
	 `
		if err := deviceDB.executeQuery(sqlStr); err != nil {
			log.WithFields(logFields).Fatalf("failed to create sensors table:%v", err)
		}
	})

	return deviceDB
}

func (devDB *DeviceDB) GetOrCreateDevice(descr DeviceDesc) (Device, error) {
	logFields := log.Fields{"fnct": "GetOrCreateDevice", "device": descr.Name}
	log.WithFields(logFields).Infoln("Look for device")
	startTime := time.Now()
	var dev Device
	err := devDB.execute(func() error {
		deviceRows, err := devDB.db.Query("SELECT * FROM devices WHERE name = ?", descr.Name)
		if err != nil {
			return err
		}
		defer deviceRows.Close()
		var dev Device
		hasDevice := deviceRows.Next() // is unique
		if hasDevice {
			log.WithFields(logFields).Infoln("Device already initialized")
			if err := deviceRows.Scan(&dev.ID, &dev.Name, &dev.Description, &dev.Buffer, &dev.Interval); err != nil {
				return err
			}
			if err = deviceRows.Err(); err != nil {
				log.WithFields(logFields).Errorf("Scan failed failed: %v", err)
				return err
			}
			log.WithFields(logFields).Infof("Device has ID %d", dev.ID)
			return nil
		}
		log.WithFields(logFields).Infof("Create new device %v", descr.Name)
		dev.Description = descr.Description
		dev.Name = descr.Name
		if err := devDB.insertDevice(dev); err != nil {
			log.WithFields(logFields).Errorf("Insert device failed: %v", err)
			return err
		}
		rows, err := devDB.db.Query("SELECT * FROM devices WHERE name = ?", descr.Name)
		if err != nil {
			log.WithFields(logFields).Errorf("Reading device after inserting failed: %v", err)
			return err
		}
		for rows.Next() {
			if err := rows.Scan(&dev.ID, &dev.Name, &dev.Description, &dev.Buffer, &dev.Interval); err != nil {
				log.WithFields(logFields).Errorf("Scan failed: %v", err)
				return err
			}
		}
		if err = rows.Err(); err != nil {
			return err
		}
		return nil
	})

	log.WithFields(logFields).Infof("Device has ID %d", dev.ID)
	log.WithFields(logFields).Warnf("GetOrCreateDevice took %v", time.Since(startTime))
	return dev, err
}

func (devDB *DeviceDB) GetDevice(name string) (Device, error) {
	logFields := log.Fields{"fnct": "GetDevice", "name": name}
	log.WithFields(logFields).Infof("Find device with name %v", name)

	var dev Device
	err := devDB.execute(func() error {
		rows, err := devDB.db.Query("SELECT * FROM devices WHERE name = ?", name)
		if err != nil {
			return err
		}
		defer rows.Close()

		var dev Device
		for rows.Next() {
			err := rows.Scan(&dev.ID, &dev.Name, &dev.Description, &dev.Buffer, &dev.Interval)
			if err != nil {
				log.WithFields(logFields).Errorf("Failed to scan device %v", err)
				return fmt.Errorf("failed to scan device %v", err)
			}
			log.WithFields(logFields).Infof("Device found %+v", dev)
		}
		if err = rows.Err(); err != nil {
			return err
		}
		return fmt.Errorf("device not found")
	})

	log.WithFields(logFields).Errorf("Device '%s' not found", name)
	return dev, err
}

func (devDB *DeviceDB) GetSensors(deviceID int) ([]Sensor, error) {
	logFields := log.Fields{"fnct": "GetSensors"}
	log.WithFields(logFields).Infof("%d", deviceID)
	var sensors []Sensor
	err := devDB.execute(func() error {
		rows, err := devDB.db.Query("SELECT id, deviceid, name, offset FROM sensors WHERE deviceid = ?", deviceID)
		if err != nil {
			return err
		}
		defer rows.Close()
		var sensors []Sensor
		for rows.Next() {
			var sensor Sensor
			if err := rows.Scan(&sensor.ID, &sensor.DeviceID, &sensor.Name, &sensor.Offset); err != nil {
				return err
			}
			sensors = append(sensors, sensor)
		}
		if err = rows.Err(); err != nil {
			return err
		}
		return nil
	})

	return sensors, err
}

func (devDB *DeviceDB) Configure(dev Device) error {
	logFields := log.Fields{"fnct": "Configure", "device": dev.Name}
	log.WithFields(logFields).Infof("Configure device '%s' with interval/buffer: %v/%v ",
		dev.Name, dev.Interval, dev.Buffer)
	err := devDB.execute(func() error {
		_, err := devDB.db.Query("UPDATE devices SET description = ? , buffer = ? , intervall = ? WHERE id = ?", dev.Description, dev.Buffer, dev.Interval, dev.ID)
		if err != nil {
			log.WithFields(logFields).Errorf("exec failed: %v)", err)
			return err
		}
		log.WithFields(logFields).Infof("Succefully updated device %v", dev.Name)
		return nil
	})
	return err
}

func (devDB *DeviceDB) ConfigureSensor(sensor Sensor) error {
	logFields := log.Fields{"fnct": "ConfigureSensor"}
	log.WithFields(logFields).Infof("Configure sensor %s with offset: %v ",
		sensor.Name, sensor.Offset)
	err := devDB.execute(func() error {
		_, err := devDB.db.Query("UPDATE sensors SET name = ? , offset = ?  WHERE deviceid = ?",
			sensor.Name, sensor.Offset, sensor.DeviceID)
		if err != nil {
			log.WithFields(logFields).Errorf("exec failed: %v", err)
			return err
		}
		log.WithFields(logFields).Infof("Succefully updated sensor %s", sensor.Name)
		return nil
	})
	return err
}

func (devDB *DeviceDB) executeQuery(sqlStr string) error {
	logFields := log.Fields{"fnct": "executeQuery"}
	log.WithFields(logFields).Infof("Execute query")

	if len(sqlStr) > 2000 {
		log.WithFields(logFields).Tracef(
			"start from query: %s\n", sqlStr[0:500])
		log.WithFields(logFields).Tracef(
			"end from query: %v\n", sqlStr[len(sqlStr)-500:])
	} else {
		log.WithFields(logFields).Tracef(
			"full query: %s\n", sqlStr)
	}

	_, err := devDB.db.Query(sqlStr)
	if err != nil {
		log.WithFields(logFields).Error(err)
		return err
	}
	log.WithFields(logFields).Infof("Query executed")

	return nil
}

func (devDB *DeviceDB) insertDevice(device Device) error {
	logFields := log.Fields{"fnct": "insertDevice", "device": device.Name}
	log.WithFields(logFields).Infof("%s", device.Name)

	_, err := devDB.db.Query("INSERT INTO devices (name) VALUES (?)", device.Name)
	if err != nil {
		log.WithFields(logFields).Error(err)
		return err
	}
	return nil
}

func (devDB *DeviceDB) InsertSensor(sensor Sensor) error {
	logFields := log.Fields{"fnct": "insertSensor", "sensor": sensor.Name}
	log.WithFields(logFields).Infof("%s", sensor.Name)
	err := devDB.execute(func() error {
		_, err := devDB.db.Query("INSERT INTO sensors (name,deviceid) VALUES (?,?)", sensor.Name, sensor.DeviceID)
		if err != nil {
			log.WithFields(logFields).Error(err)
			return err
		}
		return nil
	})
	return err
}

func (db *DeviceDB) execute(operation func() error) error {
	select {
	case db.semaphore <- struct{}{}:
		defer func() { <-db.semaphore }()
		db.mutex.Lock()
		defer db.mutex.Unlock()
		return operation()
	case <-time.After(db.timeout):
		return errors.New("operation timed out waiting for semaphore")
	}
}
