package iotedge

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/pat-rohn/timeseries"
	log "github.com/sirupsen/logrus"
)

// ErrDeviceNotFound is returned by GetDevice when no device matches the given name.
var ErrDeviceNotFound = errors.New("device not found")

type DeviceDB struct {
	*timeseries.DbHandler
	conf timeseries.DBConfig
}

// deviceDBMu guards deviceDB, dbhandler, and deviceDBReady so that the
// handler can be recovered automatically if the underlying connection is
// closed (e.g. by a test or a graceful shutdown).
var deviceDBMu sync.Mutex
var deviceDB *DeviceDB
var dbhandler *timeseries.DbHandler
var deviceDBReady bool // true after tables have been created at least once

func CreateDbHandler(config timeseries.DBConfig) *timeseries.DbHandler {
	dbhandler = timeseries.DBHandler(config)
	return dbhandler
}

// isDBAlive returns false when dbh is nil or its underlying connection is
// already closed.  It uses Ping() which is cheap (no round-trip for SQLite).
func isDBAlive(dbh *timeseries.DbHandler) bool {
	if dbh == nil {
		return false
	}
	return dbh.DB.Ping() == nil
}

func GetDeviceDB(config timeseries.DBConfig) *DeviceDB {
	logger := log.WithFields(log.Fields{"fnct": "InitializeDB", "name": config.Name})
	deviceDBMu.Lock()
	defer deviceDBMu.Unlock()

	// If the current handler is stale (closed), reset state so we re-open.
	if !isDBAlive(dbhandler) {
		if dbhandler != nil {
			logger.Warnf("DBHandler connection is stale, reinitializing")
		}
		// Obtain a fresh connection from the timeseries singleton.  If it was
		// previously closed, the singleton will have been reset to nil, so
		// DBHandler() will open a new connection.
		dbhandler = timeseries.DBHandler(config)
		if deviceDB != nil {
			deviceDB.DbHandler = dbhandler
		}
	} else if deviceDB != nil {
		logger.Tracef("Reusing existing DBHandler for deviceDB")
	}

	if !deviceDBReady {
		logger.Infoln("init")
		if deviceDB == nil {
			deviceDB = &DeviceDB{conf: config}
		}
		deviceDB.DbHandler = dbhandler
		if err := deviceDB.migrate(); err != nil {
			logger.Fatalf("failed to migrate database schema: %v", err)
		}
		deviceDBReady = true
	}
	if !compareConfigs(deviceDB.conf, config) {
		logger.Fatalf("Config must not change %+v to %+v", deviceDB.conf, config)
	}
	return deviceDB
}

// rebind rewrites '?' placeholders to '$1..$n' when the configured backend is
// Postgres — lib/pq does not accept '?'.
func (devDB *DeviceDB) rebind(sqlStr string) string {
	if !devDB.conf.UsePostgres {
		return sqlStr
	}
	var b strings.Builder
	n := 0
	for _, r := range sqlStr {
		if r == '?' {
			n++
			fmt.Fprintf(&b, "$%d", n)
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
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

// RecordDeviceAddress stores the address a device request came from.
func (devDB *DeviceDB) RecordDeviceAddress(name, address string, now time.Time) error {
	_, err := devDB.DB.Exec(devDB.rebind(`UPDATE devices SET address = ?, address_seen_at = ? WHERE name = ?`),
		address, now.UTC(), name)
	return err
}

func (devDB *DeviceDB) GetOrCreateDevice(descr DeviceDesc) (Device, error) {
	logFields := log.Fields{"fnct": "GetOrCreateDevice", "device": descr.Name}
	log.WithFields(logFields).Debugln("GetOrCreateDevice")
	startTime := time.Now()

	var insertSQL string
	if devDB.conf.UsePostgres {
		insertSQL = "INSERT INTO devices (name, description) VALUES ($1, $2) ON CONFLICT (name) DO NOTHING"
	} else {
		insertSQL = "INSERT OR IGNORE INTO devices (name, description) VALUES (?, ?)"
	}
	if err := devDB.Execute(insertSQL, descr.Name, descr.Description); err != nil {
		log.WithFields(logFields).Errorf("Upsert device failed: %v", err)
		return Device{}, err
	}
	// Device startup reports the current IP address as its description.
	// Do not overwrite it for payloads that omit a description.
	if descr.Description != "" {
		updateSQL := devDB.rebind("UPDATE devices SET description = ? WHERE name = ?")
		if err := devDB.Execute(updateSQL, descr.Description, descr.Name); err != nil {
			log.WithFields(logFields).Warnf("failed to update description: %v", err)
		}
	}

	rows, err := devDB.ExecuteQuery(devDB.rebind("SELECT id, name, description, intervall, buffer FROM devices WHERE name = ?"), descr.Name)
	if err != nil {
		return Device{}, err
	}
	defer rows.Close()
	var dev Device
	if rows.Next() {
		if err := rows.Scan(&dev.ID, &dev.Name, &dev.Description, &dev.Interval, &dev.Buffer); err != nil {
			return Device{}, fmt.Errorf("failed to scan device: %w", err)
		}
	}
	if err = rows.Err(); err != nil {
		return Device{}, err
	}
	log.WithFields(logFields).Debugf("Device has ID %d (took %v)", dev.ID, time.Since(startTime))
	return dev, nil
}

func (devDB *DeviceDB) GetDevice(name string) (Device, error) {
	logFields := log.Fields{"fnct": "GetDevice", "name": name}
	log.WithFields(logFields).Infof("Find device with name %v", name)
	rows, err := devDB.ExecuteQuery(devDB.rebind("SELECT id, name, description, intervall, buffer FROM devices WHERE name = ?"), name)
	if err != nil {
		return Device{}, err
	}
	defer rows.Close()
	var dev Device
	found := false
	for rows.Next() {
		found = true
		if err := rows.Scan(&dev.ID, &dev.Name, &dev.Description, &dev.Interval, &dev.Buffer); err != nil {
			return Device{}, fmt.Errorf("failed to scan device: %w", err)
		}
		log.WithFields(logFields).Infof("Device found %+v", dev)
	}
	if err = rows.Err(); err != nil {
		return Device{}, err
	}
	if !found {
		log.WithFields(logFields).Infof("Device '%s' not found", name)
		return Device{}, ErrDeviceNotFound
	}
	return dev, nil
}

func (devDB *DeviceDB) GetDevices() ([]Device, error) {
	logFields := log.Fields{"fnct": "GetDevices"}
	rows, err := devDB.ExecuteQuery("SELECT id, name, description, intervall, buffer FROM devices")
	if err != nil {
		return []Device{}, err
	}
	defer rows.Close()
	var devices []Device
	for rows.Next() {
		var dev Device
		err := rows.Scan(&dev.ID, &dev.Name, &dev.Description, &dev.Interval, &dev.Buffer)
		if err != nil {
			log.WithFields(logFields).Errorf("Failed to scan device %v", err)
			continue
		}
		devices = append(devices, dev)
	}
	if err = rows.Err(); err != nil {
		return []Device{}, err
	}
	return devices, err
}

func (devDB *DeviceDB) GetDevicesConfigs() ([]DeviceConfig, error) {
	logFields := log.Fields{"fnct": "GetDevicesConfigs"}
	log.WithFields(logFields).Infoln("Find all devices with sensors via JOIN")
	rows, err := devDB.ExecuteQuery(`
		SELECT d.id, d.name, d.description, d.intervall, d.buffer,
		       d.address, d.address_seen_at,
		       s.id, s.deviceid, s.name, s.sensor_offset
		FROM devices d
		LEFT JOIN sensors s ON s.deviceid = d.id
		ORDER BY d.id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	configMap := make(map[int]*DeviceConfig)
	var order []int

	for rows.Next() {
		var (
			dID       int
			dName     string
			dDesc     string
			dInterval float32
			dBuffer   int
			dAddress  string
			dSeenAt   *time.Time
			sID       *int
			sDeviceID *int
			sName     *string
			sOffset   *float32
		)
		if err := rows.Scan(&dID, &dName, &dDesc, &dInterval, &dBuffer, &dAddress, &dSeenAt,
			&sID, &sDeviceID, &sName, &sOffset); err != nil {
			log.WithFields(logFields).Errorf("Failed to scan row: %v", err)
			return nil, err
		}
		if _, exists := configMap[dID]; !exists {
			configMap[dID] = &DeviceConfig{
				ID: dID, Name: dName, Description: dDesc,
				Interval: dInterval, Buffer: dBuffer,
				Address: dAddress, AddressSeenAt: dSeenAt,
			}
			order = append(order, dID)
		}
		if sID != nil {
			configMap[dID].Sensors = append(configMap[dID].Sensors, Sensor{
				ID: *sID, DeviceID: *sDeviceID, Name: *sName, SensorOffset: *sOffset,
			})
		}
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	result := make([]DeviceConfig, 0, len(order))
	for _, id := range order {
		result = append(result, *configMap[id])
	}
	return result, nil
}

func (devDB *DeviceDB) GetSensors(deviceID int) ([]Sensor, error) {
	var sensors []Sensor
	rows, err := devDB.ExecuteQuery(devDB.rebind("SELECT id, deviceid, name, sensor_offset FROM sensors WHERE deviceid = ?"), deviceID)
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
	err := devDB.Execute(devDB.rebind("UPDATE devices SET description = ? , buffer = ? , intervall = ? WHERE id = ?"),
		dev.Description, dev.Buffer, dev.Interval, dev.ID)
	if err != nil {
		return err
	}
	return nil
}

func (devDB *DeviceDB) ConfigureSensor(sensor Sensor) error {
	err := devDB.Execute(devDB.rebind("UPDATE sensors SET sensor_offset = ? WHERE deviceid = ? AND name = ?"),
		sensor.SensorOffset, sensor.DeviceID, sensor.Name)
	if err != nil {
		return err
	}
	return nil
}

func (devDB *DeviceDB) InsertSensor(sensor Sensor) error {
	err := devDB.Execute(devDB.rebind("INSERT INTO sensors (name,deviceid) VALUES (?,?)"), sensor.Name, sensor.DeviceID)
	if err != nil {
		return err
	}
	return err
}
