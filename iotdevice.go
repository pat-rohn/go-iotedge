package iotedge

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

func (e *IoTEdge) InitializeDB() error {
	logFields := log.Fields{"fnct": "InitializeDB", "name": e.DeviceDBConfig.Name}
	log.WithFields(logFields).Infoln("init")
	if e.DeviceDBConfig.UsePostgres {
		psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
			"password=%s dbname=%s sslmode=disable",
			e.DeviceDBConfig.IPOrPath,
			e.DeviceDBConfig.Port,
			e.DeviceDBConfig.User,
			e.DeviceDBConfig.Password,
			e.DeviceDBConfig.Name)
		log.WithFields(logFields).Tracef(
			"Open database: %v", psqlInfo)
		database, err := sql.Open("postgres", psqlInfo)
		if err != nil {
			log.WithFields(logFields).Errorf(
				"Failed to open db %v", err)
			return fmt.Errorf("failed to open db %v", err)
		}
		e.DB = database
	} else {
		if len(e.DeviceDBConfig.IPOrPath) > 0 {
			log.WithFields(logFields).Tracef("Create Folder: %v", e.DeviceDBConfig.IPOrPath)
			if _, err := os.Stat(e.DeviceDBConfig.IPOrPath); err != nil {
				if os.IsNotExist(err) {
					err := os.MkdirAll(e.DeviceDBConfig.IPOrPath, 0644)
					if err != nil {
						log.WithFields(logFields).Errorf("Failed to create path %v", err)
					}
				}
			}
		}

		database, err := sql.Open("sqlite", e.DeviceDBConfig.IPOrPath+e.DeviceDBConfig.Name)
		if err != nil {
			log.WithFields(logFields).Errorf("Failed to open db %v", err)
			return fmt.Errorf("failed to open db %v", err)
		}
		e.DB = database
	}
	log.WithFields(logFields).Infof("Opened database with name %s ",
		e.DeviceDBConfig.Name)
	err := e.DB.Ping()
	if err != nil {
		panic(err.Error())
	}
	return e.createTables()
}

func (e *IoTEdge) Init(deviceDesc DeviceDesc) (Device, error) {
	logFields := log.Fields{"fnct": "Init", "Name": deviceDesc.Name, "Desc": deviceDesc.Description}
	log.WithFields(logFields).Infof("Init %s.", deviceDesc.Name)
	dev, err := e.GetOrCreateDevice(deviceDesc)
	if err != nil {
		return Device{}, errors.Wrap(err, "Creating device failed")
	}
	sensorsOnDB, err := e.GetSensors(dev.ID)
	if err != nil {
		return Device{}, fmt.Errorf("failed to get sensors: %v", err)
	}

	for _, s := range deviceDesc.Sensors {
		hasSensor := false
		for _, sensorOld := range sensorsOnDB {
			if s == sensorOld.Name {
				hasSensor = true
				log.WithFields(logFields).Infof("Has sensor %s", s)
				break
			}
		}
		if !hasSensor {
			log.WithFields(logFields).Infof("Unknown sensor: %s", s)
			sensor := Sensor{
				Name:     s,
				DeviceID: dev.ID,
			}
			if err := e.insertSensor(sensor); err != nil {
				log.Errorf("Failed to insert sensor %s: %s", sensor.Name, err)
			}
		}
	}
	return dev, nil

}

func (e *IoTEdge) GetSensors(deviceID int) ([]Sensor, error) {
	logFields := log.Fields{"fnct": "GetSensors"}
	log.WithFields(logFields).Infof("%d", deviceID)
	rows, err := e.DB.Query("SELECT id, deviceid, name, offset FROM sensors WHERE deviceid = ?", deviceID)
	if err != nil {
		return []Sensor{}, err
	}
	defer rows.Close()
	var sensors []Sensor
	for rows.Next() {
		var sensor Sensor
		if err := rows.Scan(&sensor.ID, &sensor.DeviceID, &sensor.Name, &sensor.Offset); err != nil {
			return sensors, err
		}
		sensors = append(sensors, sensor)
	}
	if err = rows.Err(); err != nil {
		return sensors, err
	}
	return sensors, nil

}

func (e *IoTEdge) Configure(dev Device) error {
	logFields := log.Fields{"fnct": "Configure", "device": dev.Name}
	log.WithFields(logFields).Infof("Configure device '%s' with interval/buffer: %v/%v ",
		dev.Name, dev.Interval, dev.Buffer)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Millisecond)
	defer cancel()
	tx, err := e.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		log.WithFields(logFields).Error(err)
		return err
	}
	sqlRes, execErr := tx.ExecContext(ctx, "UPDATE devices SET description = ? , buffer = ? , intervall = ? WHERE id = ?", dev.Description, dev.Buffer, dev.Interval, dev.ID)
	if execErr != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			log.WithFields(logFields).Errorf("update failed: %v, unable to rollback: %v\n", execErr, rollbackErr)
			return err
		}
		log.WithFields(logFields).Errorf("update failed: %v", execErr)
	}
	rowsAffected, err := sqlRes.RowsAffected()
	log.WithFields(logFields).Infof("Rows affected %d", rowsAffected)
	if rowsAffected != 1 {
		log.WithFields(logFields).Errorf("Rows affected  not 1 (%d)", rowsAffected)
		return fmt.Errorf("rows affected  not 1 (%d)", rowsAffected)
	}
	if err != nil {
		log.WithFields(logFields).Errorf("exec failed: %v)", err)
		return err
	}
	if err := tx.Commit(); err != nil {
		log.WithFields(logFields).Error(err)
		return err
	}
	log.WithFields(logFields).Infof("Succefully updated device %v", dev.Name)
	return nil
}

func (e *IoTEdge) ConfigureSensor(sensor Sensor) error {
	logFields := log.Fields{"fnct": "ConfigureSensor"}
	log.WithFields(logFields).Infof("Configure sensor %s with offset: %v ",
		sensor.Name, sensor.Offset)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Millisecond)
	defer cancel()
	tx, err := e.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		log.WithFields(logFields).Error(err)
		return err
	}
	_, execErr := tx.ExecContext(ctx, "UPDATE sensors SET name = ? , offset = ?  WHERE deviceid = ?", sensor.Name, sensor.Offset, sensor.DeviceID)
	if execErr != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			log.WithFields(logFields).Errorf("update failed: %v, unable to rollback: %v\n", execErr, rollbackErr)
			return err
		}
		log.WithFields(logFields).Errorf("update failed: %v", execErr)
	}
	if err := tx.Commit(); err != nil {
		log.WithFields(logFields).Error(err)
		return err
	}
	log.WithFields(logFields).Infof("Succefully updated sensor %s", sensor.Name)
	return nil
}
