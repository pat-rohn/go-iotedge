package iotedge

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
)

func (e *IoTEdge) createTables() error {
	logFields := log.Fields{"fnct": "CreateTables", "name": e.DeviceDBConfig.Name}
	log.WithFields(logFields).Infoln("init")
	idStr := "id integer primary key autoincrement"
	if e.DeviceDBConfig.UsePostgres {
		idStr = "id  			SERIAL PRIMARY KEY UNIQUE"
	}
	sqlStr := `CREATE TABLE IF NOT EXISTS devices (
			` + idStr + ` ,
			name        TEXT NOT NULL UNIQUE,
			description TEXT DEFAULT '',
			intervall	NUMBER DEFAULT 60,
			buffer 		INTEGER DEFAULT 2
		   );
		 `
	if err := e.executeQuery(sqlStr); err != nil {
		log.WithFields(logFields).Errorf("failed to create devices table:%v", err)
		return err
	}
	sqlStr = `CREATE TABLE IF NOT EXISTS sensors (
		` + idStr + ` ,
		deviceid        SERIAL NOT NULL,
		name			TEXT NOT NULL,
		description 	TEXT DEFAULT '',
		offset			NUMBER DEFAULT 0
	   );
	 `
	if err := e.executeQuery(sqlStr); err != nil {
		log.WithFields(logFields).Errorf("failed to create sensors table:%v", err)
		return err
	}
	log.WithFields(logFields).Infoln("success")
	return nil
}

func (e *IoTEdge) GetOrCreateDevice(descr DeviceDesc) (Device, error) {
	logFields := log.Fields{"fnct": "GetOrCreateDevice", "device": descr.Name}
	log.WithFields(logFields).Infoln("Look for device")
	deviceRows, err := e.DB.Query("SELECT * FROM devices WHERE name = ?", descr.Name)
	if err != nil {
		return Device{}, err
	}
	defer deviceRows.Close()
	var dev Device
	hasDevice := deviceRows.Next() // is unique
	if hasDevice {
		log.WithFields(logFields).Infoln("Device already initialized")
		if err := deviceRows.Scan(&dev.ID, &dev.Name, &dev.Description, &dev.Buffer, &dev.Interval); err != nil {
			return dev, err
		}
		if err = deviceRows.Err(); err != nil {
			log.WithFields(logFields).Errorf("Scan failed failed: %v", err)
			return dev, err
		}
		log.WithFields(logFields).Infof("Device has ID %d", dev.ID)
		return dev, nil
	}
	log.WithFields(logFields).Infof("Create new device %v", descr.Name)
	dev.Description = descr.Description
	dev.Name = descr.Name
	if err := e.insertDevice(dev); err != nil {
		log.WithFields(logFields).Errorf("Insert device failed: %v", err)
		return Device{}, err
	}
	rows, err := e.DB.Query("SELECT * FROM devices WHERE name = ?", descr.Name)
	if err != nil {
		log.WithFields(logFields).Errorf("Reading device after inserting failed: %v", err)
		return Device{}, err
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
	return dev, nil
}

func (e *IoTEdge) GetDevice(name string) (Device, error) {
	logFields := log.Fields{"fnct": "GetDevice"}
	log.WithFields(logFields).Infof("%s", name)
	rows, err := e.DB.Query("SELECT * FROM devices WHERE name = ?", name)
	if err != nil {
		return Device{}, err
	}
	defer rows.Close()
	var dev Device
	hasDevice := rows.Next()
	if !hasDevice {
		return Device{}, fmt.Errorf("device %s not found", name)
	}
	for rows.Next() {
		if err := rows.Scan(&dev.ID, &dev.Name, &dev.Description, &dev.Buffer, &dev.Interval); err != nil {
			return dev, err
		}
	}
	if err = rows.Err(); err != nil {
		return dev, err
	}

	return dev, nil
}

func (e *IoTEdge) executeQuery(sqlStr string) error {
	logFields := log.Fields{"fnct": "executeQuery", "name": e.DeviceDBConfig.Name}

	if len(sqlStr) > 2000 {
		log.WithFields(logFields).Tracef(
			"start from query: %s\n", sqlStr[0:500])
		log.WithFields(logFields).Tracef(
			"end from query: %v\n", sqlStr[len(sqlStr)-500:])
	} else {
		log.WithFields(logFields).Tracef(
			"full query: %s\n", sqlStr)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Millisecond)
	defer cancel()
	tx, err := e.DB.Begin()
	if err != nil {
		log.WithFields(logFields).Errorf("%v", err)
		return err
	}
	stmt, err := tx.Prepare(sqlStr)
	if err != nil {
		log.WithFields(logFields).Errorf("Failed to prepare: %v", err)
		return fmt.Errorf("failed to prepare: %v", err)
	}

	_, err = stmt.ExecContext(ctx)
	if err != nil {
		log.WithFields(logFields).Errorf("Failed to execute: %v", err)
		return err
	}

	err = tx.Commit()
	if err != nil {
		log.WithFields(logFields).Errorf("Failed to commit: %v", err)
		return err
	}

	return nil
}

func (e *IoTEdge) insertDevice(device Device) error {
	logFields := log.Fields{"fnct": "insertDevice", "device": device.Name}
	log.WithFields(logFields).Infof("%s", device.Name)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Millisecond)
	defer cancel()
	tx, err := e.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		log.WithFields(logFields).Error(err)
		return err
	}
	_, execErr := tx.ExecContext(ctx, "INSERT INTO devices (name) VALUES (?)", device.Name)
	if execErr != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			log.WithFields(logFields).Errorf("insert failed: %v, unable to rollback: %v\n", execErr, rollbackErr)
			return err
		}
		log.WithFields(logFields).Errorf("insert failed: %v", execErr)
	}
	if err := tx.Commit(); err != nil {
		log.WithFields(logFields).Error(err)
		return err
	}
	return nil
}

func (e *IoTEdge) insertSensor(sensor Sensor) error {
	logFields := log.Fields{"fnct": "insertSensor", "sensor": sensor.Name}
	log.WithFields(logFields).Infof("%s", sensor.Name)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Millisecond)
	defer cancel()
	tx, err := e.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		log.WithFields(logFields).Error(err)
		return err
	}
	_, execErr := tx.ExecContext(ctx, "INSERT INTO sensors (name,deviceid) VALUES (?,?)", sensor.Name, sensor.DeviceID)
	if execErr != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			log.WithFields(logFields).Errorf("insert failed: %v, unable to rollback: %v\n", execErr, rollbackErr)
			return err
		}
		log.WithFields(logFields).Errorf("insert failed: %v", execErr)
	}
	if err := tx.Commit(); err != nil {
		log.WithFields(logFields).Error(err)
		return err
	}
	return nil
}
