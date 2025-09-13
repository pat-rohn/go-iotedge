package iotedge

import (
	"fmt"

	_ "modernc.org/sqlite"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

func (e *IoTEdge) Init(deviceDesc DeviceDesc) (Device, error) {
	logFields := log.Fields{"fnct": "Init", "Name": deviceDesc.Name, "Desc": deviceDesc.Description}
	log.WithFields(logFields).Infof("Init %s.", deviceDesc.Name)
	dev, err := e.DeviceDB.GetOrCreateDevice(deviceDesc)
	if err != nil {
		return Device{}, errors.Wrap(err, "Creating device failed")
	}
	sensorsOnDB, err := e.DeviceDB.GetSensors(dev.ID)
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
			if err := e.DeviceDB.InsertSensor(sensor); err != nil {
				log.Errorf("Failed to insert sensor %s: %s", sensor.Name, err)
			}
		}
	}
	return dev, nil

}
