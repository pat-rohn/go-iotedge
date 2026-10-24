package iotedge

import (
	"fmt"

	"github.com/pkg/errors"
	"log/slog"
	_ "modernc.org/sqlite"
)

func (e *IoTEdge) Init(deviceDesc DeviceDesc) (Device, error) {
	logFields := []any{"fnct", "Init", "Name", deviceDesc.Name, "Desc", deviceDesc.Description}
	slog.Info(fmt.Sprintf("Init %s.", deviceDesc.Name), logFields...)
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
				break
			}
		}
		if !hasSensor {
			sensor := Sensor{Name: s, DeviceID: dev.ID}
			if err := e.DeviceDB.InsertSensor(sensor); err != nil {
				return Device{}, fmt.Errorf("failed to insert sensor %s: %w", sensor.Name, err)
			}
		}
	}
	return dev, nil
}
