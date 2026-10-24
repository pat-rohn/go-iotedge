package homeassistant

import (
	"encoding/json"
	"strconv"
	"time"

	"github.com/pat-rohn/go-iotedge/internal/store"
)

// Message is a retained MQTT publication for Home Assistant.
type Message struct {
	Topic   string
	Payload string
}

func haDevice(device string) map[string]any {
	return map[string]any{
		"identifiers":  []string{"enlightened:" + device},
		"name":         device,
		"manufacturer": "esp-enlightened",
	}
}

// Discovery returns the discovery configs for the entities that
// only the edge server can compute, sharing the device the firmware announces.
func Discovery(device string) []Message {
	base := "iotedge/" + device
	common := func(name, metric string) map[string]any {
		return map[string]any{
			"name":                  name,
			"unique_id":             "enlightened_" + device + "_" + metric,
			"state_topic":           base + "/state/" + metric,
			"entity_category":       "diagnostic",
			"availability_topic":    base + "/availability",
			"payload_available":     "online",
			"payload_not_available": "offline",
			"device":                haDevice(device),
		}
	}
	lastSeen := common("Last seen", "last_seen")
	lastSeen["device_class"] = "timestamp"
	restarts := common("Restart count", "restarts")
	restarts["state_class"] = "measurement"
	msgs := []Message{}
	for _, e := range []struct {
		metric string
		cfg    map[string]any
	}{{"last_seen", lastSeen}, {"restarts", restarts}} {
		payload, _ := json.Marshal(e.cfg)
		msgs = append(msgs, Message{
			Topic:   "homeassistant/sensor/" + device + "_" + e.metric + "/config",
			Payload: string(payload),
		})
	}
	return msgs
}

// State returns the retained states for one device.
func State(summary store.DeviceMonitoringSummary) []Message {
	msgs := []Message{}
	if summary.LastSeen != nil {
		msgs = append(msgs, Message{
			Topic:   "iotedge/" + summary.Device + "/state/last_seen",
			Payload: summary.LastSeen.UTC().Format(time.RFC3339),
		})
	}
	msgs = append(msgs, Message{
		Topic:   "iotedge/" + summary.Device + "/state/restarts",
		Payload: strconv.Itoa(summary.Restarts),
	})
	return msgs
}
