package iotedge

// web_system_test.go — W6 of doc/EDGE_SERVER_WORKBOOK.md: system page with
// version, database, MQTT, forwarding and configuration warnings.

import (
	"net/http"
	"strings"
	"testing"
)

func TestConfigWarnings(t *testing.T) {
	shipped := IoTConfig{Password: defaultConfigPassword, SessionSecret: defaultConfigSessionSecret,
		MQTTRedirectAddress: "http://central:3004"}
	text := strings.Join(warningTexts(configWarnings(shipped)), "\n")
	for _, want := range []string{"Password", "SessionSecret", "IngestAuth", "ProtectAdminRoutes", "MQTTRedirectAddress"} {
		if !strings.Contains(text, want) {
			t.Errorf("warnings for the shipped config lack %q:\n%s", want, text)
		}
	}

	hardened := IoTConfig{Password: testPassword, SessionSecret: "unit-test-session-secret-32-bytes!!",
		Security: SecurityConfig{RequireCredentials: true, ProtectAdminRoutes: true, IngestAuth: IngestAuthEnforce, IngestToken: "t"}}
	for _, w := range configWarnings(hardened) {
		if w.Level == "warning" {
			t.Errorf("hardened config still warns: %s", w.Text)
		}
	}
}

func warningTexts(ws []configWarning) []string {
	var out []string
	for _, w := range ws {
		out = append(out, w.Text)
	}
	return out
}

func TestWebSystemPage(t *testing.T) {
	_, router := securityRouter(t, SecurityConfig{})
	cookie := login(t, router)
	w := serve(router, http.MethodGet, URIDashboard+"/system", nil, cookie)
	if w.Code != http.StatusOK {
		t.Fatalf("GET system = %d", w.Code)
	}
	body := w.Body.String()
	for _, want := range []string{"</html>", shortCommit(), "Database", "MQTT broker", "not running", "Forwarding", "Off", "IngestAuth"} {
		if !strings.Contains(body, want) {
			t.Errorf("system page lacks %q", want)
		}
	}
	if w := serve(router, http.MethodGet, URIDashboard+"/system", nil); w.Code != http.StatusFound {
		t.Errorf("system page without login = %d, want redirect", w.Code)
	}
}

func TestWebSystemPageShowsBrokerClients(t *testing.T) {
	iot, router := securityRouter(t, SecurityConfig{})
	broker, err := iot.startMQTTBroker(freePort(t))
	if err != nil {
		t.Fatal(err)
	}
	defer broker.Close()
	client, err := connectClient(broker.port, "")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Disconnect(100)
	publish(t, client, "frick/sys/dev/tag/data", "1")
	cookie := login(t, router)
	body := serve(router, http.MethodGet, URIDashboard+"/system", nil, cookie).Body.String()
	if !strings.Contains(body, `id="mqtt-clients">1<`) {
		t.Errorf("system page does not show 1 connected MQTT client")
	}
	if !strings.Contains(body, `id="mqtt-received">1<`) {
		t.Errorf("system page does not show 1 received message")
	}
}

func shortCommit() string {
	c := CurrentVersion().Commit
	if len(c) > 12 {
		return c[:12]
	}
	return c
}
