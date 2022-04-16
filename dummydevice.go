package iotedge

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"time"

	"github.com/pat-rohn/timeseries"
	log "github.com/sirupsen/logrus"
)

type DummyDevice struct {
	Url        string
	DeviceDesc DeviceDesc
}

func (d *DummyDevice) Simulate() {
	type Request struct {
		Device DeviceDesc
	}
	req := Request{d.DeviceDesc}
	json_data, err := json.Marshal(req)
	if err != nil {
		log.Fatal(err)
	}

	log.Infof("%+v", req)

	resp, err := http.Post(d.Url+URIInitDevice, "application/json",
		bytes.NewBuffer(json_data))

	if err != nil {
		log.Fatal(err)
	}

	var res map[string]interface{}

	json.NewDecoder(resp.Body).Decode(&res)

	fmt.Println(res["json"])
	for {

		var data []timeseries.TimeseriesImportStruct

		val := timeseries.TimeseriesImportStruct{
			Tag: fmt.Sprintf("%s", d.DeviceDesc.Name),
		}
		for i := 0; i < 10; i++ {
			time.Sleep(time.Millisecond * 50)
			val.Timestamps = append(val.Timestamps, time.Now().Format("2006-01-02 15:04:05.000"))
			val.Values = append(val.Values, fmt.Sprintf("%f", 283.0+(rand.Float32()*15)))
			val.Comments = append(val.Values, "dummy")
		}
		data = append(data, val)

		jsonData, err := json.Marshal(data)
		if err != nil {
			log.Fatal(err)
		}

		resp, err := http.Post(d.Url+URISaveTimeseries, "application/json",
			bytes.NewBuffer(jsonData))

		if err != nil {
			log.Fatal(err)
		}

		var res map[string]interface{}

		json.NewDecoder(resp.Body).Decode(&res)

		fmt.Println(res["json"])
	}
}
