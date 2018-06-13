package api

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	nats "github.com/nats-io/go-nats"
)

type GetAggregatesResponse struct {
	Symbol  string `json:"symbol"`
	AggType string `json:"aggType"`
	Map     struct {
		O string `json:"o"`
		C string `json:"c"`
		H string `json:"h"`
		L string `json:"l"`
		V string `json:"v"`
		D string `json:"d"`
	} `json:"map"`
	Ticks []struct {
		Open        float64 `json:"o"`
		Close       float64 `json:"c"`
		High        float64 `json:"h"`
		Low         float64 `json:"l"`
		Volume      int     `json:"v"`
		EpochMillis int64   `json:"d"`
	} `json:"ticks"`
}

var (
	baseURL = "https://api.polygon.io"
	apiKey  string
	NY, _   = time.LoadLocation("America/New_York")
)

func SetAPIKey(key string) {
	apiKey = key
}

func SetBaseURL(url string) {
	baseURL = url
}

func GetAggregates(symbol string, from time.Time) (*GetAggregatesResponse, error) {
	resp := GetAggregatesResponse{}

	from = from.In(NY)
	to := from.Add(7 * 24 * time.Hour)

	for {
		url := fmt.Sprintf("%s/v1/historic/agg/%s/%s?apiKey=%s&from=%s&to=%s",
			baseURL, "minute", symbol,
			apiKey,
			from.Format("2006-01-02"),
			to.Format("2006-01-02"))

		res, err := http.Get(url)

		if err != nil {
			return nil, err
		}

		if res.StatusCode >= http.StatusMultipleChoices {
			return nil, fmt.Errorf("status code %v", res.StatusCode)
		}

		r := &GetAggregatesResponse{}

		body, err := ioutil.ReadAll(res.Body)

		if err != nil {
			return nil, err
		}

		err = json.Unmarshal(body, r)

		if err != nil {
			return nil, err
		}

		if len(r.Ticks) == 0 {
			break
		}

		resp.Ticks = append(resp.Ticks, r.Ticks...)

		from = to.Add(24 * time.Hour)
		to = from.Add(24 * 7 * time.Hour)
	}

	return &resp, nil
}

type StreamAggregate struct {
	Symbol      string  `json:"sym"`
	Open        float64 `json:"o"`
	High        float64 `json:"h"`
	Low         float64 `json:"l"`
	Close       float64 `json:"c"`
	Volume      int     `json:"v"`
	EpochMillis int64   `json:"s"`

	// unneeded
	X int     `json:"-"`
	A float64 `json:"-"`
	T float64 `json:"-"`
	E int64   `json:"-"`
}

func Stream(handler func(m *nats.Msg)) error {
	servers := "nats://nats1.polygon.io:30401, nats://nats2.polygon.io:30402, nats://nats3.polygon.io:30403"

	nc, _ := nats.Connect(
		servers,
		nats.Token(apiKey))

	_, err := nc.Subscribe("AM.*", handler)

	return err
}
