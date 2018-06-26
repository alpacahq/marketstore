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
	servers = "nats://nats1.polygon.io:30401, nats://nats2.polygon.io:30402, nats://nats3.polygon.io:30403"
	apiKey  string
	NY, _   = time.LoadLocation("America/New_York")
)

func SetAPIKey(key string) {
	apiKey = key
}

func SetBaseURL(url string) {
	baseURL = url
}

func SetNatsServers(serverList string) {
	servers = serverList
}

func GetAggregates(symbol string, from time.Time) (*GetAggregatesResponse, error) {
	resp := GetAggregatesResponse{}

	from = from.In(NY)
	to := from.Add(7 * 24 * time.Hour)

	retry := 0

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

		// Sometimes polygon returns empty data set even though the data
		// is there. Here we retry up to 5 times to ensure the data
		// is really empty. This does add overhead, but since it is only
		// called for the beginning backfill, it is worth it to not miss
		// any data. Usually the data is returned within 3 retries.
		if len(r.Ticks) == 0 {
			if retry <= 5 && from.Before(time.Now()) {
				retry++
				continue
			} else {
				retry = 0
				break
			}
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

// Stream from the polygon nats server
func Stream(handler func(m *nats.Msg), symbols []string) (err error) {
	nc, _ := nats.Connect(
		servers,
		nats.Token(apiKey))

	if symbols != nil && len(symbols) > 0 {
		for _, symbol := range symbols {
			if _, err = nc.Subscribe(
				fmt.Sprintf("AM.%s", symbol),
				handler); err != nil {
				return
			}
		}
	} else {
		_, err = nc.Subscribe("AM.*", handler)
	}

	return
}
