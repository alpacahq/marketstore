package api

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"time"

	"github.com/golang/glog"
)

//TradeBucketedResponse json from bitMEX bucketed trade API
type TradeBucketedResponse struct {
	Timestamp       string  `json:"timestamp"`
	Symbol          string  `json:"symbol"`
	Open            float64 `json:"open"`
	High            float64 `json:"high"`
	Low             float64 `json:"low"`
	Close           float64 `json:"close"`
	Trades          float64 `json:"trades"`
	Volume          float64 `json:"volume"`
	Vwap            float64 `json:"vwap"`
	LastSize        float64 `json:"lastSize"`
	Turnover        float64 `json:"turnover"`
	HomeNotional    float64 `json:"homeNotional"`
	ForeignNotional float64 `json:"foreignNotional"`
}

var (
	client = &http.Client{
		Timeout: time.Second * 10,
	}
	baseURL = "https://www.bitmex.com"
)

// GetBuckets from bitmex Trade API
func GetBuckets(symbol string, from time.Time) ([]TradeBucketedResponse, error) {
	resp := []TradeBucketedResponse{}

	values := url.Values{
		"symbol":    []string{symbol},
		"binSize":   []string{"1m"},
		"partial":   []string{"false"},
		"count":     []string{"500"},
		"reverse":   []string{"false"},
		"startTime": []string{from.String()},
	}
	uri, _ := url.Parse(baseURL + "/api/v1/trade/bucketed")
	uri.RawQuery = values.Encode()
	reqURL := uri.String()
	log.Println(reqURL)
	req, err := http.NewRequest(http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")

	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("status code %v", res.StatusCode)
	}
	err = json.NewDecoder(res.Body).Decode(&resp)
	if err != nil {
		return nil, err
	}
	if len(resp) == 0 {
		glog.Info("len(rates) == 0")
	}

	return resp, nil
}
