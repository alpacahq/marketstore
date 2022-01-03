package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"
)

//TradeBucketedResponse json from bitMEX bucketed trade API.
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

// BitmexClient with direct API methods.
type BitmexClient struct {
	Client        *http.Client
	baseURL       string
	apiURL        string
	bitmexBinSize map[string]string
}

// NewBitmexClient is the constructor of the BitmexClient.
func NewBitmexClient(hc *http.Client) *BitmexClient {
	return &BitmexClient{
		Client:  hc,
		baseURL: "https://www.bitmex.com",
		apiURL:  "/api/v1/",
		bitmexBinSize: map[string]string{
			"1Min": "1m",
			"5Min": "5m",
			"1H":   "1h",
			"1D":   "1d",
		},
	}
}

// GetInstruments from bitmex API.
func (c *BitmexClient) GetInstruments() ([]string, error) {
	reqURL := c.baseURL + c.apiURL + "/instrument/active"
	req, err := http.NewRequestWithContext(context.Background(), "GET", reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create http req for %s: %w", reqURL, err)
	}
	res, err := c.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("unable to get active instruments: %w", err)
	}
	defer res.Body.Close()
	instruments := []struct {
		Symbol string `json:"symbol"`
	}{}
	err = json.NewDecoder(res.Body).Decode(&instruments)
	if err != nil {
		return nil, err
	}
	symbols := make([]string, len(instruments))
	for i, instrument := range instruments {
		symbols[i] = instrument.Symbol
	}
	return symbols, nil
}

// GetBuckets from bitmex Trade API.
func (c *BitmexClient) GetBuckets(symbol string, from time.Time, binSize string) ([]TradeBucketedResponse, error) {
	resp := []TradeBucketedResponse{}

	values := url.Values{
		"symbol":    []string{symbol},
		"binSize":   []string{c.bitmexBinSize[binSize]},
		"partial":   []string{"false"},
		"count":     []string{"500"},
		"reverse":   []string{"false"},
		"startTime": []string{from.String()},
	}
	uri, err := url.Parse(c.baseURL + c.apiURL + "/trade/bucketed")
	if err != nil {
		return nil, err
	}
	uri.RawQuery = values.Encode()
	reqURL := uri.String()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")

	res, err := c.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode >= http.StatusMultipleChoices {
		body, err := ioutil.ReadAll(res.Body)
		if err != nil {
			return nil, err
		}

		return nil, fmt.Errorf("status code %v, response=%v", res.StatusCode, string(body))
	}
	err = json.NewDecoder(res.Body).Decode(&resp)
	if err != nil {
		return nil, err
	}

	return resp, nil
}
