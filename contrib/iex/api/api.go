package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/alpacahq/marketstore/v4/utils/log"
)

const (
	BatchSize = 100
)

var (
	NY, _           = time.LoadLocation("America/New_York")
	token           string
	base            = "https://cloud.iexapis.com/stable"
	symbolsExcluded = map[string]bool{}
)

func SetToken(t string) {
	token = t
}

func SetSandbox(b bool) {
	if b {
		base = "https://sandbox.iexapis.com/stable"
	} else {
		base = "https://cloud.iexapis.com/stable"
	}
}

type GetBarsResponse map[string]*ChartResponse

type ChartResponse struct {
	Chart          []Chart `json:"chart"`
	IntradayPrices []Chart `json:"intraday-prices"`
}

type Chart struct {
	Date                 string  `json:"date"`
	Minute               string  `json:"minute"`
	Label                string  `json:"label"`
	High                 float32 `json:"high"`
	Low                  float32 `json:"low"`
	Average              float64 `json:"average"`
	Volume               int32   `json:"volume"`
	Notional             float64 `json:"notional"`
	NumberOfTrades       int     `json:"numberOfTrades"`
	MarketHigh           float64 `json:"marketHigh"`
	MarketLow            float64 `json:"marketLow"`
	MarketAverage        float64 `json:"marketAverage"`
	MarketVolume         int     `json:"marketVolume"`
	MarketNotional       float64 `json:"marketNotional"`
	MarketNumberOfTrades int     `json:"marketNumberOfTrades"`
	Open                 float32 `json:"open"`
	Close                float32 `json:"close"`
	MarketOpen           float64 `json:"marketOpen,omitempty"`
	MarketClose          float64 `json:"marketClose,omitempty"`
	ChangeOverTime       float64 `json:"changeOverTime"`
	MarketChangeOverTime float64 `json:"marketChangeOverTime"`
}

func (c *Chart) GetTimestamp() (ts time.Time, err error) {
	if c.Minute == "" {
		// daily bar
		ts, err = time.ParseInLocation("2006-01-02", c.Date, NY)
	} else {
		// intraday bar
		tStr := fmt.Sprintf("%v %v", c.Date, c.Minute)
		ts, err = time.ParseInLocation("2006-01-02 15:04", tStr, NY)
	}
	return ts, err
}

func SupportedRange(r string) bool {
	switch r {
	case "5y":
	case "2y":
	case "1y":
	case "ytd":
	case "6m":
	case "3m":
	case "1m":
	case "1d":
	case "date":
	case "dynamic":
	default:
		return false
	}
	return true
}

func GetBars(ctx context.Context, symbols []string, barRange string, limit *int, retries int,
) (*GetBarsResponse, error) {
	u, err := url.Parse(fmt.Sprintf("%s/stock/market/batch", base))
	if err != nil {
		return nil, err
	}

	if len(symbols) == 0 {
		return &GetBarsResponse{}, nil
	}

	var newsymbols []string
	for _, sym := range symbols {
		if !symbolsExcluded[sym] {
			newsymbols = append(newsymbols, sym)
		}
	}
	symbols = newsymbols

	q := u.Query()

	q.Set("symbols", strings.Join(symbols, ","))
	q.Set("token", token)
	if barRange == "1d" {
		q.Set("types", "intraday-prices")
	} else {
		q.Set("types", "chart")
	}
	q.Set("chartIEXOnly", "true")

	if SupportedRange(barRange) {
		q.Set("range", barRange)
	} else {
		return nil, fmt.Errorf("%v is not a supported bar range", barRange)
	}

	if limit != nil && *limit > 0 {
		const decimal = 10
		q.Set("chartLast", strconv.FormatInt(int64(*limit), decimal))
	}

	u.RawQuery = q.Encode()

	// fmt.Println(u.String())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("create GET request for %s: %w", u.String(), err)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	defer func(Body io.ReadCloser) {
		if err2 := Body.Close(); err2 != nil {
			log.Error(fmt.Sprintf("failed to close readCloser. err=%v", err2))
		}
	}(res.Body)

	if res.StatusCode == http.StatusTooManyRequests {
		if retries > 0 {
			<-time.After(time.Second)
			return GetBars(ctx, symbols, barRange, limit, retries-1)
		}

		return nil, fmt.Errorf("retry count exceeded")
	}

	var resp GetBarsResponse

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	if res.StatusCode == http.StatusUnavailableForLegalReasons {
		// One of the symbols is DELAYED_OTC
		// Binary divide the symbols list until we can identify the conflict
		if len(symbols) == 1 { // Idenified an OTC symbol
			symbolsExcluded[symbols[0]] = true
			return nil, fmt.Errorf("OTC Error: %s: %s [Symbol: %s]", res.Status, string(body), symbols[0])
		}

		split := len(symbols) / 2

		// fmt.Printf("Symbol groups: %v - %v\n", symbols[:split], symbols[split:])

		resp = *addBarsToResp(ctx, resp, symbols[:split], barRange, limit, retries)
		resp = *addBarsToResp(ctx, resp, symbols[split:], barRange, limit, retries)

		return &resp, nil
	}

	// res.StatusCode != http.StatusUnavailableForLegalReasons
	if err = json.Unmarshal(body, &resp); err != nil {
		return nil, errors.New(res.Status + ": " + string(body))
	}

	if q.Get("types") == "intraday-prices" {
		for key, val := range resp {
			resp[key].Chart = val.IntradayPrices
		}
	}

	if resp[symbols[0]] != nil && resp[symbols[0]].Chart == nil {
		if retries > 0 {
			// log.Info("retrying due to null response")
			<-time.After(time.Second)
			return GetBars(ctx, symbols, barRange, limit, retries-1)
		}
		return nil, fmt.Errorf("retry count exceeded")
	}

	return &resp, nil
}

func addBarsToResp(ctx context.Context, resp GetBarsResponse, symbols []string, barRange string,
	limit *int, retries int,
) *GetBarsResponse {
	r, err := GetBars(ctx, symbols, barRange, limit, retries)
	if err != nil {
		log.Error(err.Error())
	} else {
		for k, v := range *r {
			resp[k] = v
		}
	}
	return &resp
}

type ListSymbolsResponse []struct {
	Symbol    string `json:"symbol"`
	Date      string `json:"date"`
	IsEnabled bool   `json:"isEnabled"`
}

func ListSymbols() (*ListSymbolsResponse, error) {
	symbolsURL := fmt.Sprintf("%s/ref-data/iex/symbols?token=%s", base, token)

	req, err := http.NewRequest(http.MethodGet, symbolsURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create http request for %s: %w", symbolsURL, err)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	defer func(Body io.ReadCloser) {
		err2 := Body.Close()
		if err2 != nil {
			log.Error(fmt.Sprintf("failed to close readCloser. err=%v", err2))
		}
	}(res.Body)

	if res.StatusCode > http.StatusMultipleChoices {
		return nil, fmt.Errorf("status code %v", res.StatusCode)
	}

	var resp ListSymbolsResponse

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	if err2 := json.Unmarshal(body, &resp); err2 != nil {
		return nil, err2
	}

	return &resp, nil
}
