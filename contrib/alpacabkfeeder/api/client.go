package api

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	v1 "github.com/alpacahq/marketstore/v4/contrib/alpacabkfeeder/api/v1"
)

const (
	rateLimitRetryCount = 3
	rateLimitRetryDelay = time.Second
)

var (
	// DefaultClient is the default Alpaca client using the
	// environment variable set credentials.
	DefaultClient = NewClient(Credentials())
	base          = "https://api.alpaca.markets"
	dataURL       = "https://data.alpaca.markets"
	apiVersion    = "v2"
	clientTimeout = 10 * time.Second
	do            = defaultDo
)

func defaultDo(c *Client, req *http.Request) (*http.Response, error) {
	if c.credentials.OAuth != "" {
		req.Header.Set("Authorization", "Bearer "+c.credentials.OAuth)
	} else {
		if strings.Contains(req.URL.String(), "sandbox") {
			// Add Basic Auth
			req.SetBasicAuth(c.credentials.ID, c.credentials.Secret)
		} else {
			req.Header.Set("APCA-API-KEY-ID", c.credentials.ID)
			req.Header.Set("APCA-API-SECRET-KEY", c.credentials.Secret)
		}
	}

	client := &http.Client{
		Timeout: clientTimeout,
	}
	var resp *http.Response
	var err error
	for i := 0; ; i++ {
		resp, err = client.Do(req)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			break
		}
		if i >= rateLimitRetryCount {
			break
		}
		time.Sleep(rateLimitRetryDelay)
	}

	if err = verify(resp); err != nil {
		return nil, err
	}

	return resp, nil
}

const (
	// v2MaxLimit is the maximum allowed limit parameter for all v2 endpoints.
	v2MaxLimit = 10000
)

func init() {
	if s := os.Getenv("APCA_API_BASE_URL"); s != "" {
		base = s
	} else if s := os.Getenv("ALPACA_BASE_URL"); s != "" {
		// legacy compatibility...
		base = s
	}
	if s := os.Getenv("APCA_DATA_URL"); s != "" {
		dataURL = s
	}
	// also allow APCA_API_DATA_URL to be consistent with the python SDK
	if s := os.Getenv("APCA_API_DATA_URL"); s != "" {
		dataURL = s
	}
	if s := os.Getenv("APCA_API_VERSION"); s != "" {
		apiVersion = s
	}
	if s := os.Getenv("APCA_API_CLIENT_TIMEOUT"); s != "" {
		d, err := time.ParseDuration(s)
		if err != nil {
			log.Fatal("invalid APCA_API_CLIENT_TIMEOUT: " + err.Error())
		}
		clientTimeout = d
	}
}

// APIError wraps the detailed code and message supplied
// by Alpaca's API for debugging purposes.
type APIError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *APIError) Error() string {
	return e.Message
}

// Client is an Alpaca REST API client.
type Client struct {
	credentials *APIKey
}

func SetBaseUrl(baseUrl string) {
	base = baseUrl
}

// NewClient creates a new Alpaca client with specified
// credentials.
func NewClient(credentials *APIKey) *Client {
	return &Client{credentials: credentials}
}

// GetSnapshots returns the snapshots for multiple symbol.
func (c *Client) GetSnapshots(symbols []string) (map[string]*Snapshot, error) {
	u, err := url.Parse(fmt.Sprintf("%s/v2/stocks/snapshots?symbols=%s",
		dataURL, strings.Join(symbols, ",")))
	if err != nil {
		return nil, err
	}

	resp, err := c.get(u)
	if err != nil {
		return nil, err
	}

	var snapshots map[string]*Snapshot

	if err = unmarshal(resp, &snapshots); err != nil {
		return nil, err
	}

	return snapshots, nil
}

// GetMultiBars returns bars for the given symbols.
func (c *Client) GetMultiBars(
	symbols []string, params GetBarsParams,
) (map[string][]Bar, error) {
	bars := make(map[string][]Bar, len(symbols))
	for item := range c.GetMultiBarsAsync(symbols, params) {
		if err := item.Error; err != nil {
			return nil, err
		}
		bars[item.Symbol] = append(bars[item.Symbol], item.Bar)
	}
	return bars, nil
}

// GetMultiBarsAsync returns a channel that will be populated with the bars for the requested symbols.
func (c *Client) GetMultiBarsAsync(symbols []string, params GetBarsParams) <-chan MultiBarItem {
	ch := make(chan MultiBarItem)

	go func() {
		defer close(ch)

		u, err := url.Parse(fmt.Sprintf("%s/v2/stocks/bars", dataURL))
		if err != nil {
			ch <- MultiBarItem{Error: err}
			return
		}

		q := u.Query()
		q.Set("symbols", strings.Join(symbols, ","))
		setQueryBarParams(q, params, "")

		received := 0
		for params.TotalLimit == 0 || received < params.TotalLimit {
			setQueryLimit(q, params.TotalLimit, params.PageLimit, received)
			u.RawQuery = q.Encode()

			resp, err := c.get(u)
			if err != nil {
				ch <- MultiBarItem{Error: err}
				return
			}

			var barResp multiBarResponse
			if err = unmarshal(resp, &barResp); err != nil {
				ch <- MultiBarItem{Error: err}
				return
			}

			sortedSymbols := make([]string, 0, len(barResp.Bars))
			for symbol := range barResp.Bars {
				sortedSymbols = append(sortedSymbols, symbol)
			}
			sort.Strings(sortedSymbols)

			for _, symbol := range sortedSymbols {
				bars := barResp.Bars[symbol]
				for _, bar := range bars {
					ch <- MultiBarItem{Symbol: symbol, Bar: bar}
				}
				received += len(bars)
			}
			if barResp.NextPageToken == nil {
				return
			}
			q.Set("page_token", *barResp.NextPageToken)
		}
	}()

	return ch
}

func setQueryBarParams(q url.Values, params GetBarsParams, feed string) {
	setBaseQuery(q, params.Start, params.End, params.Feed, feed)
	adjustment := Raw
	if params.Adjustment != "" {
		adjustment = params.Adjustment
	}
	q.Set("adjustment", string(adjustment))
	timeframe := OneDay
	if params.TimeFrame.N != 0 {
		timeframe = params.TimeFrame
	}
	q.Set("timeframe", timeframe.String())
}

func setBaseQuery(q url.Values, start, end time.Time, feed, defaultFeed string) {
	if !start.IsZero() {
		q.Set("start", start.Format(time.RFC3339))
	}
	if !end.IsZero() {
		q.Set("end", end.Format(time.RFC3339))
	}
	if feed != "" {
		q.Set("feed", feed)
	} else {
		if defaultFeed != "" {
			q.Set("feed", feed)
		}
	}
}

func setQueryLimit(q url.Values, totalLimit int, pageLimit int, received int) {
	limit := 0 // use server side default if unset
	if pageLimit != 0 {
		limit = pageLimit
	}
	if totalLimit != 0 {
		remaining := totalLimit - received
		if remaining <= 0 { // this should never happen
			return
		}
		if (limit == 0 || limit > remaining) && remaining <= v2MaxLimit {
			limit = remaining
		}
	}

	if limit != 0 {
		q.Set("limit", fmt.Sprintf("%d", limit))
	}
}

// ListAssets returns the list of assets, filtered by
// the input parameters.
func (c *Client) ListAssets(status *string) ([]v1.Asset, error) {
	// TODO: add tests
	apiVer := apiVersion
	if strings.Contains(base, "broker") {
		apiVer = "v1"
	}

	// TODO: support different asset classes
	u, err := url.Parse(fmt.Sprintf("%s/%s/assets", base, apiVer))
	if err != nil {
		return nil, err
	}

	q := u.Query()

	if status != nil {
		q.Set("status", *status)
	}

	u.RawQuery = q.Encode()

	resp, err := c.get(u)
	if err != nil {
		return nil, err
	}

	assets := []v1.Asset{}

	if err = unmarshal(resp, &assets); err != nil {
		return nil, err
	}

	return assets, nil
}

func (c *Client) get(u *url.URL) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}

	return do(c, req)
}

func verify(resp *http.Response) (err error) {
	if resp.StatusCode >= http.StatusMultipleChoices {
		var body []byte
		defer resp.Body.Close()

		body, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}

		apiErr := APIError{}

		err = json.Unmarshal(body, &apiErr)
		if err != nil {
			return fmt.Errorf("json unmarshal error: %s", err.Error())
		}
		if err == nil {
			err = &apiErr
		}
	}

	return
}

func unmarshal(resp *http.Response, data interface{}) error {
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	return json.Unmarshal(body, data)
}
