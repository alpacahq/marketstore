package api

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/alpacahq/marketstore/utils/log"
	"github.com/pkg/errors"
)

const (
	// XigniteBaseURL is a Base URL for Quick Xignite API
	// (https://www.marketdata-cloud.quick-co.jp/Products/)
	XigniteBaseURL = "https://api.marketdata-cloud.quick-co.jp"
	// GetQuotesURL is the URL of Get Quotes endpoint
	// (https://www.marketdata-cloud.quick-co.jp/Products/QUICKEquityRealTime/Overview/GetQuotes)
	GetQuotesURL = XigniteBaseURL + "/QUICKEquityRealTime.json/GetQuotes"
	// ListSymbolsURL is the URL of List Symbols endpoint
	// (https://www.marketdata-cloud.quick-co.jp/Products/QUICKEquityRealTime/Overview/ListSymbols)
	ListSymbolsURL = XigniteBaseURL + "/QUICKEquityRealTime.json/ListSymbols"
	// GetQuotesRangeURL is the URL of Get Quotes Range endpoint
	// (https://www.marketdata-cloud.quick-co.jp/Products/QUICKEquityHistorical/Overview/GetQuotesRange)
	GetQuotesRangeURL = XigniteBaseURL + "/QUICKEquityHistorical.json/GetQuotesRange"
)

// Client calls an endpoint and returns the parsed response
type Client interface {
	GetRealTimeQuotes(identifiers []string) (GetQuotesResponse, error)
	ListSymbols(exchange string) (ListSymbolsResponse, error)
	GetQuotesRange(identifier string, startDate, endDate time.Time) (response GetQuotesRangeResponse, err error)
}

// NewDefaultAPIClient initializes Xignite API client with the specified API token and HTTP timeout[sec].
func NewDefaultAPIClient(token string, timeoutSec int) *DefaultClient {
	return &DefaultClient{
		httpClient: &http.Client{Timeout: time.Duration(timeoutSec) * time.Second},
		token:      token,
	}
}

// DefaultClient is the Xignite API client object.
type DefaultClient struct {
	httpClient *http.Client
	token      string
}

// GetRealTimeQuotes calls GetQuotes endpoint of Xignite API with specified identifiers
//// and returns the parsed API response
// https://www.marketdata-cloud.quick-co.jp/Products/QUICKEquityRealTime/Overview/GetQuotes
func (c *DefaultClient) GetRealTimeQuotes(identifiers []string) (response GetQuotesResponse, err error) {

	form := url.Values{
		"IdentifierType": {"Symbol"},
		"_token":         {c.token},
		"Identifiers":    {strings.Join(identifiers, ",")},
	}
	req, err := http.NewRequest("POST", GetQuotesURL, strings.NewReader(form.Encode()))
	if err != nil {
		return response, errors.Wrap(err, "failed to create an http request.")
	}
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	err = c.execute(req, &response)
	if err != nil {
		return response, err
	}

	log.Debug(fmt.Sprintf("[Xignite API] Delay(sec) in GetQuotes response= %f", response.DelaySec))

	return response, nil
}

// ListSymbols calls ListSymbols endpoint of Xignite API with a specified exchange
// and returns the parsed API response
// https://www.marketdata-cloud.quick-co.jp/Products/QUICKEquityRealTime/Overview/ListSymbols
// exchange: XTKS, XNGO, XSAP, XFKA, XJAS, XTAM
func (c *DefaultClient) ListSymbols(exchange string) (response ListSymbolsResponse, err error) {
	apiURL := ListSymbolsURL + fmt.Sprintf("?_token=%s&Exchange=%s", c.token, exchange)
	req, err := http.NewRequest("GET", apiURL, nil)
	if err != nil {
		return response, errors.Wrap(err, "failed to create an http request.")
	}

	err = c.execute(req, &response)
	if err != nil {
		return response, err
	}

	if response.Outcome != "Success" {
		return response, errors.Errorf("error response is returned from Xignite. %v", response)
	}

	return response, nil
}

// GetQuotesRange calls GetQuotes endpoint of Xignite API with specified identifiers
//// and returns the parsed API response
// https://www.marketdata-cloud.quick-co.jp/Products/QUICKEquityRealTime/Overview/GetQuotes
func (c *DefaultClient) GetQuotesRange(identifier string, startDate, endDate time.Time) (response GetQuotesRangeResponse, err error) {
	form := url.Values{
		"IdentifierType":   {"Symbol"},
		"_token":           {c.token},
		"Identifier":       {identifier},
		"AdjustmentMethod": {"All"},
		// "yyyy/mm/dd" format
		"StartOfDate": {fmt.Sprintf("%d/%02d/%02d", startDate.Year(), startDate.Month(), startDate.Day())},
		"EndOfDate":   {fmt.Sprintf("%d/%02d/%02d", endDate.Year(), endDate.Month(), endDate.Day())},
	}
	req, err := http.NewRequest("POST", GetQuotesRangeURL, strings.NewReader(form.Encode()))
	if err != nil {
		return response, errors.Wrap(err, "failed to create an http request.")
	}
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	err = c.execute(req, &response)
	if err != nil {
		return response, err
	}

	if response.Outcome != "Success" {
		return response, errors.Errorf("error response is returned from Xignite. response=%v"+
			", identifier=%s", response, identifier)
	}

	return response, nil
}

// execute performs an HTTP request and parse the response body
func (c *DefaultClient) execute(req *http.Request, responsePtr interface{}) error {
	log.Debug(fmt.Sprintf("[Xignite API] request url=%v", req.URL))

	// execute the HTTP request
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to execute HTTP request. request=%v", req))
	}
	defer func() {
		if cerr := resp.Body.Close(); err == nil {
			err = errors.Wrap(cerr, fmt.Sprintf("failed to close HTTP response. resp=%v", resp))
		}
	}()

	// read the response body and parse to a json
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to read the response body. resp=%v", resp))
	}
	// API response body is too big to output...
	// log.Debug(fmt.Sprintf("[Xignite API response] url=%v, response= %v", req.URL, string(b)))

	if err := json.Unmarshal(b, responsePtr); err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to json_parse the response body. resp.Body=%v", string(b)))
	}

	return nil
}
