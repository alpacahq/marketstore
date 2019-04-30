package api

import (
	"encoding/json"
	"fmt"
	"github.com/alpacahq/marketstore/utils/log"
	"github.com/pkg/errors"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	XigniteBaseURL = "https://api.marketdata-cloud.quick-co.jp"
	GetQuotesURL   = XigniteBaseURL + "/QUICKEquityRealTime.json/GetQuotes"
	// ?IdentifierType=Symbol&Identifiers=6501.XTKS,7751.XTKS&_Language=English"
	ListSymbolsURL = XigniteBaseURL + "/QUICKEquityRealTime.json/ListSymbols"
	// ?Exchange=XJAS&_Language=English
	GetQuotesRangeURL = XigniteBaseURL + "/QUICKEquityHistorical.json/GetQuotesRange"
)

// Client calls an endpoint and returns the parsed response
type Client interface {
	GetRealTimeQuotes(identifiers []string) (GetQuotesResponse, error)
	ListSymbols(exchange string) (ListSymbolsResponse, error)
	GetQuotesRange(identifier string, startDate, endDate time.Time) (response GetQuotesRangeResponse, err error)
}

func NewDefaultAPIClient(token string, timeoutSec int) *DefaultClient {
	return &DefaultClient{
		httpClient: &http.Client{Timeout: time.Duration(timeoutSec) * time.Second},
		token:      token,
	}
}

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
		return response, errors.Wrap(err, fmt.Sprintf("failed to create an http request."))
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
	apiUrl := ListSymbolsURL + fmt.Sprintf("?_token=%s&Exchange=%s", c.token, exchange)
	req, err := http.NewRequest("GET", apiUrl, nil)
	if err != nil {
		return response, errors.Wrap(err, fmt.Sprintf("failed to create an http request."))
	}

	err = c.execute(req, &response)
	if err != nil {
		return response, err
	}

	if response.Outcome != "Success" {
		return response, errors.New(fmt.Sprintf("error response is returned from Xignite. %v", response))
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
		return response, errors.Wrap(err, fmt.Sprintf("failed to create an http request."))
	}
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	err = c.execute(req, &response)
	if err != nil {
		return response, err
	}

	if response.Outcome != "Success" {
		return response, errors.New(fmt.Sprintf("error response is returned from Xignite. response=%v"+
			", identifier=%s", response, identifier))
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
