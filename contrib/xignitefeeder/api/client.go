package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/alpacahq/marketstore/v4/utils/log"
)

const (
	// XigniteBaseURL is a Base URL for Quick Xignite API
	// (https://www.marketdata-cloud.quick-co.jp/Products/)
	XigniteBaseURL = "https://api.marketdata-cloud.quick-co.jp"
	// GetQuotesURL is the URL of Get Quotes endpoint
	// (https://www.marketdata-cloud.quick-co.jp/Products/QUICKEquityRealTime/Overview/GetQuotes)
	GetQuotesURL = XigniteBaseURL + "/QUICKEquityRealTime.json/GetQuotes"
	// ListSymbolsURL is the URL of List symbols endpoint
	// (https://www.marketdata-cloud.quick-co.jp/Products/QUICKEquityRealTime/Overview/ListSymbols)
	ListSymbolsURL = XigniteBaseURL + "/QUICKEquityRealTime.json/ListSymbols"
	// ListIndexSymbolsURL is the URL of List symbols endpoint
	// (https://www.marketdata-cloud.quick-co.jp/Products/QUICKIndexHistorical/Overview/ListSymbols)
	// /QUICKEquityRealTime.json/ListSymbols : list symbols for a exchange
	// /QUICKIndexHistorical.json/ListSymbols : list index symbols for an index group (ex. TOPIX).
	ListIndexSymbolsURL = XigniteBaseURL + "/QUICKIndexHistorical.json/ListSymbols"
	// GetBarsURL is the URL of Get Bars endpoint
	// (https://www.marketdata-cloud.quick-co.jp/Products/QUICKEquityRealTime/Overview/GetBars)
	GetBarsURL = XigniteBaseURL + "/QUICKEquityRealTime.json/GetBars"
	// GetIndexBarsURL is the URL of QuickIndexRealTime/GetBars endpoint
	// (https://www.marketdata-cloud.quick-co.jp/Products/QUICKIndexRealTime/Overview/GetBars)
	GetIndexBarsURL = XigniteBaseURL + "/QUICKIndexRealTime.json/GetBars"
	// GetQuotesRangeURL is the URL of Get Quotes Range endpoint
	// (https://www.marketdata-cloud.quick-co.jp/Products/QUICKEquityHistorical/Overview/GetQuotesRange)
	GetQuotesRangeURL = XigniteBaseURL + "/QUICKEquityHistorical.json/GetQuotesRange"
	// GetIndexQuotesRangeURL is the URL of Get Index Quotes Range endpoint
	// (https://www.marketdata-cloud.quick-co.jp/Products/QUICKIndexHistorical/Overview/GetQuotesRange)
	GetIndexQuotesRangeURL = XigniteBaseURL + "/QUICKIndexHistorical.json/GetQuotesRange"

	SuccessOutcome = "Success"
)

// Client calls an endpoint and returns the parsed response.
type Client interface {
	GetRealTimeQuotes(ctx context.Context, identifiers []string) (GetQuotesResponse, error)
	ListSymbols(ctx context.Context, exchange string) (ListSymbolsResponse, error)
	ListIndexSymbols(ctx context.Context, indexGroup string) (ListIndexSymbolsResponse, error)
	GetRealTimeBars(ctx context.Context, identifier string, start, end time.Time,
	) (response GetBarsResponse, err error)
	GetIndexBars(ctx context.Context, identifier string, start, end time.Time,
	) (response GetIndexBarsResponse, err error)
	GetQuotesRange(ctx context.Context, identifier string, startDate, endDate time.Time,
	) (response GetQuotesRangeResponse, err error)
	GetIndexQuotesRange(ctx context.Context, identifier string, startDate, endDate time.Time,
	) (response GetIndexQuotesRangeResponse, err error)
}

// NewDefaultAPIClient initializes Xignite API client with the specified API token and HTTP timeout[sec].
func NewDefaultAPIClient(token string, timeoutSec int) *DefaultClient {
	return &DefaultClient{
		httpClient: &http.Client{Timeout: time.Duration(timeoutSec) * time.Second},
		token:      token,
	}
}

// DefaultClient is the Xignite API client with a default http client.
type DefaultClient struct {
	httpClient *http.Client
	token      string
}

// GetRealTimeQuotes calls GetQuotes endpoint of Xignite API with specified identifiers
// and returns the parsed API response
// https://www.marketdata-cloud.quick-co.jp/Products/QUICKEquityRealTime/Overview/GetQuotes
func (c *DefaultClient) GetRealTimeQuotes(ctx context.Context, identifiers []string,
) (response GetQuotesResponse, err error) {
	form := url.Values{
		"IdentifierType": {"Symbol"},
		"_token":         {c.token},
		"Identifiers":    {strings.Join(identifiers, ",")},
	}
	req, err := http.NewRequestWithContext(ctx,
		"POST", GetQuotesURL, strings.NewReader(form.Encode()))
	if err != nil {
		return response, errors.Wrap(err, "failed to create an http request")
	}
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	log.Info("GetRealTimeQuotes API request: IdentifierType=Symbol, num_identifiers=%d", len(identifiers))
	err = c.execute(req, &response)
	if err != nil {
		return response, err
	}

	// log not-successful responses
	if len(identifiers) != len(response.ArrayOfEquityQuote) {
		log.Error(fmt.Sprintf("The len(ArrayOfEquityQuotes) returned by GetQuotes API is different "+
			"from len(identifiers) requested. returned=%d, requested=%d, error response=%v",
			len(response.ArrayOfEquityQuote), len(identifiers), response))
		return response, nil
	}

	for i, equityQuote := range response.ArrayOfEquityQuote {
		if equityQuote.Outcome != SuccessOutcome {
			log.Error(fmt.Sprintf("GetQuotes API returned an error. identifier=%s, response=%v",
				identifiers[i], equityQuote))
		}
	}

	return response, nil
}

// ListSymbols calls /QUICKEquityRealTime.json/ListSymbols endpoint of Xignite API with a specified exchange
// and returns the parsed API response
// https://www.marketdata-cloud.quick-co.jp/Products/QUICKEquityRealTime/Overview/ListSymbols
// exchange: XTKS, XNGO, XSAP, XFKA, XJAS, XTAM
func (c *DefaultClient) ListSymbols(ctx context.Context, exchange string) (response ListSymbolsResponse, err error) {
	apiURL := ListSymbolsURL + fmt.Sprintf("?_token=%s&Exchange=%s", c.token, exchange)
	req, err := http.NewRequestWithContext(ctx, "GET", apiURL, http.NoBody)
	if err != nil {
		return response, errors.Wrap(err, "failed to create an http request")
	}

	err = c.execute(req, &response)
	if err != nil {
		return response, err
	}

	if response.Outcome != SuccessOutcome {
		return response, fmt.Errorf("error from Xignite API(ListSymbols) %v", response)
	}

	return response, nil
}

// ListIndexSymbols calls QUICKIndexHistorical.json/ListSymbols endpoint of Xignite API with a specified index group
// and returns the parsed API response
// https://www.marketdata-cloud.quick-co.jp/Products/QUICKIndexHistorical/Overview/ListSymbols
// indexGroup: INDXJPX, IND_NIKKEI.
func (c *DefaultClient) ListIndexSymbols(ctx context.Context, indexGroup string,
) (response ListIndexSymbolsResponse, err error) {
	apiURL := ListIndexSymbolsURL + fmt.Sprintf("?_token=%s&GroupName=%s", c.token, indexGroup)
	req, err := http.NewRequestWithContext(ctx, "GET", apiURL, http.NoBody)
	if err != nil {
		return response, errors.Wrap(err, "failed to create an http request")
	}

	err = c.execute(req, &response)
	if err != nil {
		return response, err
	}

	if response.Outcome != SuccessOutcome {
		return response, errors.Errorf("error response is returned from Xignite. %v", response)
	}

	return response, nil
}

func getBarsURLValues(token, identifier string, start, end time.Time) url.Values {
	return url.Values{
		"IdentifierType":   {"Symbol"},
		"_token":           {token},
		"Identifier":       {identifier},
		"StartDateTime":    {start.Format(XigniteDateTimeLayout)},
		"EndDateTime":      {end.Format(XigniteDateTimeLayout)},
		"Precision":        {"FiveMinutes"},
		"AdjustmentMethod": {"All"},
		"Language":         {"Japanese"},
	}
}

// GetRealTimeBars calls GetBars endpoint of Xignite API with a specified identifier, time period
// and Precision=FiveMinutes, and returns the parsed API response
// https://www.marketdata-cloud.quick-co.jp/Products/QUICKEquityRealTime/Overview/GetBars
func (c *DefaultClient) GetRealTimeBars(ctx context.Context, identifier string, start, end time.Time,
) (response GetBarsResponse, err error) {
	form := getBarsURLValues(c.token, identifier, start, end)
	req, err := getBarsRequest(ctx, GetBarsURL, form.Encode())
	if err != nil {
		return response, err
	}
	err = c.execute(req, &response)
	if err != nil {
		return response, err
	}

	log.Debug(fmt.Sprintf("[Xignite API] Delay(sec) in GetBars response= %f", response.DelaySec))

	return response, nil
}

// GetIndexBars calls QUICKIndex/GetBars endpoint of Xignite API with a specified identifier, time period
// and Precision=FiveMinutes, and returns the parsed API response
// https://www.marketdata-cloud.quick-co.jp/Products/QUICKIndexRealTime/Overview/GetBars
func (c *DefaultClient) GetIndexBars(ctx context.Context, identifier string, start, end time.Time,
) (response GetIndexBarsResponse, err error) {
	form := getBarsURLValues(c.token, identifier, start, end)
	req, err := getBarsRequest(ctx, GetIndexBarsURL, form.Encode())
	if err != nil {
		return response, err
	}
	err = c.execute(req, &response)
	if err != nil {
		return response, err
	}

	log.Debug(fmt.Sprintf("[Xignite API] Delay(sec) in QUICKIndexRealTime/GetBars response= %f", response.DelaySec))

	return response, nil
}

func getBarsRequest(ctx context.Context, url, body string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx,
		"POST", url, strings.NewReader(body))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create an http request")
	}
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	return req, nil
}

// GetQuotesRange calls QUICKEquityHistorical/GetQuotesRange endpoint of Xignite API with a specified identifier
// // and returns the parsed API response
// https://www.marketdata-cloud.quick-co.jp/Products/QUICKEquityRealTime/Overview/GetQuotes
func (c *DefaultClient) GetQuotesRange(ctx context.Context, identifier string, startDate, endDate time.Time,
) (response GetQuotesRangeResponse, err error) {
	formValues := quotesRangeFormValues(c.token, identifier, startDate, endDate)
	req, err := quotesRangeReq(ctx, GetQuotesRangeURL, formValues.Encode())
	if err != nil {
		return response, err
	}
	err = c.execute(req, &response)
	if err != nil {
		return response, err
	}

	if response.Outcome != SuccessOutcome {
		return response, errors.Errorf("error response is returned from Xignite. response=%v"+
			", identifier=%s", response, identifier)
	}

	return response, nil
}

// GetIndexQuotesRange calls QUICKIndexHistorical/GetQuotesRange endpoint with a specified index symbol
// and returns the parsed API response
// https://www.marketdata-cloud.quick-co.jp/Products/QUICKIndexHistorical/Overview/GetQuotesRange
// As of 2019-08, the API response model is exactly the same as Get Quotes Range API.
func (c *DefaultClient) GetIndexQuotesRange(ctx context.Context, identifier string, startDate, endDate time.Time,
) (response GetIndexQuotesRangeResponse, err error) {
	formValues := quotesRangeFormValues(c.token, identifier, startDate, endDate)
	req, err := quotesRangeReq(ctx, GetIndexQuotesRangeURL, formValues.Encode())
	if err != nil {
		return response, err
	}
	err = c.execute(req, &response)
	if err != nil {
		return response, err
	}

	if response.Outcome != SuccessOutcome {
		return response, errors.Errorf("error response is returned from Xignite. response=%v"+
			", identifier=%s", response, identifier)
	}

	return response, nil
}

func quotesRangeFormValues(token, identifier string, startDate, endDate time.Time) *url.Values {
	return &url.Values{
		"IdentifierType":   {"Symbol"},
		"_token":           {token},
		"Identifier":       {identifier},
		"AdjustmentMethod": {"All"},
		// "yyyy/mm/dd" format
		"StartOfDate": {fmt.Sprintf("%d/%02d/%02d", startDate.Year(), startDate.Month(), startDate.Day())},
		"EndOfDate":   {fmt.Sprintf("%d/%02d/%02d", endDate.Year(), endDate.Month(), endDate.Day())},
	}
}

func quotesRangeReq(ctx context.Context, url, body string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, "POST", url, strings.NewReader(body))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create an http request")
	}
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	return req, nil
}

// execute performs an HTTP request and parse the response body.
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
	b, err := io.ReadAll(resp.Body)
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
