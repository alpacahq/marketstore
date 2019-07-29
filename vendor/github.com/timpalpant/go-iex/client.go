package iex

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/google/go-querystring/query"
)

const baseEndpoint = "https://api.iextrading.com/1.0"

// HTTPClient an interface to describe simple requests to a url
type HTTPClient interface {
	Get(url string) (resp *http.Response, err error)
}

// Client provides methods to interact with IEX's HTTP API for developers.
type Client struct {
	client HTTPClient
}

// NewClient create a new client
func NewClient(client HTTPClient) *Client {
	return &Client{client}
}

// GetTOPS provides IEX’s aggregated best quoted bid and offer
// position in near real time for all securities on IEX’s
// displayed limit order book. TOPS is ideal for developers
// needing both quote and trade data.
//
// Symbols may be any of the available symbols returned by
// GetSymbols(). If symbols is nil, then all symbols will be returned.
func (c *Client) GetTOPS(symbols []string) ([]*TOPS, error) {
	req := &topsRequest{symbols}
	var result []*TOPS
	err := c.getJSON("/tops", req, &result)
	return result, err
}

type topsRequest struct {
	Symbols []string `url:"symbols,comma,omitempty"`
}

// GetLast provides trade data for executions on IEX.
// It is a near real time, intraday API that provides IEX last sale price,
// size and time. Last is ideal for developers that need a lightweight
// stock quote.
//
// Symbols may be any of the available symbols returned by
// GetSymbols(). If symbols is nil, then all symbols will be returned.
func (c *Client) GetLast(symbols []string) ([]*Last, error) {
	req := &lastRequest{symbols}
	var result []*Last
	err := c.getJSON("/tops/last", req, &result)
	return result, err
}

type lastRequest struct {
	Symbols []string `url:"symbols,comma,omitempty"`
}

// GetHIST will provide the output of IEX data products for download on
// a T+1 basis. Data will remain available for the trailing twelve months.
//
// Only data for the given day will be returned.
func (c *Client) GetHIST(date time.Time) ([]*HIST, error) {
	req := &histRequest{}
	if !date.IsZero() {
		req.Date = date.Format("20060102")
	}

	var result []*HIST
	err := c.getJSON("/hist", req, &result)
	return result, err
}

type histRequest struct {
	Date string `url:"date,omitempty"`
}

// GetAllAvailableHIST returns HIST data for all available dates.
// Returns a map of date string "20060102" -> HIST data for that date.
func (c *Client) GetAllAvailableHIST() (map[string][]*HIST, error) {
	var result map[string][]*HIST
	err := c.getJSON("/hist", nil, &result)
	return result, err
}

// GetDEEP is used to receive real-time depth of book quotations direct from IEX.
// The depth of book quotations received via DEEP provide an aggregated size
// of resting displayed orders at a price and side, and do not indicate the
// size or number of individual orders at any price level. Non-displayed
// orders and non-displayed portions of reserve orders are not represented
// in DEEP.
//
// DEEP also provides last trade price and size information.
// Trades resulting from either displayed or non-displayed orders
// matching on IEX will be reported. Routed executions will not be reported.
func (c *Client) GetDEEP(symbol string) (*DEEP, error) {
	req := &deepRequest{symbol}
	result := &DEEP{}
	err := c.getJSON("/deep", req, &result)
	return result, err
}

type deepRequest struct {
	Symbols string `url:"symbols"`
}

// GetBook shows IEX’s bids and asks for given symbols.
//
// A maximumum of 10 symbols may be requested.
func (c *Client) GetBook(symbols []string) (map[string]*Book, error) {
	req := &bookRequest{symbols}
	var result map[string]*Book
	err := c.getJSON("/deep/book", req, &result)
	return result, err
}

type bookRequest struct {
	Symbols []string `url:"symbols,comma,omitempty"`
}

// GetTrades report messages are sent when an order on the IEX Order Book is
// executed in whole or in part. DEEP sends a Trade report message for
// every individual fill.
//
// A maximum of 10 symbols may be requested. Last is the number of trades
// to fetch, and must be <= 500.
func (c *Client) GetTrades(symbols []string, last int) (map[string][]*Trade, error) {
	req := &tradesRequest{symbols, last}
	var result map[string][]*Trade
	err := c.getJSON("/deep/trades", req, &result)
	return result, err
}

type tradesRequest struct {
	Symbols []string `url:"symbols,comma,omitempty"`
	Last    int      `url:"last,omitempty"`
}

// GetSystemEvents gets the system event message which is used to indicate events that apply to
// the market or the data feed.
//
// There will be a single message disseminated per channel for each
// System Event type within a given trading session.
//
// A maximumum of 10 symbols may be requested.
func (c *Client) GetSystemEvents(symbols []string) (map[string]*SystemEvent, error) {
	req := &systemEventRequest{symbols}
	var result map[string]*SystemEvent
	err := c.getJSON("/deep/system-event", req, &result)
	return result, err
}

type systemEventRequest struct {
	Symbols []string `url:"symbols,comma,omitempty"`
}

// GetTradingStatus gets the trading status message which, is used to
// indicate the current trading status of a security.
// For IEX-listed securities, IEX acts as the primary market
// and has the authority to institute a trading halt or trading pause in a
// security due to news dissemination or regulatory reasons. For
// non-IEX-listed securities, IEX abides by any regulatory trading halts
// and trading pauses instituted by the primary or listing market, as
// applicable.
//
// IEX disseminates a full pre-market spin of Trading status messages
// indicating the trading status of all securities. In the spin, IEX will
// send out a Trading status message with “T” (Trading) for all securities
// that are eligible for trading at the start of the Pre-Market Session.
// If a security is absent from the dissemination, firms should assume
// that the security is being treated as operationally halted in the IEX
// Trading System.
//
// After the pre-market spin, IEX will use the Trading status message to
// relay changes in trading status for an individual security. Messages
// will be sent when a security is:
//
//     Halted
//     Paused*
//     Released into an Order Acceptance Period*
//     Released for trading
//
// *The paused and released into an Order Acceptance Period status will be
// disseminated for IEX-listed securities only. Trading pauses on
// non-IEX-listed securities will be treated simply as a halt.
//
// A maximumum of 10 symbols may be requested.
func (c *Client) GetTradingStatus(symbols []string) (map[string]*TradingStatusMessage, error) {
	req := &tradingStatusRequest{symbols}
	var result map[string]*TradingStatusMessage
	err := c.getJSON("/deep/trading-status", req, &result)
	return result, err
}

type tradingStatusRequest struct {
	Symbols []string `url:"symbols,comma,omitempty"`
}

// GetOperationalHaltStatus gets all of the instances where the
// security were halted
//
// The Exchange may suspend trading of one or more securities on IEX
// for operational reasons and indicates such operational halt using
// the Operational halt status message.
//
// IEX disseminates a full pre-market spin of Operational halt status
// messages indicating the operational halt status of all securities.
// In the spin, IEX will send out an Operational Halt Message with “N”
// (Not operationally halted on IEX) for all securities that are
// eligible for trading at the start of the Pre-Market Session. If a
// security is absent from the dissemination, firms should assume that
// the security is being treated as operationally halted in the IEX
// Trading System at the start of the Pre-Market Session.
//
// After the pre-market spin, IEX will use the Operational halt status
// message to relay changes in operational halt status for an
// individual security.
//
// A maximumum of 10 symbols may be requested.
func (c *Client) GetOperationalHaltStatus(symbols []string) (map[string]*OpHaltStatus, error) {
	req := &opHaltStatusRequest{symbols}
	var result map[string]*OpHaltStatus
	err := c.getJSON("/deep/op-halt-status", req, &result)
	return result, err
}

type opHaltStatusRequest struct {
	Symbols []string `url:"symbols,comma,omitempty"`
}

// GetShortSaleRestriction In association with Rule 201 of Regulation SHO, the Short Sale
// Price Test Message is used to indicate when a short sale price
// test restriction is in effect for a security.
//
// IEX disseminates a full pre-market spin of Short sale price test
// status messages indicating the Rule 201 status of all securities.
// After the pre-market spin, IEX will use the Short sale price test
// status message in the event of an intraday status change.
//
// The IEX Trading System will process orders based on the latest
// short sale price test restriction status.
//
// A maximumum of 10 symbols may be requested.
func (c *Client) GetShortSaleRestriction(symbols []string) (map[string]*SSRStatus, error) {
	req := &ssrStatusRequest{symbols}
	var result map[string]*SSRStatus
	err := c.getJSON("/deep/ssr-status", req, &result)
	return result, err
}

type ssrStatusRequest struct {
	Symbols []string `url:"symbols,comma,omitempty"`
}

// GetSecurityEvents The Security event message is used to indicate events that
// apply to a security. A Security event message will be sent
// whenever such event occurs.
//
// A maximumum of 10 symbols may be requested.
func (c *Client) GetSecurityEvents(symbols []string) (map[string]*SecurityEventMessage, error) {
	req := &securityEventRequest{symbols}
	var result map[string]*SecurityEventMessage
	err := c.getJSON("/deep/security-event", req, &result)
	return result, err
}

type securityEventRequest struct {
	Symbols []string `url:"symbols,comma,omitempty"`
}

// GetTradeBreaks Trade break messages are sent when an execution on IEX is broken
// on that same trading day. Trade breaks are rare and only affect
// applications that rely upon IEX execution based data.
//
// A maximum of 10 symbols may be requested. Last is the number of trades
// to fetch, and must be <= 500.
func (c *Client) GetTradeBreaks(symbols []string, last int) (map[string][]*TradeBreak, error) {
	req := &tradeBreaksRequest{symbols, last}
	var result map[string][]*TradeBreak
	err := c.getJSON("/deep/trade-breaks", req, &result)
	return result, err
}

type tradeBreaksRequest struct {
	Symbols []string `url:"symbols,comma,omitempty"`
	Last    int      `url:"last,omitempty"`
}

// GetMarkets This endpoint returns near real time traded volume on the markets.
// Market data is captured by the IEX system from approximately
// 7:45 a.m. to 5:15 p.m. ET.
func (c *Client) GetMarkets() ([]*Market, error) {
	var result []*Market
	err := c.getJSON("/market", nil, &result)
	return result, err
}

// GetSymbols returns an array of symbols IEX supports for trading.
// This list is updated daily as of 7:45 a.m. ET. Symbols may be added
// or removed by IEX after the list was produced.
func (c *Client) GetSymbols() ([]*Symbol, error) {
	var result []*Symbol
	err := c.getJSON("/ref-data/symbols", nil, &result)
	return result, err
}

// GetIntradayStats gets intra day volume and pricing data
func (c *Client) GetIntradayStats() (*IntradayStats, error) {
	var result *IntradayStats
	err := c.getJSON("/stats/intraday", nil, &result)
	return result, err
}

// GetRecentStats This call will return a minimum of the last five trading days up
// to all trading days of the current month.
func (c *Client) GetRecentStats() ([]*Stats, error) {
	var result []*Stats
	err := c.getJSON("/stats/recent", nil, &result)
	return result, err
}

// GetHistoricalSummary Historical data is only available for prior months,
// starting with January 2014.
// If date IsZero(), returns the prior month's data.
func (c *Client) GetHistoricalSummary(date time.Time) ([]*HistoricalSummary, error) {
	req := &historicalSummaryRequest{}
	if !date.IsZero() {
		req.Date = date.Format("20060102")
	}

	var result []*HistoricalSummary
	err := c.getJSON("/stats/historical", req, &result)
	return result, err
}

type historicalSummaryRequest struct {
	Date string `url:"date,omitempty"`
}

// GetHistoricalDaily This call will return daily stats for a given month or day.
// Historical data is only available for prior months, starting with January 2014.
func (c *Client) GetHistoricalDaily(req *HistoricalDailyRequest) ([]*Stats, error) {
	var result []*Stats
	err := c.getJSON("/stats/historical/daily", req, &result)
	return result, err
}

// HistoricalDailyRequest holds optional data either for Date or Last
type HistoricalDailyRequest struct {
	// Option 1: Value needs to be in four-digit year, two-digit
	// month format (YYYYMM) (i.e January 2017 would be written as 201701)
	//
	// Option 2: Value needs to be in four-digit year, two-digit month,
	// two-digit day format (YYYYMMDD) (i.e January 21, 2017 would be
	// written as 20170121).
	Date string `url:"date,omitempty"`

	// Is used in place of date to retrieve last n number of trading days.
	// Value can only be a number up to 90.
	Last int `url:"last,omitempty"`
}

// GetKeyStats returns key statistics for a symbol.
func (c *Client) GetKeyStats(symbol string) (*KeyStats, error) {
	var result *KeyStats
	err := c.getJSON("/stock/"+symbol+"/stats", nil, &result)
	if err != nil {
		return nil, err
	}
	if x, ok := result.ExDividendDateJSON.(int); ok {
		result.ExDividendDate = "n/a"
	} else {
		result.ExDividendDate = fmt.Sprintf("%v", x)
	}
	if x, ok := result.ShortDateJSON.(int); ok {
		result.ShortDate = "n/a"
	} else {
		result.ShortDate = fmt.Sprintf("%v", x)
	}
	if x, ok := result.RevenuePerEmployeeJSON.(float64); ok {
		result.RevenuePerEmployee = x
	} else {
		result.RevenuePerEmployee = 0
	}
	return result, nil
}

// GetNews returns news items for a symbol. Use "market" to receive global market news.
func (c *Client) GetNews(symbol string) ([]*News, error) {
	var result []*News
	err := c.getJSON("/stock/"+symbol+"/news", nil, &result)
	return result, err
}

// GetStockQuotes returns a map of quotes for the given symbols.
//
// A maximumum of 100 symbols may be requested.
func (c *Client) GetStockQuotes(symbols []string) (map[string]*StockQuote, error) {
	req := &stockQuotesRequest{symbols, "quote"}
	var qresult map[string]map[string]*StockQuote
	err := c.getJSON("/stock/market/batch", req, &qresult)
	if err != nil {
		return nil, err
	}
	result := map[string]*StockQuote{}
	for k := range qresult {
		result[k] = qresult[k]["quote"]
	}
	return result, err
}

type stockQuotesRequest struct {
	Symbols []string `url:"symbols,comma,omitempty"`
	Type    string   `url:"types,comma,omitempty"`
}

// GetList returns a map of quotes for the given list.
// list can be "mostactive", "gainers" or "losers".
//
// See: https://iextrading.com/developer/docs/#list
func (c *Client) GetList(list string) ([]*StockQuote, error) {
	var result []*StockQuote
	err := c.getJSON("/stock/market/list/"+list+"?displayPercent=true", nil, &result)
	return result, err
}

// GetCompany gets company information
func (c *Client) GetCompany(symbol string) (*Company, error) {
	var result *Company
	err := c.getJSON("/stock/"+symbol+"/company", nil, &result)
	return result, err
}

// GetDividends gets last 5 years of dividends
func (c *Client) GetDividends(symbol string) ([]*Dividends, error) {
	var result []*Dividends
	err := c.getJSON("/stock/"+symbol+"/dividends/5y", nil, &result)
	if err != nil {
		return nil, err
	}
	for _, d := range result {
		if x, ok := d.IndicatedJSON.(float64); ok {
			d.Indicated = x
		} else {
			d.Indicated = 0
		}
		if x, ok := d.AmountJSON.(float64); ok {
			d.Amount = x
		} else {
			d.Amount = 0
		}
	}
	return result, nil
}

// GetChart retuns chart data for a symbol covering a date range.
// Range can be: 5y 2y 1y ytd 6m 3d 1m 1d
// Please note the 1d range returns different data than other formats.
//
// TODO: This is pretty undefined and unsupported right now due to different chart types.
// See: https://iextrading.com/developer/docs/#chart
func (c *Client) GetChart(symbol string, daterange string) ([]*Chart, error) {
	var result []*Chart
	err := c.getJSON("/stock/"+symbol+"/chart/"+daterange, nil, &result)
	return result, err
}

func (c *Client) getJSON(route string, request interface{}, response interface{}) error {
	url := c.endpoint(route)

	values, err := query.Values(request)
	if err != nil {
		return err
	}
	queryString := values.Encode()
	if queryString != "" {
		url = url + "?" + queryString
	}

	resp, err := c.client.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf("%v: %v", resp.Status, string(body))
	}

	dec := json.NewDecoder(resp.Body)
	return dec.Decode(response)
}

func (c *Client) endpoint(route string) string {
	return baseEndpoint + route
}
