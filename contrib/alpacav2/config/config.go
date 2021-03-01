package config

import (
	"fmt"
	"strings"
)

// Config describes the AlpacaStreamer configuration
type Config struct {
	// How many workers should be used to process the incoming WS messages
	WSWorkerCount int `json:"ws_worker_count"`
	// Alpaca API key ID for authenticating with their APIs
	APIKey string `json:"api_key"`
	// Alpaca API key secret for authenticating with their APIs
	APISecret string `json:"api_secret"`
	// websocket server for Alpaca
	WSServer string `json:"ws_server"`
	// Source is the data source to use
	Source string `json:"source"`
	// UseOldSchema sets whether the old schema should be used
	UseOldSchema bool `json:"use_old_schema"`
	// AddTickCnt sets whether TickCnt column should be added to old schema bars
	AddTickCnt bool `json:"add_tick_cnt"`
	// The things we want to subscribe to
	Subscription
}

// Subscription is the collection of Bars, Quotes and Trades we subscribe to
type Subscription struct {
	// list of symbols whose minute bars are important
	MinuteBarSymbols []string `json:"minute_bar_symbols"`
	// list of symbols whose quotes are important
	QuoteSymbols []string `json:"quote_symbols"`
	// list of symbols whose trades are important
	TradeSymbols []string `json:"trade_symbols"`
}

// String returns the subscriptions in the format that is expected by the server
// i.e. `"trades":["AAPL","..."],"quotes":["AAPL","..."],"bars":["AAPL","..."]`
func (s Subscription) String() string {
	return fmt.Sprintf(
		`"trades":%s,"qoutes":%s,"bars":%s`,
		toJSONArray(s.TradeSymbols),
		toJSONArray(s.QuoteSymbols),
		toJSONArray(s.MinuteBarSymbols),
	)
}

func toJSONArray(s []string) string {
	return strings.ReplaceAll(fmt.Sprintf("%q", s), " ", ",")
}
