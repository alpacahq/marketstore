package config

import (
	"strings"

	"github.com/alpacahq/marketstore/v4/contrib/alpaca/enums"
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
	// The things we want to subscribe to
	Subscription
}

type Subscription struct {
	// list of symbols whose minute bars are important
	MinuteBarSymbols []string `json:"minute_bar_symbols"`
	// list of symbols whose quotes are important
	QuoteSymbols []string `json:"quote_symbols"`
	// list of symbols whose trades are important
	TradeSymbols []string `json:"trade_symbols"`
}

// AsCanonical returns the list of prefixed
// streams that we want to subscribe to
func (s *Subscription) AsCanonical() []string {
	return flatten(
		prefixStrings(normalizeSubscriptions(s.MinuteBarSymbols), enums.AggToMinute),
		prefixStrings(normalizeSubscriptions(s.QuoteSymbols), enums.Quote),
		prefixStrings(normalizeSubscriptions(s.TradeSymbols), enums.Trade),
	)
}

func flatten(lists ...[]string) []string {
	totalLength := 0
	for _, l := range lists {
		totalLength += len(l)
	}
	res := make([]string, 0, totalLength)
	for _, l := range lists {
		res = append(res, l...)
	}
	return res
}

func prefixStrings(list []string, prefix enums.Prefix) []string {
	res := make([]string, len(list))
	for i, s := range list {
		res[i] = string(prefix) + s
	}
	return res
}

func normalizeSubscriptions(list []string) []string {
	if containsWildcard(list) {
		return []string{"*"}
	}
	return list
}

func containsWildcard(list []string) bool {
	for _, s := range list {
		if strings.Contains(s, "*") {
			return true
		}
	}
	return false
}
