package configs

import (
	"os"
	"strings"
	"time"
)

// APItoken and UpdateTime settings can be overridden by environment variables
// to flexibly re-run processes that are performed only at marketstore start-up/certain times of the day
// and not to write security-related configs directly in the configuration file.

// envOverride updates some configs by environment variables.
func envOverride(config *DefaultConfig) (*DefaultConfig, error) {
	// override UpdateTime
	updateTime := os.Getenv("XIGNITE_FEEDER_UPDATE_TIME")
	if updateTime != "" {
		t, err := time.Parse(ctLayout, updateTime)
		if err != nil {
			return nil, err
		}
		config.UpdateTime = t
	}

	// override APIToken
	apiToken := os.Getenv("XIGNITE_FEEDER_API_TOKEN")
	if apiToken != "" {
		config.APIToken = apiToken
	}

	// override NotQuoteSymbolList
	notQuoteStockList := os.Getenv("XIGNITE_FEEDER_NOT_QUOTE_STOCK_LIST")
	if notQuoteStockList != "" {
		config.NotQuoteStockList = strings.Split(notQuoteStockList, ",")
	}

	return config, nil
}
