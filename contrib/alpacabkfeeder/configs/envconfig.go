package configs

import (
	"os"
	"time"
)

// APItoken, UpdateTime, and Basic Auth of stocksJsonURL settings can be overridden by environment variables
// to flexibly re-run processes that are performed only at marketstore start-up/certain times of the day
// and not to write security-related configs directly in the configuration file.

// envOverride updates some configs by environment variables.
func envOverride(config *DefaultConfig) (*DefaultConfig, error) {
	// override SymbolsUpdateTime
	symbolsUpdateTime := os.Getenv("ALPACA_BROKER_FEEDER_SYMBOLS_UPDATE_TIME")
	if symbolsUpdateTime != "" {
		t, err := time.Parse(ctLayout, symbolsUpdateTime)
		if err != nil {
			return nil, err
		}
		config.SymbolsUpdateTime = t
	}

	// override UpdateTime
	updateTime := os.Getenv("ALPACA_BROKER_FEEDER_UPDATE_TIME")
	if updateTime != "" {
		t, err := time.Parse(ctLayout, updateTime)
		if err != nil {
			return nil, err
		}
		config.UpdateTime = t
	}

	// override API Key ID / API Secret Key
	apiKeyID := os.Getenv("ALPACA_BROKER_FEEDER_API_KEY_ID")
	if apiKeyID != "" {
		config.APIKeyID = apiKeyID
	}

	apiSecretKey := os.Getenv("ALPACA_BROKER_FEEDER_API_SECRET_KEY")
	if apiSecretKey != "" {
		config.APISecretKey = apiSecretKey
	}

	// override the basic Auth of Stocks Json URL and basic auth
	// override the basic Auth of Stocks Json URL
	stocksJSONURL := os.Getenv("ALPACA_BROKER_FEEDER_STOCKS_JSON_URL")
	if stocksJSONURL != "" {
		config.StocksJSONURL = stocksJSONURL
	}
	stocksJSONBasicAuth := os.Getenv("ALPACA_BROKER_FEEDER_STOCKS_JSON_BASIC_AUTH")
	if stocksJSONBasicAuth != "" {
		config.StocksJSONBasicAuth = stocksJSONBasicAuth
	}

	return config, nil
}
