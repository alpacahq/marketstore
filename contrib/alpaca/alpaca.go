package main

import (
	"encoding/json"
	"errors"

	"github.com/alpacahq/marketstore/v4/contrib/alpaca/api"
	"github.com/alpacahq/marketstore/v4/contrib/alpaca/config"
	"github.com/alpacahq/marketstore/v4/contrib/alpaca/handlers"
	"github.com/alpacahq/marketstore/v4/plugins/bgworker"
)

const defaultWSWorkerCount = 10

type AlpacaStreamer struct {
	config config.Config
}

// NewBgWorker returns a new instance of AlpacaStreamer. See config
// for more details about configuring AlpacaStreamer.
// nolint:deadcode // used as a marketstore plugin
func NewBgWorker(conf map[string]interface{}) (w bgworker.BgWorker, err error) {
	data, _ := json.Marshal(conf)
	cfg := config.Config{
		WSServer:      "wss://data.alpaca.markets/stream",
		WSWorkerCount: defaultWSWorkerCount,
	}
	err = json.Unmarshal(data, &cfg)
	if err != nil {
		return
	}

	if cfg.APIKey == "" || cfg.APISecret == "" {
		return nil, errors.New("api_key and api_secret needs to be set")
	}

	return &AlpacaStreamer{
		config: cfg,
	}, nil
}

// Run the AlpacaStreamer, by starting the streaming API.
func (as *AlpacaStreamer) Run() {
	api.NewSubscription(as.config).Start(handlers.MessageHandler)
	select {}
}

func main() {}
