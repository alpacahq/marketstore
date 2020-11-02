package main

import (
	"encoding/json"
	"errors"

	"github.com/alpacahq/marketstore/v4/contrib/alpaca/config"
	"github.com/alpacahq/marketstore/v4/contrib/alpaca/handlers"

	"github.com/alpacahq/marketstore/v4/contrib/alpaca/api"
	"github.com/alpacahq/marketstore/v4/plugins/bgworker"
)

type AlpacaStreamer struct {
	config config.Config
}

// NewBgWorker returns a new instance of AlpacaStreamer. See config
// for more details about configuring AlpacaStreamer.
func NewBgWorker(conf map[string]interface{}) (w bgworker.BgWorker, err error) {
	data, _ := json.Marshal(conf)
	config := config.Config{
		WSServer:      "wss://data.alpaca.markets/stream",
		WSWorkerCount: 10,
	}
	err = json.Unmarshal(data, &config)
	if err != nil {
		return
	}

	if config.APIKey == "" || config.APISecret == "" {
		return nil, errors.New("api_key and api_secret needs to be set")
	}

	return &AlpacaStreamer{
		config: config,
	}, nil
}

// Run the AlpacaStreamer, by starting the streaming API.
func (as *AlpacaStreamer) Run() {
	api.NewSubscription(as.config).Start(handlers.MessageHandler)
	select {}
}

func main() {}
