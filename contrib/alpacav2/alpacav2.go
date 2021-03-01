package main

import (
	"encoding/json"
	"errors"

	"github.com/alpacahq/marketstore/v4/contrib/alpacav2/config"
	"github.com/alpacahq/marketstore/v4/contrib/alpacav2/handlers"

	"github.com/alpacahq/marketstore/v4/contrib/alpacav2/api"
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
		WSServer:      "wss://stream.data.alpaca.markets/v2",
		WSWorkerCount: 10,
		Source:        "iex",
	}
	err = json.Unmarshal(data, &config)
	if err != nil {
		return
	}

	if config.APIKey == "" || config.APISecret == "" {
		return nil, errors.New("api_key and api_secret needs to be set")
	}

	handlers.UseOldSchema = config.UseOldSchema
	handlers.AddTickCnt = config.AddTickCnt

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
