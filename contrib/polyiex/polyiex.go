package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"

	"github.com/alpacahq/marketstore/contrib/polyiex/api"
	"github.com/alpacahq/marketstore/contrib/polyiex/handlers"
	"github.com/alpacahq/marketstore/plugins/bgworker"
	"github.com/alpacahq/marketstore/utils/log"
)

type PolyIEXFetcher struct {
	config FetcherConfig
}

type FetcherConfig struct {
	APIKey  string `json:"api_key"`
	BaseURL string `json:"base_url"`
}

// NewBgWorker creates a new bgworker for polygon/IEX
func NewBgWorker(conf map[string]interface{}) (bgworker.BgWorker, error) {
	data, _ := json.Marshal(conf)
	config := FetcherConfig{}
	json.Unmarshal(data, &config)

	if config.APIKey == "" {
		err := errors.New("[polyiex]: api_key is required")
		log.Error("%v", err)
		return nil, err
	}

	if config.BaseURL == "" {
		err := errors.New("[polyiex]: base_url is required")
		log.Error("%v", err)
		return nil, err
	}

	return &PolyIEXFetcher{
		config: config,
	}, nil
}

// Run is the bgworker main entry point.
func (pf *PolyIEXFetcher) Run() {
	// configure api package
	api.SetAPIKey(pf.config.APIKey)
	api.SetBaseURL(pf.config.BaseURL)

	// api.Stream(handlers.Trade, api.TradePrefix, nil)
	api.Stream(handlers.Book, api.BookPrefix, nil)

	select {}
}

func (pf *PolyIEXFetcher) handleMessage(rawMsg []byte) error {
	log.Info("recv: %v", string(rawMsg))

	return nil
}

func main() {
	log.SetLevel(log.DEBUG)
	conf := map[string]interface{}{}
	conf["api_key"] = os.Getenv("POLYIEX_API_KEY")
	if len(os.Args) < 2 {
		progname := path.Base(os.Args[0])
		fmt.Printf("Usage: %s <base_url>\n", progname)
		return
	}
	conf["base_url"] = os.Args[1]
	pf, err := NewBgWorker(conf)
	if err != nil {
		log.Error("failed to create bgworker: %v", err)
		return
	}
	pf.Run()
}
