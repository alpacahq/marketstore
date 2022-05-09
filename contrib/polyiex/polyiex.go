package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/alpacahq/marketstore/v4/contrib/polyiex/api"
	"github.com/alpacahq/marketstore/v4/contrib/polyiex/handlers"
	"github.com/alpacahq/marketstore/v4/plugins/bgworker"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

type PolyIEXFetcher struct {
	config FetcherConfig
}

type FetcherConfig struct {
	APIKey  string `json:"api_key"`
	BaseURL string `json:"base_url"`
}

// NewBgWorker creates a new bgworker for polygon/IEX.
func NewBgWorker(conf map[string]interface{}) (bgworker.BgWorker, error) {
	data, _ := json.Marshal(conf)
	config := FetcherConfig{}
	_ = json.Unmarshal(data, &config)

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

	err := api.Stream(handlers.Tick, api.TradePrefix, nil)
	if err != nil {
		log.Error("PolyIEXFetcher error(Trade):" + err.Error())
	}
	err = api.Stream(handlers.Tick, api.BookPrefix, nil)
	if err != nil {
		log.Error("PolyIEXFetcher error(Book):" + err.Error())
	}

	select {}
}

func configLog() {
	atom := zap.NewAtomicLevel()

	encoderCfg := zap.NewProductionEncoderConfig()
	encoderCfg.TimeKey = "timestamp"
	encoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder

	logger := zap.New(zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderCfg),
		zapcore.Lock(os.Stdout),
		atom,
	))
	atom.SetLevel(zapcore.DebugLevel)

	zap.ReplaceGlobals(logger)
	log.SetLevel(log.DEBUG)
}

func main() {
	configLog()

	handlers.SkipWrite(true)
	conf := map[string]interface{}{}
	conf["api_key"] = os.Getenv("POLYIEX_API_KEY")
	if len(os.Args) < 2 {
		progname := path.Base(os.Args[0])
		// nolint:forbidigo // CLI output needs fmt.Println
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
