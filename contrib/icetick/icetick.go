package main

import (
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/icetick/icetickloader"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/models"
	"github.com/alpacahq/marketstore/v4/models/enum"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/log"
	"github.com/spf13/cobra"
)

const (
	defaultConfigFilePath = "./mkts.yml"
	configDesc            = "set the path for the marketstore YAML configuration file"
)

var (
	// configFilePath set flag for a path to the config file.
	configFilePath string
)

func start() error {

	// Attempt to read config file.
	data, err := ioutil.ReadFile(configFilePath)
	if err != nil {
		return fmt.Errorf("failed to read configuration file error: %s", err.Error())
	}

	// Log config location.
	log.Info("using %v for configuration", configFilePath)

	config, err := utils.InstanceConfig.Parse(data)
	if err != nil {
		return fmt.Errorf("failed to parse configuration file error: %v", err.Error())
	}
	// start := time.Now()

	var rs executor.ReplicationSender
	// instanceConfig, shutdownPending, walWG := executor.NewInstanceSetup(
	executor.NewInstanceSetup(
		config.RootDirectory,
		rs,
		config.WALRotateInterval,
		config.InitCatalog,
		config.InitWALCache,
		false, //config.BackgroundSync,
		true,  //config.WALBypass,
	)

	return nil
}

var cmd = &cobra.Command{
	Short: "load <filename>",
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := start(); err != nil {
			return err
		}

		fileName := args[0]
		fp, err := os.Open(fileName)
		if err != nil {
			return err
		}
		defer fp.Close()

		greader, err := gzip.NewReader(fp)
		if err != nil {
			return err
		}
		var mode icetickloader.Mode
		bufferSize := 1024 * 10
		mode.Set("q")
		quotesMap := map[string]*models.Quote{}
		_, qStream := icetickloader.ParseStream(greader, mode)
		for q := range qStream {
			quotes, ok := quotesMap[q.Symbol]
			if !ok {
				quotes = models.NewQuote(q.Symbol, bufferSize)
				quotesMap[q.Symbol] = quotes
			}
			bidPrice, _ := strconv.ParseFloat(q.BidPrice, 64)
			askPrice, _ := strconv.ParseFloat(q.AskPrice, 64)
			bidSize, _ := strconv.ParseUint(q.BidSize, 10, 64)
			askSize, _ := strconv.ParseUint(q.AskSize, 10, 64)
			quotes.Add(
				q.Timestamp.Unix(),
				q.Timestamp.Nanosecond(),
				bidPrice,
				askPrice,
				int(bidSize),
				int(askSize),
				enum.UndefinedExchange, // .BidExchange,
				enum.UndefinedExchange, // .AskExchange,
				' ',
			)

			if err := quotes.FlushIfFull(); err != nil {
				log.Error("flush error: %w", err)
			}
		}
		for symbol := range quotesMap {
			quotesMap[symbol].Write()
		}
		return nil
	},
}

func main() {
	utils.InstanceConfig.StartTime = time.Now()
	cmd.Flags().StringVarP(&configFilePath, "config", "c", defaultConfigFilePath, configDesc)
	if err := cmd.Execute(); err != nil {
		log.Fatal("failed main %v", err)
	}
}
