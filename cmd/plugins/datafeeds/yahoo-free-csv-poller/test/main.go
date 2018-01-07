package main

import (
	"fmt"
	"github.com/alpacahq/marketstore/cmd/plugins/datafeeds"
	"github.com/alpacahq/marketstore/feedmanager"
	"github.com/alpacahq/marketstore/utils/io"
	"os"
)

func main() {
	var err error
	/*
		Open the plugin for this datafeed
	*/
	pi, err := feedmanager.OpenPluginInGOPATH("yahoo-free-csv-poller.so")
	if err != nil {
		fmt.Println("Error opening plugin...")
		os.Exit(1)
	}
	sym, err := pi.Lookup("Datafeed")
	if err != nil {
		panic(err)
	}
	df := sym.(*datafeeds.DatafeedType)

	/*
		Initialize the plugin with a list of symbols using the default baseURL
	*/
	symbols := []string{"AAPL", "TSLA"}
	var destinations []*io.TimeBucketKey
	catKey := "Symbol/Timeframe/AttributeGroup"
	for _, sym := range symbols {
		itemKey := sym + "/1Min/BIDASKSHARES"
		destinations = append(destinations,
			io.NewTimeBucketKey(itemKey, catKey),
		)
	}
	feedState, exampleCSM, err := df.Init("", destinations)
	if err != nil {
		panic(err)
	}
	fmt.Println(feedState, exampleCSM)

	// Test status using Get()
	result, err := df.Get(feedState, nil)
	fmt.Println(result)

	// Test building internal data structure
	csm, err := df.Poll(feedState, nil)
	for key, cs := range csm {
		fmt.Println(key.GetItemInCategory("Symbol"))
		for _, name := range cs.GetColumnNames() {
			col := cs.GetColumn(name)
			fmt.Printf("%s: %v ", name, col)
		}
		fmt.Printf("\n")
	}
}
