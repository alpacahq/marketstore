package api

import (
	"testing"
	"time"
)

var client BitmexClient

func init() {
	client = Init()
}

func TestGetInstruments(t *testing.T) {
	symbols, err := client.GetInstruments()
	if err != nil {
		t.Error(err)
	}
	found := false
	for _, symbol := range symbols {
		if symbol == "XBTUSD" {
			found = true
		}
	}
	if !found {
		t.Error("Did not find XBTUSD symbol")
	}
}

func TestGetBucket(t *testing.T) {
	symbol := "XBTUSD"
	lastWeek := time.Now().AddDate(0, -1, 0)
	trades, err := client.GetBuckets(symbol, lastWeek, "1H")
	if err != nil {
		t.Error(err)
	}
	if len(trades) == 0 {
		t.Errorf("Did not load any trades from GetBucket()")
	}
	if trades[0].Symbol != symbol {
		t.Errorf("Did not load trades from correct symbol %s", symbol)
	}
}
