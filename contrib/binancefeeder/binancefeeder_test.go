package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/alpacahq/marketstore/v4/plugins/bgworker"
)

func getConfig(data string) (ret map[string]interface{}) {
	json.Unmarshal([]byte(data), &ret)
	return
}

func TestNew(t *testing.T) {
	t.Parallel()
	var config = getConfig(`{
		"symbols": ["ETH"],
		"base_currencies": ["USDT", "BTC"]
        }`)
	var worker *BinanceFetcher
	var ret bgworker.BgWorker
	var err error
	ret, err = NewBgWorker(config)
	worker = ret.(*BinanceFetcher)
	assert.Len(t, worker.symbols, 1)
	assert.Equal(t, worker.symbols[0], "ETH")
	assert.Len(t, worker.baseCurrencies, 2)
	assert.Equal(t, worker.baseCurrencies[0], "USDT")
	assert.Equal(t, worker.baseCurrencies[1], "BTC")
	assert.Nil(t, err)

	//The symbols from the biannce API can very well change so
	//if this test fails, consider that the API might of changed with more symbols

	// config = getConfig(``)
	// ret, err = NewBgWorker(config)
	// worker = ret.(*BinanceFetcher)
	// c.Assert(err, IsNil)
	// c.Assert(len(worker.symbols), Equals, 357)

	config = getConfig(`{
        "query_start": "2017-01-02 00:00"
        }`)
	ret, err = NewBgWorker(config)
	worker = ret.(*BinanceFetcher)
	assert.Nil(t, err)
	assert.False(t, worker.queryStart.IsZero())
}
