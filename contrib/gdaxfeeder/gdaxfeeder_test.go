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
	config := getConfig(`{
        "symbols": ["BTC-USD"]
        }`)
	var worker *GdaxFetcher
	var ret bgworker.BgWorker
	var err error
	ret, err = NewBgWorker(config)
	worker = ret.(*GdaxFetcher)
	assert.Len(t, worker.symbols, 1)
	assert.Equal(t, worker.symbols[0], "BTC-USD")
	assert.Nil(t, err)

	config = getConfig(`{
        "symbols": ["BTC-USD", "ETH-USD", "LTC-BTC"]
        }`)
	ret, err = NewBgWorker(config)
	assert.Nil(t, err)
	worker = ret.(*GdaxFetcher)
	assert.Len(t, worker.symbols, 3)

	config = getConfig(`{
        "query_start": "2017-01-02 00:00"
        }`)
	ret, err = NewBgWorker(config)
	worker = ret.(*GdaxFetcher)
	assert.Nil(t, err)
	assert.False(t, worker.queryStart.IsZero())
}
