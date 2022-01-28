package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"

	"github.com/alpacahq/marketstore/v4/plugins/bgworker"
)

func getConfig(t *testing.T, data string) (ret map[string]interface{}) {
	t.Helper()

	err := json.Unmarshal([]byte(data), &ret)
	require.Nil(t, err)
	return ret
}

func TestNew(t *testing.T) {
	t.Parallel()
	config := getConfig(t, `{
		"symbols": ["ETH"],
		"base_currencies": ["USDT", "BTC"]
        }`)
	var worker *BinanceFetcher
	var ret bgworker.BgWorker
	var err error
	ret, err = NewBgWorker(config)
	worker, ok := ret.(*BinanceFetcher)
	assert.True(t, ok)
	assert.Len(t, worker.symbols, 1)
	assert.Equal(t, worker.symbols[0], "ETH")
	assert.Len(t, worker.baseCurrencies, 2)
	assert.Equal(t, worker.baseCurrencies[0], "USDT")
	assert.Equal(t, worker.baseCurrencies[1], "BTC")
	assert.Nil(t, err)

	config = getConfig(t, `{
        "query_start": "2017-01-02 00:00"
        }`)
	ret, err = NewBgWorker(config)
	worker, ok = ret.(*BinanceFetcher)
	assert.True(t, ok)
	assert.Nil(t, err)
	assert.False(t, worker.queryStart.IsZero())
}
