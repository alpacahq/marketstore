package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/alpacahq/marketstore/v4/plugins/bgworker"
)

func getConfig(data string) (ret map[string]interface{}, err error) {
	err = json.Unmarshal([]byte(data), &ret)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func TestNew(t *testing.T) {
	t.Parallel()
	config, err := getConfig(`{
        "symbols": ["BTC-USD"]
        }`)
	assert.Nil(t, err)
	var worker *GdaxFetcher
	var ret bgworker.BgWorker
	ret, err = NewBgWorker(config)
	worker, ok := ret.(*GdaxFetcher)
	assert.True(t, ok)
	assert.Len(t, worker.symbols, 1)
	assert.Equal(t, worker.symbols[0], "BTC-USD")
	assert.Nil(t, err)

	config, err = getConfig(`{
        "symbols": ["BTC-USD", "ETH-USD", "LTC-BTC"]
        }`)
	assert.Nil(t, err)
	ret, err = NewBgWorker(config)
	assert.Nil(t, err)
	worker, ok = ret.(*GdaxFetcher)
	assert.True(t, ok)
	assert.Len(t, worker.symbols, 3)

	config, err = getConfig(`{
        "query_start": "2017-01-02 00:00"
        }`)
	assert.Nil(t, err)
	ret, err = NewBgWorker(config)
	worker, ok = ret.(*GdaxFetcher)
	assert.True(t, ok)
	assert.Nil(t, err)
	assert.False(t, worker.queryStart.IsZero())
}
