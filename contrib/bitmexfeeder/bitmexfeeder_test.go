package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	bitmex "github.com/alpacahq/marketstore/v4/contrib/bitmexfeeder/api"
	"github.com/alpacahq/marketstore/v4/plugins/bgworker"
)

func getConfig(t *testing.T, data string) (ret map[string]interface{}) {
	t.Helper()

	if data == "" {
		return nil
	}

	err := json.Unmarshal([]byte(data), &ret)
	require.Nil(t, err)
	return ret
}

func TestNew(t *testing.T) {
	t.Parallel()
	hc := NewTestClient(func(req *http.Request) *http.Response {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewBuffer([]byte(getInstrumentsResponseMock))),
			Header:     make(http.Header),
		}
	})

	config := getConfig(t, `{
        "symbols": ["XBTUSD"]
        }`)
	config["httpClient"] = hc // inject http client
	var worker *BitmexFetcher
	var ret bgworker.BgWorker
	var err error
	ret, err = NewBgWorker(config)
	worker, ok := ret.(*BitmexFetcher)
	assert.True(t, ok)
	assert.Equal(t, 1, len(worker.symbols))
	assert.Equal(t, "XBTUSD", worker.symbols[0])
	assert.Nil(t, err)

	getConfig(t, ``)
	config = map[string]interface{}{"httpClient": hc} // inject http client
	ret, err = NewBgWorker(config)
	worker, ok = ret.(*BitmexFetcher)
	assert.True(t, ok)
	assert.Nil(t, err)

	client := bitmex.NewBitmexClient(hc)
	symbols, err := client.GetInstruments()
	assert.Nil(t, err)
	assert.Equal(t, len(worker.symbols), len(symbols))

	config = getConfig(t, `{
	    "query_start": "2017-01-02 00:00"
		}`)
	config["httpClient"] = hc // inject http client
	ret, err = NewBgWorker(config)
	if err != nil {
		t.Log(err)
	}
	worker, ok = ret.(*BitmexFetcher)
	assert.True(t, ok)
	t.Logf("%v", worker)
	assert.Nil(t, err)
	assert.Equal(t, worker.queryStart.IsZero(), false)
}
