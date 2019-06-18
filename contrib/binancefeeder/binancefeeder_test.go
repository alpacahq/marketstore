package main

import (
	"encoding/json"
	"testing"

	"github.com/alpacahq/marketstore/plugins/bgworker"
	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

var _ = Suite(&TestSuite{})

type TestSuite struct{}

func getConfig(data string) (ret map[string]interface{}) {
	json.Unmarshal([]byte(data), &ret)
	return
}

func (t *TestSuite) TestNew(c *C) {
	var config = getConfig(`{
		"symbols": ["ETH"],
		"base_currencies": ["USDT", "BTC"]
        }`)
	var worker *BinanceFetcher
	var ret bgworker.BgWorker
	var err error
	ret, err = NewBgWorker(config)
	worker = ret.(*BinanceFetcher)
	c.Assert(len(worker.symbols), Equals, 1)
	c.Assert(worker.symbols[0], Equals, "ETH")
	c.Assert(len(worker.baseCurrencies), Equals, 2)
	c.Assert(worker.baseCurrencies[0], Equals, "USDT")
	c.Assert(worker.baseCurrencies[1], Equals, "BTC")
	c.Assert(err, IsNil)

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
	c.Assert(err, IsNil)
	c.Assert(worker.queryStart.IsZero(), Equals, false)
}
