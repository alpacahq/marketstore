package main

import (
	"encoding/json"
	"net/http"
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
        "symbols": ["BTC-USD"]
        }`)
	var worker *GdaxFetcher
	var ret bgworker.BgWorker
	var err error
	ret, err = NewBgWorker(config)
	worker = ret.(*GdaxFetcher)
	c.Assert(len(worker.symbols), Equals, 1)
	c.Assert(worker.symbols[0], Equals, "BTC-USD")
	c.Assert(err, IsNil)

	config = getConfig(``)
	ret, err = NewBgWorker(config)
	c.Assert(err, IsNil)
	worker = ret.(*GdaxFetcher)
	if resp, err := http.Get("https://api.pro.coinbase.com/products"); err != nil {
		c.Fatalf("Unable to connect to GDAX API: %v", err)
	} else {
		defer resp.Body.Close()
		var products []interface{}
		if err := json.NewDecoder(resp.Body).Decode(&products); err != nil {
			c.Fatalf("Unable to decode json form GDAX /products API: %v", err)
		}
		c.Assert(len(worker.symbols), Equals, len(products))
	}

	config = getConfig(`{
        "query_start": "2017-01-02 00:00"
        }`)
	ret, err = NewBgWorker(config)
	worker = ret.(*GdaxFetcher)
	c.Assert(err, IsNil)
	c.Assert(worker.queryStart.IsZero(), Equals, false)
}
