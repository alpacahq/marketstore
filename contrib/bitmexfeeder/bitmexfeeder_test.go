package main

import (
	"encoding/json"
	"fmt"
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
        "symbols": ["XBTUSD"]
        }`)
	var worker *BitmexFetcher
	var ret bgworker.BgWorker
	var err error
	ret, err = NewBgWorker(config)
	worker = ret.(*BitmexFetcher)
	c.Assert(len(worker.symbols), Equals, 1)
	c.Assert(worker.symbols[0], Equals, "XBTUSD")
	c.Assert(err, IsNil)

	config = getConfig(``)
	ret, err = NewBgWorker(config)
	worker = ret.(*BitmexFetcher)
	c.Assert(err, IsNil)
	c.Assert(len(worker.symbols), Equals, 28)

	config = getConfig(`{
	    "query_start": "2017-01-02 00:00"
		}`)
	ret, err = NewBgWorker(config)
	if err != nil {
		fmt.Println(err)
	}
	worker = ret.(*BitmexFetcher)
	fmt.Printf("%v", worker)
	c.Assert(err, IsNil)
	c.Assert(worker.queryStart.IsZero(), Equals, false)
}
