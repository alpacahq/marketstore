package main

import (
	"encoding/json"
	"math/rand"
	"testing"
	"time"

	"github.com/alpacahq/marketstore/plugins/bgworker"
	"github.com/alpacahq/slait/cache"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

var _ = Suite(&TestSuite{})

type TestSuite struct{}

var cryptoConfig = getConfig(`{
	"endpoint": "localhost:5000",
	"topic": "bars_gdax",
	"partitions": [
		["gdax","5Min"],
		["gdd","*"]
	],
	"attribute_group": "OHLCV",
	"shape": [
		["Epoch", "int64"],
		["Open", "float64"],
		["High", "float64"],
		["Low", "float64"],
		["Close", "float64"],
		["Volume", "float64"]
	]
}`)

var stockConfig = getConfig(`{
	"endpoint": "localhost:5000",
	"topic": "bars_gdax",
	"partitions": [
		["gdax","5Min"],
		["gdd","*"]
	],
	"attribute_group": "OHLCV",
	"shape": [
		["Epoch", "int64"],
		["Open", "float32"],
		["High", "float32"],
		["Low", "float32"],
		["Close", "float32"],
		["Volume", "int32"]
	]
}`)

func (t *TestSuite) TestNew(c *C) {
	var worker *SlaitSubscriber
	var ret bgworker.BgWorker
	var err error
	ret, err = NewBgWorker(cryptoConfig)
	c.Assert(err, IsNil)
	worker = ret.(*SlaitSubscriber)
	c.Assert(worker, NotNil)
	c.Assert(len(worker.shape), Equals, 6)
	c.Assert(worker.attributeGroup, Equals, "OHLCV")
	c.Assert(worker.topic, Equals, "bars_gdax")
	c.Assert(worker.endpoint, Equals, "localhost:5000")
}

type stockBar struct {
	Timestamp time.Time
	Open      float32
	High      float32
	Low       float32
	Close     float32
	Volume    int32
}

func mockStockBar(t time.Time) []byte {
	bar := stockBar{
		t,
		rand.Float32(),
		rand.Float32(),
		rand.Float32(),
		rand.Float32(),
		rand.Int31(),
	}
	buf, _ := json.Marshal(bar)
	return buf
}

type cryptoBar struct {
	Timestamp time.Time
	Open      float64
	High      float64
	Low       float64
	Close     float64
	Volume    float64
}

func mockCryptoBar(t time.Time) []byte {
	buf, _ := json.Marshal(cryptoBar{
		t,
		rand.Float64(),
		rand.Float64(),
		rand.Float64(),
		rand.Float64(),
		rand.Float64(),
	})
	return buf
}

func (t *TestSuite) TestPublicationToCSM(c *C) {
	// homogeneous crypto bar
	ret, _ := NewBgWorker(cryptoConfig)
	cb1 := mockCryptoBar(time.Now().Truncate(time.Minute).Add(-time.Minute))
	cb2 := mockCryptoBar(time.Now().Truncate(time.Minute))
	p := cache.Publication{
		Topic:     "bars_gdax",
		Partition: "BTC-USD",
		Entries: cache.Entries{
			&cache.Entry{
				Timestamp: time.Time{},
				Data:      cb1,
			},
			&cache.Entry{
				Timestamp: time.Time{},
				Data:      cb2,
			},
		},
	}
	ss := ret.(*SlaitSubscriber)
	csm, err := ss.publicationToCSM(p, "1Min")
	c.Assert(err, IsNil)
	c.Assert(csm, NotNil)
	c.Assert(csm.IsEmpty(), Equals, false)
	for _, cs := range csm {
		epoch := cs.GetColumn("Epoch")
		open := cs.GetColumn("Open")
		high := cs.GetColumn("High")
		low := cs.GetColumn("Low")
		close := cs.GetColumn("Close")
		volume := cs.GetColumn("Volume")
		for i, e := range p.Entries {
			cb := cryptoBar{}
			json.Unmarshal(e.Data, &cb)
			c.Assert(cb.Timestamp.Unix(), Equals, epoch.([]int64)[i])
			c.Assert(cb.Open, Equals, open.([]float64)[i])
			c.Assert(cb.High, Equals, high.([]float64)[i])
			c.Assert(cb.Low, Equals, low.([]float64)[i])
			c.Assert(cb.Close, Equals, close.([]float64)[i])
			c.Assert(cb.Volume, Equals, volume.([]float64)[i])
		}
	}

	// heterogenous stock bar
	ret, _ = NewBgWorker(stockConfig)
	sb1 := mockStockBar(time.Now().Truncate(time.Minute).Add(-time.Minute))
	sb2 := mockStockBar(time.Now().Truncate(time.Minute))
	p = cache.Publication{
		Topic:     "bars_bats",
		Partition: "AAPL",
		Entries: cache.Entries{
			&cache.Entry{
				Timestamp: time.Time{},
				Data:      sb1,
			},
			&cache.Entry{
				Timestamp: time.Time{},
				Data:      sb2,
			},
		},
	}
	ss = ret.(*SlaitSubscriber)
	csm, err = ss.publicationToCSM(p, "1Min")
	c.Assert(err, IsNil)
	c.Assert(csm, NotNil)
	c.Assert(csm.IsEmpty(), Equals, false)
	for _, cs := range csm {
		epoch := cs.GetColumn("Epoch")
		open := cs.GetColumn("Open")
		high := cs.GetColumn("High")
		low := cs.GetColumn("Low")
		close := cs.GetColumn("Close")
		volume := cs.GetColumn("Volume")
		for i, e := range p.Entries {
			sb := stockBar{}
			json.Unmarshal(e.Data, &sb)
			c.Assert(sb.Timestamp.Unix(), Equals, epoch.([]int64)[i])
			c.Assert(sb.Open, Equals, open.([]float32)[i])
			c.Assert(sb.High, Equals, high.([]float32)[i])
			c.Assert(sb.Low, Equals, low.([]float32)[i])
			c.Assert(sb.Close, Equals, close.([]float32)[i])
			c.Assert(sb.Volume, Equals, volume.([]int32)[i])
		}
	}

	c.Assert(len(csm.GetMetadataKeys()), Equals, 1)
	c.Assert(csm.IsEmpty(), Equals, false)
}
