package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/alpacahq/marketstore/utils/io"
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
        "destinations": ["5Min", "1D"],
        "filter": "something"
        }`)
	var ret, err = NewTrigger(config)
	var trig = ret.(*OnDiskAggTrigger)
	c.Assert(len(trig.destinations), Equals, 2)
	c.Assert(trig.filter, Equals, "")
	c.Assert(err, IsNil)

	// missing destinations
	config = getConfig(`{}`)
	ret, err = NewTrigger(config)
	c.Assert(ret, IsNil)
	c.Assert(err, NotNil)
}

func (t *TestSuite) TestAgg(c *C) {
	epoch := []int64{
		time.Date(2017, 12, 15, 10, 3, 0, 0, time.UTC).Unix(),
		time.Date(2017, 12, 15, 10, 4, 0, 0, time.UTC).Unix(),
		time.Date(2017, 12, 15, 10, 5, 0, 0, time.UTC).Unix(),
		time.Date(2017, 12, 15, 10, 6, 0, 0, time.UTC).Unix(),
		time.Date(2017, 12, 15, 10, 10, 0, 0, time.UTC).Unix(),
	}
	open := []float32{1., 2., 3., 4., 5.}
	high := []float32{1.1, 2.1, 3.1, 4.1, 5.1}
	low := []float32{0.9, 1.9, 2.9, 3.9, 4.9}
	close := []float32{1.05, 2.05, 3.05, 4.05, 5.05}

	tbk := io.NewTimeBucketKey("TEST/5Min/OHLC")
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", epoch)
	cs.AddColumn("Open", open)
	cs.AddColumn("High", high)
	cs.AddColumn("Low", low)
	cs.AddColumn("Close", close)

	outCs := aggregate(cs, tbk)
	c.Assert(outCs.Len(), Equals, 3)
	c.Assert(outCs.GetColumn("Open").([]float32)[0], Equals, float32(1.))
	c.Assert(outCs.GetColumn("High").([]float32)[1], Equals, float32(4.1))
	c.Assert(outCs.GetColumn("Low").([]float32)[0], Equals, float32(0.9))
	c.Assert(outCs.GetColumn("Close").([]float32)[1], Equals, float32(4.05))
}
