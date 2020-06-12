package trigger

import (
	"reflect"
	"testing"
	"time"

	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
	. "gopkg.in/check.v1"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type TestSuite struct {
}

var _ = Suite(&TestSuite{})

func (s *TestSuite) SetUpSuite(c *C) {}

func (s *TestSuite) TearDownSuite(c *C) {}

type EmptyTrigger struct{}

func (t *EmptyTrigger) Fire(keyPath string, records []Record) {
	// do nothing
}

func (s *TestSuite) TestMatch(c *C) {
	trig := &EmptyTrigger{}
	matcher := NewMatcher(trig, "*/1Min/OHLC")
	var matched bool
	matched = matcher.Match("TSLA/1Min/OHLC")
	c.Check(matched, Equals, true)
	matched = matcher.Match("TSLA/5Min/OHLC")
	c.Check(matched, Equals, false)
}

func (s *TestSuite) TestRecordsToColumnSeries(c *C) {
	epoch := []int64{
		time.Date(2017, 12, 14, 10, 3, 0, 0, utils.InstanceConfig.Timezone).Unix(),
		time.Date(2017, 12, 14, 10, 4, 0, 0, utils.InstanceConfig.Timezone).Unix(),
		time.Date(2017, 12, 14, 10, 5, 0, 0, utils.InstanceConfig.Timezone).Unix(),
		time.Date(2017, 12, 14, 10, 6, 0, 0, utils.InstanceConfig.Timezone).Unix(),
		time.Date(2017, 12, 14, 10, 10, 0, 0, utils.InstanceConfig.Timezone).Unix(),
		time.Date(2017, 12, 15, 10, 3, 0, 0, utils.InstanceConfig.Timezone).Unix(),
		time.Date(2017, 12, 15, 10, 4, 0, 0, utils.InstanceConfig.Timezone).Unix(),
		time.Date(2017, 12, 15, 10, 5, 0, 0, utils.InstanceConfig.Timezone).Unix(),
		time.Date(2017, 12, 15, 10, 6, 0, 0, utils.InstanceConfig.Timezone).Unix(),
		time.Date(2017, 12, 15, 10, 10, 0, 0, utils.InstanceConfig.Timezone).Unix(),
	}
	open := []float32{1., 2., 3., 4., 5., 1., 2., 3., 4., 5.}
	high := []float32{1.1, 2.1, 3.1, 4.1, 5.1, 1.1, 2.1, 3.1, 4.1, 5.1}
	low := []float32{0.9, 1.9, 2.9, 3.9, 4.9, 0.9, 1.9, 2.9, 3.9, 4.9}
	close := []float32{1.05, 2.05, 3.05, 4.05, 5.05, 1.05, 2.05, 3.05, 4.05, 5.05}

	tbk := io.NewTimeBucketKey("TEST/1Min/OHLC")
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", epoch)
	cs.AddColumn("Open", open)
	cs.AddColumn("High", high)
	cs.AddColumn("Low", low)
	cs.AddColumn("Close", close)

	rs := cs.ToRowSeries(*tbk, true)
	rowData := rs.GetData()
	times := rs.GetTime()
	numRows := len(times)
	rowLen := len(rowData) / numRows

	records := make([]Record, numRows)

	for i := 0; i < numRows; i++ {
		pos := i * rowLen
		record := rowData[pos : pos+rowLen]
		index := io.TimeToIndex(times[i], time.Minute)

		buf, _ := io.Serialize(nil, index)
		buf = append(buf, record[8:]...)

		records[i] = Record(buf)
	}

	testCS := RecordsToColumnSeries(
		*tbk, cs.GetDataShapes(),
		cs.GetCandleAttributes(),
		time.Minute, int16(2017),
		records)

	for name, col := range cs.GetColumns() {
		testCol := testCS.GetByName(name)

		cV := reflect.ValueOf(col)
		tcV := reflect.ValueOf(testCol)

		c.Check(cV.Len(), Equals, tcV.Len())
	}

	c.Check(len(cs.GetEpoch()), Equals, len(testCS.GetEpoch()))
	for i := 0; i < len(epoch); i++ {
		c.Check(cs.GetEpoch()[i], Equals, testCS.GetEpoch()[i])
	}
}
