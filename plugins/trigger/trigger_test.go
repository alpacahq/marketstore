package trigger_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/alpacahq/marketstore/v4/plugins/trigger"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

type EmptyTrigger struct{}

func (t *EmptyTrigger) Fire(keyPath string, records []trigger.Record) {
	// do nothing
}

func TestMatch(t *testing.T) {
	trig := &EmptyTrigger{}
	matcher := trigger.NewMatcher(trig, "*/1Min/OHLC")
	var matched bool
	matched = matcher.Match("TSLA/1Min/OHLC")
	assert.True(t, matched)
	matched = matcher.Match("TSLA/5Min/OHLC")
	assert.False(t, matched)
}

func TestRecordsToColumnSeries(t *testing.T) {
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
	clos := []float32{1.05, 2.05, 3.05, 4.05, 5.05, 1.05, 2.05, 3.05, 4.05, 5.05}

	tbk := io.NewTimeBucketKey("TEST/1Min/OHLC")
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", epoch)
	cs.AddColumn("Open", open)
	cs.AddColumn("High", high)
	cs.AddColumn("Low", low)
	cs.AddColumn("Close", clos)

	rs, err := cs.ToRowSeries(*tbk, true)
	assert.Nil(t, err)
	rowData := rs.GetData()
	times, _ := rs.GetTime()
	numRows := len(times)
	rowLen := len(rowData) / numRows

	records := make([]trigger.Record, numRows)

	for i := 0; i < numRows; i++ {
		pos := i * rowLen
		record := rowData[pos : pos+rowLen]
		index := io.TimeToIndex(times[i], time.Minute)

		buf, _ := io.Serialize(nil, index)
		buf = append(buf, record[8:]...)

		records[i] = trigger.Record(buf)
	}

	testCS, err := trigger.RecordsToColumnSeries(
		*tbk, cs.GetDataShapes(),
		time.Minute, int16(2017),
		records)
	assert.Nil(t, err)

	for name, col := range cs.GetColumns() {
		testCol := testCS.GetColumn(name)

		cV := reflect.ValueOf(col)
		tcV := reflect.ValueOf(testCol)

		assert.Equal(t, cV.Len(), tcV.Len())
	}

	assert.Equal(t, len(cs.GetEpoch()), len(testCS.GetEpoch()))
	for i := 0; i < len(epoch); i++ {
		assert.Equal(t, cs.GetEpoch()[i], testCS.GetEpoch()[i])
	}
}
