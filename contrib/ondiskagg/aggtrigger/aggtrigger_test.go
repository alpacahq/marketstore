package aggtrigger

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/planner"
	"github.com/alpacahq/marketstore/v4/plugins/trigger"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

func getConfig(data string) (ret map[string]interface{}) {
	json.Unmarshal([]byte(data), &ret)
	return
}

func TestNew(t *testing.T) {
	t.Parallel()
	config := getConfig(`{
        "destinations": ["5Min", "1D"],
        "filter": "something"
        }`)
	ret, err := NewTrigger(config)
	trig := ret.(*OnDiskAggTrigger)
	assert.Len(t, trig.destinations, 2)
	assert.Equal(t, trig.filter, "")
	assert.Nil(t, err)

	// missing destinations
	config = getConfig(`{}`)
	ret, err = NewTrigger(config)
	assert.Nil(t, ret)
	assert.NotNil(t, err)
}

func TestAgg(t *testing.T) {
	t.Parallel()
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

	baseTbk := io.NewTimeBucketKey("TEST/1Min/OHLCV")
	aggTbk := io.NewTimeBucketKey("TEST/5Min/OHLC")
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", epoch)
	cs.AddColumn("Open", open)
	cs.AddColumn("High", high)
	cs.AddColumn("Low", low)
	cs.AddColumn("Close", close)

	outCs, err := aggregate(cs, aggTbk, baseTbk, "TEST")
	assert.Nil(t, err)
	assert.Equal(t, outCs.Len(), 3)
	assert.Equal(t, outCs.GetColumn("Open").([]float32)[0], float32(1.))
	assert.Equal(t, outCs.GetColumn("High").([]float32)[1], float32(4.1))
	assert.Equal(t, outCs.GetColumn("Low").([]float32)[0], float32(0.9))
	assert.Equal(t, outCs.GetColumn("Close").([]float32)[1], float32(4.05))

	utils.InstanceConfig.Timezone, _ = time.LoadLocation("America/New_York")

	epoch = []int64{
		time.Date(2017, 12, 15, 10, 3, 0, 0, utils.InstanceConfig.Timezone).Unix(),
		time.Date(2017, 12, 15, 10, 4, 0, 0, utils.InstanceConfig.Timezone).Unix(),
		time.Date(2017, 12, 16, 10, 5, 0, 0, utils.InstanceConfig.Timezone).Unix(),
		time.Date(2017, 12, 16, 10, 6, 0, 0, utils.InstanceConfig.Timezone).Unix(),
		time.Date(2017, 12, 16, 10, 10, 0, 0, utils.InstanceConfig.Timezone).Unix(),
	}

	aggTbk = io.NewTimeBucketKey("TEST/1D/OHLC")
	cs = io.NewColumnSeries()
	cs.AddColumn("Epoch", epoch)
	cs.AddColumn("Open", open)
	cs.AddColumn("High", high)
	cs.AddColumn("Low", low)
	cs.AddColumn("Close", close)

	outCs, err = aggregate(cs, aggTbk, baseTbk, "TEST")
	assert.Nil(t, err)
	assert.Equal(t, outCs.Len(), 2)
	d1 := time.Date(2017, 12, 15, 0, 0, 0, 0, utils.InstanceConfig.Timezone)
	d2 := time.Date(2017, 12, 16, 0, 0, 0, 0, utils.InstanceConfig.Timezone)
	assert.Equal(t, outCs.GetEpoch()[0], d1.Unix())
	assert.Equal(t, outCs.GetEpoch()[1], d2.Unix())
}

func TestFireBars(t *testing.T) {
	t.Parallel()
	// We assume WriteCSM here is synchronous by not running
	// background writer
	utils.InstanceConfig.Timezone, _ = time.LoadLocation("America/New_York")

	tempDir, _ := ioutil.TempDir("", "aggtrigger.TestFireBars")
	defer os.RemoveAll(tempDir)

	rootDir := filepath.Join(tempDir, "mktsdb")
	_ = os.MkdirAll(rootDir, 0o777)
	_, _, _, err := executor.NewInstanceSetup(
		rootDir, nil, nil,
		5, executor.BackgroundSync(false))
	assert.Nil(t, err)

	ts := utils.TriggerSetting{
		// Module: "ondiskagg.so",
		// On:     "*/1Min/OHLC",
		Config: map[string]interface{}{
			"filter":       "nasdaq",
			"destinations": []string{"5Min", "1D"},
		},
	}

	trig, err := NewTrigger(ts.Config)
	if err != nil {
		panic(err)
	}

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

	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", epoch)
	cs.AddColumn("Open", open)
	cs.AddColumn("High", high)
	cs.AddColumn("Low", low)
	cs.AddColumn("Close", clos)
	tbk := io.NewTimeBucketKey("TEST/1Min/OHLC")
	csm := io.NewColumnSeriesMap()
	csm.AddColumnSeries(*tbk, cs)
	err = executor.WriteCSM(csm, false)
	assert.Nil(t, err)

	rs, err := cs.ToRowSeries(*tbk, true)
	assert.Nil(t, err)
	rowData := rs.GetData()
	times, err := rs.GetTime()
	assert.Nil(t, err)
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

	trig.Fire("TEST/1Min/OHLC/2017.bin", records)

	// verify 5Min agg
	catalogDir := executor.ThisInstance.CatalogDir
	q := planner.NewQuery(catalogDir)
	tbk5 := io.NewTimeBucketKey("TEST/5Min/OHLC")
	q.AddTargetKey(tbk5)
	q.SetRange(planner.MinTime, planner.MaxTime)
	parsed, err := q.Parse()
	assert.Nil(t, err)
	scanner, err := executor.NewReader(parsed)
	assert.Nil(t, err)
	csm5, err := scanner.Read()
	assert.Nil(t, err)
	cs5 := csm5[*tbk5]
	assert.NotNil(t, cs5)
	assert.Equal(t, cs5.Len(), 6)

	// verify 1D agg
	tbk1D := io.NewTimeBucketKey("TEST/1D/OHLC")
	q = planner.NewQuery(catalogDir)
	q.AddTargetKey(tbk1D)
	q.SetRange(planner.MinTime, planner.MaxTime)
	parsed, err = q.Parse()
	assert.Nil(t, err)
	scanner, err = executor.NewReader(parsed)
	assert.Nil(t, err)
	csm1D, err := scanner.Read()
	assert.Nil(t, err)
	cs1D := csm1D[*tbk1D]
	assert.NotNil(t, cs1D)
	assert.Equal(t, cs1D.Len(), 2)
	t1 := time.Unix(cs1D.GetEpoch()[0], 0).In(utils.InstanceConfig.Timezone)
	assert.True(t, t1.Equal(time.Date(2017, 12, 14, 0, 0, 0, 0, utils.InstanceConfig.Timezone)))
	t2 := time.Unix(cs1D.GetEpoch()[1], 0).In(utils.InstanceConfig.Timezone)
	assert.True(t, t2.Equal(time.Date(2017, 12, 15, 0, 0, 0, 0, utils.InstanceConfig.Timezone)))
}
