package aggtrigger

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/alpacahq/marketstore/contrib/quanatee_trigger_without_underscore"

	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/planner"
	"github.com/alpacahq/marketstore/utils"
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

	utils.InstanceConfig.Timezone, _ = time.LoadLocation("America/New_York")

	epoch = []int64{
		time.Date(2017, 12, 15, 10, 3, 0, 0, utils.InstanceConfig.Timezone).Unix(),
		time.Date(2017, 12, 15, 10, 4, 0, 0, utils.InstanceConfig.Timezone).Unix(),
		time.Date(2017, 12, 16, 10, 5, 0, 0, utils.InstanceConfig.Timezone).Unix(),
		time.Date(2017, 12, 16, 10, 6, 0, 0, utils.InstanceConfig.Timezone).Unix(),
		time.Date(2017, 12, 16, 10, 10, 0, 0, utils.InstanceConfig.Timezone).Unix(),
	}

	tbk = io.NewTimeBucketKey("TEST/1D/OHLC")
	cs = io.NewColumnSeries()
	cs.AddColumn("Epoch", epoch)
	cs.AddColumn("Open", open)
	cs.AddColumn("High", high)
	cs.AddColumn("Low", low)
	cs.AddColumn("Close", close)

	outCs = aggregate(cs, tbk)
	c.Assert(outCs.Len(), Equals, 2)
	d1 := time.Date(2017, 12, 15, 0, 0, 0, 0, utils.InstanceConfig.Timezone)
	d2 := time.Date(2017, 12, 16, 0, 0, 0, 0, utils.InstanceConfig.Timezone)
	c.Assert(outCs.GetEpoch()[0], Equals, d1.Unix())
	c.Assert(outCs.GetEpoch()[1], Equals, d2.Unix())
}

func (t *TestSuite) TestFire(c *C) {
	// We assume WriteCSM here is synchronous by not running
	// background writer
	utils.InstanceConfig.Timezone, _ = time.LoadLocation("America/New_York")

	rootDir := filepath.Join(c.MkDir(), "mktsdb")
	os.MkdirAll(rootDir, 0777)
	executor.NewInstanceSetup(
		rootDir,
		true, true, false, false)

	ts := utils.TriggerSetting{
		Module: "ondiskagg.so",
		On:     "*/1Min/OHLC",
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
	close := []float32{1.05, 2.05, 3.05, 4.05, 5.05, 1.05, 2.05, 3.05, 4.05, 5.05}

	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", epoch)
	cs.AddColumn("Open", open)
	cs.AddColumn("High", high)
	cs.AddColumn("Low", low)
	cs.AddColumn("Close", close)
	tbk := io.NewTimeBucketKey("TEST/1Min/OHLC")
	csm := io.NewColumnSeriesMap()
	csm.AddColumnSeries(*tbk, cs)
	err = executor.WriteCSM(csm, false)
	c.Assert(err, IsNil)

	rs := cs.ToRowSeries(*tbk, true)
	rowData := rs.GetData()
	times := rs.GetTime()
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
	q.SetRange(planner.MinEpoch, planner.MaxEpoch)
	parsed, err := q.Parse()
	c.Check(err, IsNil)
	scanner, err := executor.NewReader(parsed)
	c.Check(err, IsNil)
	csm5, err := scanner.Read()
	c.Check(err, IsNil)
	cs5 := csm5[*tbk5]
	c.Check(cs5, NotNil)
	c.Check(cs5.Len(), Equals, 6)

	// verify 1D agg
	tbk1D := io.NewTimeBucketKey("TEST/1D/OHLC")
	q = planner.NewQuery(catalogDir)
	q.AddTargetKey(tbk1D)
	q.SetRange(planner.MinEpoch, planner.MaxEpoch)
	parsed, err = q.Parse()
	c.Check(err, IsNil)
	scanner, err = executor.NewReader(parsed)
	c.Check(err, IsNil)
	csm1D, err := scanner.Read()
	c.Check(err, IsNil)
	cs1D := csm1D[*tbk1D]
	c.Check(cs1D, NotNil)
	c.Check(cs1D.Len(), Equals, 2)
	t1 := time.Unix(cs1D.GetEpoch()[0], 0).In(utils.InstanceConfig.Timezone)
	c.Assert(t1.Equal(time.Date(2017, 12, 14, 0, 0, 0, 0, utils.InstanceConfig.Timezone)), Equals, true)
	t2 := time.Unix(cs1D.GetEpoch()[1], 0).In(utils.InstanceConfig.Timezone)
	c.Assert(t2.Equal(time.Date(2017, 12, 15, 0, 0, 0, 0, utils.InstanceConfig.Timezone)), Equals, true)
}
