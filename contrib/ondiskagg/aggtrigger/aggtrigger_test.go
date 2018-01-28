package aggtrigger

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/planner"
	"github.com/alpacahq/marketstore/plugins/trigger"
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
}

func (t *TestSuite) TestFire(c *C) {
	// We assume WriteCSM here is synchronous by not running
	// background writer
	utils.InstanceConfig.Timezone = time.UTC
	rootDir := filepath.Join(c.MkDir(), "mktsdb")
	os.MkdirAll(rootDir, 0777)
	executor.NewInstanceSetup(
		rootDir,
		true, true, false, false)
	ts := utils.TriggerSetting{
		Module: "ondiskagg.so",
		On:     "*/1Min/OHLCV",
		Config: map[string]interface{}{
			"filter":       "nasdaq",
			"destinations": []string{"5Min"},
		},
	}
	trig, err := NewTrigger(ts.Config)
	if err != nil {
		panic(err)
	}
	executor.ThisInstance.TriggerMatchers = []*trigger.TriggerMatcher{
		trigger.NewMatcher(trig, ts.On),
	}

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

	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", epoch)
	cs.AddColumn("Open", open)
	cs.AddColumn("High", high)
	cs.AddColumn("Low", low)
	cs.AddColumn("Close", close)
	tbk := io.NewTimeBucketKey("TEST/1Min/OHLC")
	csm := io.NewColumnSeriesMap()
	csm.AddColumnSeries(*tbk, cs)
	executor.WriteCSM(csm, false)

	indexes := make([]int64, 0)
	intervalsPerDay := int64(60 * 24)
	for _, val := range epoch {
		index := io.TimeToIndex(time.Unix(val, 0).UTC(), intervalsPerDay)
		indexes = append(indexes, index)
	}
	trig.Fire("TEST/1Min/OHLC/2017.bin", indexes)

	catalogDir := executor.ThisInstance.CatalogDir
	q := planner.NewQuery(catalogDir)
	tbk5 := io.NewTimeBucketKey("TEST/5Min/OHLC")
	q.AddTargetKey(tbk5)
	q.SetRange(planner.MinTime, planner.MaxTime)
	parsed, err := q.Parse()
	c.Check(err, IsNil)
	scanner, err := executor.NewReader(parsed)
	c.Check(err, IsNil)
	csm5, _, err := scanner.Read()
	c.Check(err, IsNil)
	cs5 := csm5[*tbk5]
	c.Check(cs5, NotNil)
	c.Check(cs5.Len(), Equals, 3)
}
