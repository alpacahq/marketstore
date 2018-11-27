package candlecandler

import (
	"testing"

	. "gopkg.in/check.v1"

	"fmt"
	"reflect"
	"time"

	. "github.com/alpacahq/marketstore/catalog"
	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/planner"
	"github.com/alpacahq/marketstore/utils/io"
	. "github.com/alpacahq/marketstore/utils/test"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

var _ = Suite(&TestSuite{nil, "", nil, nil})

type TestSuite struct {
	DataDirectory *Directory
	Rootdir       string
	// Number of items written in sample data (non-zero index)
	ItemsWritten map[string]int
	WALFile      *executor.WALFileType
}

func (s *TestSuite) SetUpSuite(c *C) {
	s.Rootdir = c.MkDir()
	s.ItemsWritten = MakeDummyStockDir(s.Rootdir, true, false)
	executor.NewInstanceSetup(s.Rootdir, true, true, false, true) // WAL Bypass
	s.DataDirectory = executor.ThisInstance.CatalogDir
	s.WALFile = executor.ThisInstance.WALFile
}

func (s *TestSuite) TearDownSuite(c *C) {
	CleanupDummyDataDir(s.Rootdir)
}

func (s *TestSuite) TestCandleCandler(c *C) {
	cdl, am := CandleCandler{}.New()
	ds := io.NewDataShapeVector(
		[]string{"Open", "High", "Low", "Close", "Volume"},
		[]io.EnumElementType{io.FLOAT32, io.FLOAT32, io.FLOAT32, io.FLOAT32, io.INT32},
	)
	// Sum and Avg are optional inputs, let's map them to Volume
	am.MapRequiredColumn("Sum", ds[4])
	am.MapRequiredColumn("Avg", ds[4])
	am.MapRequiredColumn("Open", ds[0])
	am.MapRequiredColumn("High", ds[1])
	am.MapRequiredColumn("Low", ds[2])
	am.MapRequiredColumn("Close", ds[3])
	err := cdl.Init("5Min")
	c.Assert(err == nil, Equals, true)

	// Test data range query - across year
	q := planner.NewQuery(s.DataDirectory)
	q.AddRestriction("AttributeGroup", "OHLCV")
	q.AddRestriction("Symbol", "AAPL")
	q.AddRestriction("Timeframe", "1Min")
	startDate := time.Date(2001, time.October, 15, 12, 0, 0, 0, time.UTC)
	endDate := time.Date(2001, time.October, 15, 12, 15, 0, 0, time.UTC)
	q.SetRange(startDate.Unix(), endDate.Unix())
	parsed, _ := q.Parse()
	scanner, err := executor.NewReader(parsed)
	c.Assert(err == nil, Equals, true)
	csm, _ := scanner.Read()
	for _, cs := range csm {
		epoch := cs.GetEpoch()
		c.Assert(time.Unix(epoch[0], 0).UTC(), Equals, startDate)
		c.Assert(time.Unix(epoch[len(epoch)-1], 0).UTC(), Equals, endDate)
		err = cdl.Accum(cs)
		c.Assert(err == nil, Equals, true)
	}
	cols := cdl.Output()
	c.Assert(cols.Len(), Equals, 4)
	vsum := cols.GetColumn("Volume_SUM")
	vavg := cols.GetColumn("Volume_AVG")
	/*
		There should be four 5Min candles in the interval 12:00 -> 12:15
		12:00, 12:05, 12:10 and 12:15
		Note that the 12:15 candle is incomplete, it is created from
		a single 1Min candle 12:15->12:16
	*/
	// Sum of volume and avg of volume
	cmpsum := []float64{2070015, 2070040, 2070065, 414016}
	cmpavg := []float64{414003, 414008, 414013, 414016}
	c.Assert(reflect.DeepEqual(cmpsum, vsum), Equals, true)
	c.Assert(reflect.DeepEqual(cmpavg, vavg), Equals, true)
}

/*
Utility functions
*/
func printCandles(cols io.ColumnInterface) {
	fmt.Println(cols.GetTime())
	fmt.Println(cols.GetColumn("Open"))
	fmt.Println(cols.GetColumn("High"))
	fmt.Println(cols.GetColumn("Low"))
	fmt.Println(cols.GetColumn("Close"))
	fmt.Println(cols.GetColumn("Volume_SUM"))
	fmt.Println(cols.GetColumn("Volume_AVG"))
}
