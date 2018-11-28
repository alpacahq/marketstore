package tickcandler

import (
	"testing"

	. "gopkg.in/check.v1"

	"reflect"
	"time"

	. "github.com/alpacahq/marketstore/catalog"
	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/planner"
	"github.com/alpacahq/marketstore/utils"
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
	s.ItemsWritten = MakeDummyCurrencyDir(s.Rootdir, false, false)
	executor.NewInstanceSetup(s.Rootdir, true, true, false, true) // WAL Bypass
	s.DataDirectory = executor.ThisInstance.CatalogDir
	s.WALFile = executor.ThisInstance.WALFile
}

func (s *TestSuite) TearDownSuite(c *C) {
	CleanupDummyDataDir(s.Rootdir)
}

func (s *TestSuite) TestTickCandler(c *C) {
	dd := executor.ThisInstance.CatalogDir

	cdl, am := TickCandler{}.New()
	ds := io.NewDataShapeVector([]string{"Bid", "Ask"}, []io.EnumElementType{io.FLOAT32, io.FLOAT32})
	// Sum and Avg are optional inputs, let's map them arbitrarily
	//am.MapInputColumn("Sum", ds[1:])
	am.MapRequiredColumn("Sum", ds...)
	am.MapRequiredColumn("Avg", ds...)
	err := cdl.Init("1Min")
	c.Assert(err != nil, Equals, true)
	am.MapRequiredColumn("CandlePrice", ds...)
	err = cdl.Init("1Min")
	c.Assert(err == nil, Equals, true)
	/*
		We expect an error with an empty input arg set
	*/
	err = cdl.Accum(&io.Rows{})
	c.Assert(err != nil, Equals, true)

	/*
		Create some tick data with symbol "TEST"
	*/
	createTickBucket("TEST")

	/*
		Read some tick data
	*/
	q := planner.NewQuery(dd)
	q.AddRestriction("Symbol", "TEST")
	q.AddRestriction("AttributeGroup", "TICK")
	q.AddRestriction("Timeframe", "1Min")
	q.SetStart(time.Date(2016, time.November, 1, 12, 0, 0, 0, time.UTC).Unix())
	parsed, _ := q.Parse()
	reader, err := executor.NewReader(parsed)
	c.Assert(err == nil, Equals, true)
	csm, err := reader.Read()
	c.Assert(err == nil, Equals, true)
	c.Assert(len(csm), Equals, 1)
	for _, cs := range csm {
		c.Assert(cs.Len(), Equals, 200)
		err = cdl.Accum(cs)
		c.Assert(err == nil, Equals, true)
	}
	rows := cdl.Output()
	c.Assert(rows.Len(), Equals, 4)
	tsa := rows.GetTime()
	tbase := time.Date(2016, time.December, 31, 2, 59, 0, 0, time.UTC)
	c.Assert(tsa[0] == tbase, Equals, true)
	c.Assert(reflect.DeepEqual(rows.GetColumn("Ask_AVG"), []float64{200, 200, 200, 200}), Equals, true)
	/*
		fmt.Println("Ask_SUM", rows.GetColumn("Ask_SUM"))
		fmt.Println("Bid_SUM", rows.GetColumn("Bid_SUM"))
		fmt.Println("Ask_AVG", rows.GetColumn("Ask_AVG"))
		fmt.Println("Bid_AVG", rows.GetColumn("Bid_AVG"))
	*/

	/*
		Test Reset()
	*/
	cdl.Reset()
	for _, cs := range csm {
		c.Assert(cs.Len(), Equals, 200)
		err = cdl.Accum(cs)
		c.Assert(err == nil, Equals, true)
	}
	rows = cdl.Output()
	c.Assert(rows.Len(), Equals, 4)
	tsa = rows.GetTime()
	tbase = time.Date(2016, time.December, 31, 2, 59, 0, 0, time.UTC)
	c.Assert(tsa[0] == tbase, Equals, true)
	c.Assert(reflect.DeepEqual(rows.GetColumn("Ask_AVG"), []float64{200, 200, 200, 200}), Equals, true)
}

/*
Utility functions
*/
func createTickBucket(symbol string) {
	dd := executor.ThisInstance.CatalogDir
	rd := executor.ThisInstance.RootDir
	tgc := executor.ThisInstance.TXNPipe
	wf := executor.ThisInstance.WALFile

	// Create a new variable data bucket
	tbk := io.NewTimeBucketKey(symbol + "/1Min/TICK")
	tf := utils.NewTimeframe("1Min")
	eTypes := []io.EnumElementType{io.FLOAT32, io.FLOAT32}
	eNames := []string{"Bid", "Ask"}
	dsv := io.NewDataShapeVector(eNames, eTypes)
	tbinfo := io.NewTimeBucketInfo(*tf, tbk.GetPathToYearFiles(rd), "Test", int16(2016), dsv, io.VARIABLE)
	executor.ThisInstance.CatalogDir.AddTimeBucket(tbk, tbinfo)

	/*
		Write some data
	*/
	w, err := executor.NewWriter(tbinfo, tgc, dd)
	if err != nil {
		panic(err)
	}
	row := struct {
		Epoch    int64
		Bid, Ask float32
	}{0, 100, 200}
	ts := time.Date(2016, time.December, 31, 2, 59, 18, 0, time.UTC)
	for ii := 0; ii < 200; ii++ {
		ts = ts.Add(time.Second)
		row.Epoch = ts.Unix()
		buffer, _ := io.Serialize([]byte{}, row)
		w.WriteRecords([]time.Time{ts}, buffer)
	}
	wf.RequestFlush()
}
