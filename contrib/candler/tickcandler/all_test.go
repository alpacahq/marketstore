package tickcandler_test

import (
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	"github.com/alpacahq/marketstore/v4/catalog"
	"github.com/alpacahq/marketstore/v4/utils/test"

	"github.com/alpacahq/marketstore/v4/contrib/candler/tickcandler"
	"github.com/stretchr/testify/assert"

	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/planner"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

func setup(t *testing.T, testName string,
) (tearDown func(), rootDir string, itemsWritten map[string]int, metadata *executor.InstanceMetadata) {
	t.Helper()

	rootDir, _ = ioutil.TempDir("", fmt.Sprintf("tickcandler_test-%s", testName))
	itemsWritten = test.MakeDummyCurrencyDir(rootDir, true, false)
	metadata, _, _ = executor.NewInstanceSetup(rootDir, nil, nil, 5, true, true, false)

	return func() { test.CleanupDummyDataDir(rootDir) }, rootDir, itemsWritten, metadata
}

func TestTickCandler(t *testing.T) {
	tearDown, rootDir, _, metadata := setup(t, "TestTickCandler")
	defer tearDown()

	cdl, am := tickcandler.TickCandler{}.New()
	ds := io.NewDataShapeVector([]string{"Bid", "Ask"}, []io.EnumElementType{io.FLOAT32, io.FLOAT32})
	// Sum and Avg are optional inputs, let's map them arbitrarily
	//am.MapInputColumn("Sum", ds[1:])
	am.MapRequiredColumn("Sum", ds...)
	am.MapRequiredColumn("Avg", ds...)
	err := cdl.Init("1Min")
	assert.NotNil(t, err)
	am.MapRequiredColumn("CandlePrice", ds...)
	err = cdl.Init("1Min")
	assert.Nil(t, err)
	/*
		We expect an error with an empty input arg set
	*/
	err = cdl.Accum(&io.Rows{}, metadata.CatalogDir)
	assert.NotNil(t, err)

	/*
		Create some tick data with symbol "TEST"
	*/
	createTickBucket("TEST", rootDir, metadata.CatalogDir, metadata.WALFile)

	/*
		Read some tick data
	*/
	q := planner.NewQuery(metadata.CatalogDir)
	q.AddRestriction("Symbol", "TEST")
	q.AddRestriction("AttributeGroup", "TICK")
	q.AddRestriction("Timeframe", "1Min")
	q.SetStart(time.Date(2016, time.November, 1, 12, 0, 0, 0, time.UTC))
	parsed, _ := q.Parse()
	reader, err := executor.NewReader(parsed)
	assert.Nil(t, err)
	csm, err := reader.Read()
	assert.Nil(t, err)
	assert.Len(t, csm, 1)
	for _, cs := range csm {
		assert.Equal(t, cs.Len(), 200)
		err = cdl.Accum(cs, metadata.CatalogDir)
		assert.Nil(t, err)
	}
	rows := cdl.Output()
	assert.Equal(t, rows.Len(), 4)
	tsa, err := rows.GetTime()
	tbase := time.Date(2016, time.December, 31, 2, 59, 0, 0, time.UTC)
	assert.Equal(t, tsa[0], tbase)
	assert.Equal(t, rows.GetColumn("Ask_AVG"), []float64{200, 200, 200, 200})
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
		assert.Equal(t, cs.Len(), 200)
		err = cdl.Accum(cs, metadata.CatalogDir)
		assert.Nil(t, err)
	}
	rows = cdl.Output()
	assert.Equal(t, rows.Len(), 4)
	tsa, err = rows.GetTime()
	tbase = time.Date(2016, time.December, 31, 2, 59, 0, 0, time.UTC)
	assert.Equal(t, tsa[0], tbase)
	assert.Equal(t, rows.GetColumn("Ask_AVG"), []float64{200, 200, 200, 200})
}

/*
Utility functions
*/
func createTickBucket(symbol, rootDir string, catalogDir *catalog.Directory, wf *executor.WALFileType) {

	// Create a new variable data bucket
	tbk := io.NewTimeBucketKey(symbol + "/1Min/TICK")
	tf := utils.NewTimeframe("1Min")
	eTypes := []io.EnumElementType{io.FLOAT32, io.FLOAT32}
	eNames := []string{"Bid", "Ask"}
	dsv := io.NewDataShapeVector(eNames, eTypes)
	tbinfo := io.NewTimeBucketInfo(*tf, tbk.GetPathToYearFiles(rootDir), "Test", int16(2016), dsv, io.VARIABLE)
	catalogDir.AddTimeBucket(tbk, tbinfo)

	/*
		Write some data
	*/
	w, err := executor.NewWriter(catalogDir, wf)
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
		w.WriteRecords([]time.Time{ts}, buffer, dsv, tbinfo)
	}
	wf.RequestFlush()
}
