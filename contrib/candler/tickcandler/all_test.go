package tickcandler_test

import (
	"github.com/alpacahq/marketstore/v4/internal/di"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/catalog"
	"github.com/alpacahq/marketstore/v4/contrib/candler/tickcandler"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/planner"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/functions"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/test"
)

func setup(t *testing.T) (rootDir string, itemsWritten map[string]int, metadata *executor.InstanceMetadata) {
	t.Helper()

	rootDir = t.TempDir()
	itemsWritten = test.MakeDummyCurrencyDir(rootDir, true, false)
	c := di.NewContainer(utils.NewDefaultConfig(rootDir))
	metadata = executor.NewInstanceSetup(c.GetCatalogDir(), c.GetInitWALFile())

	return rootDir, itemsWritten, metadata
}

func TestTickCandler(t *testing.T) {
	rootDir, _, metadata := setup(t)

	tc := tickcandler.TickCandler{}
	am := functions.NewArgumentMap(tc.GetRequiredArgs(), tc.GetOptionalArgs()...)
	ds := io.NewDataShapeVector([]string{"Bid", "Ask"}, []io.EnumElementType{io.FLOAT32, io.FLOAT32})
	// Sum and Avg are optional inputs, let's map them arbitrarily
	// am.MapInputColumn("Sum", ds[1:])
	am.MapRequiredColumn("Sum", ds...)
	am.MapRequiredColumn("Avg", ds...)
	_, err := tc.New(am, "1Min")
	assert.NotNil(t, err)
	am.MapRequiredColumn("CandlePrice", ds...)
	cdl, err := tc.New(am, "1Min")
	assert.Nil(t, err)
	/*
		We expect an error with an empty input arg set
	*/
	_, err = cdl.Accum(io.TimeBucketKey{}, am, &io.Rows{})
	assert.NotNil(t, err)

	/*
		Create some tick data with symbol "TEST"
	*/
	createTickBucket(t, "TEST", rootDir, metadata.CatalogDir, metadata.WALFile)

	/*
		Read some tick data
	*/
	tbk := io.NewTimeBucketKeyFromString("TEST/TICK/1Min")
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
	var rows *io.ColumnSeries
	for _, cs := range csm {
		assert.Equal(t, cs.Len(), 200)
		rows, err = cdl.Accum(*tbk, am, cs)
		assert.Nil(t, err)
	}
	assert.Equal(t, rows.Len(), 4)
	tsa, err := rows.GetTime()
	assert.Nil(t, err)
	tbase := time.Date(2016, time.December, 31, 2, 59, 0, 0, time.UTC)
	assert.Equal(t, tsa[0], tbase)
	assert.Equal(t, rows.GetColumn("Ask_AVG"), []float64{200, 200, 200, 200})
	/*
		t.Log("Ask_SUM", rows.GetColumn("Ask_SUM"))
		t.Log("Bid_SUM", rows.GetColumn("Bid_SUM"))
		t.Log("Ask_AVG", rows.GetColumn("Ask_AVG"))
		t.Log("Bid_AVG", rows.GetColumn("Bid_AVG"))
	*/

	/*
		Test Reset()
	*/
	cdl, err = tc.New(am, "1Min")
	assert.Nil(t, err)
	for _, cs := range csm {
		assert.Equal(t, cs.Len(), 200)
		rows, err = cdl.Accum(*tbk, am, cs)
		assert.Nil(t, err)
	}
	assert.Equal(t, rows.Len(), 4)
	tsa, err = rows.GetTime()
	assert.Nil(t, err)
	tbase = time.Date(2016, time.December, 31, 2, 59, 0, 0, time.UTC)
	assert.Equal(t, tsa[0], tbase)
	assert.Equal(t, rows.GetColumn("Ask_AVG"), []float64{200, 200, 200, 200})
}

/*
Utility functions.
*/
func createTickBucket(t *testing.T, symbol, rootDir string, catalogDir *catalog.Directory, wf *executor.WALFileType) {
	t.Helper()
	// Create a new variable data bucket
	tbk := io.NewTimeBucketKey(symbol + "/1Min/TICK")
	tf := utils.NewTimeframe("1Min")
	eTypes := []io.EnumElementType{io.FLOAT32, io.FLOAT32}
	eNames := []string{"Bid", "Ask"}
	dsv := io.NewDataShapeVector(eNames, eTypes)
	tbinfo := io.NewTimeBucketInfo(*tf, tbk.GetPathToYearFiles(rootDir), "Test", int16(2016), dsv, io.VARIABLE)
	err := catalogDir.AddTimeBucket(tbk, tbinfo)
	require.Nil(t, err)

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
		err = w.WriteRecords([]time.Time{ts}, buffer, dsv, tbinfo)
		require.Nil(t, err)
	}
	wf.RequestFlush()
}
