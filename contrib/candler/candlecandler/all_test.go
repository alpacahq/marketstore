package candlecandler_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/alpacahq/marketstore/v4/internal/di"
	"github.com/alpacahq/marketstore/v4/utils"

	"github.com/stretchr/testify/assert"

	"github.com/alpacahq/marketstore/v4/contrib/candler/candlecandler"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/planner"
	"github.com/alpacahq/marketstore/v4/utils/functions"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/test"
)

func setup(t *testing.T) (rootDir string, itemsWritten map[string]int, metadata *executor.InstanceMetadata) {
	t.Helper()

	rootDir = t.TempDir()
	itemsWritten = test.MakeDummyStockDir(rootDir, true, false)
	cfg := utils.NewDefaultConfig(rootDir)
	c := di.NewContainer(cfg)
	metadata = executor.NewInstanceSetup(c.GetCatalogDir(), c.GetInitWALFile())

	return rootDir, itemsWritten, metadata
}

func TestCandleCandler(t *testing.T) {
	_, _, metadata := setup(t)

	c := candlecandler.CandleCandler{}
	am := functions.NewArgumentMap(c.GetRequiredArgs(), c.GetOptionalArgs()...)
	if unmapped := am.Validate(); unmapped != nil {
		t.Fatalf("unmapped columns: %s", unmapped)
	}
	ca := candlecandler.CandleCandler{}
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
	cdl, err := ca.New(am, "5Min")
	assert.Nil(t, err)

	// Test data range query - across year
	tbk := io.NewTimeBucketKey("OHLCV/AAPL/1Min")
	q := planner.NewQuery(metadata.CatalogDir)
	q.AddRestriction("AttributeGroup", "OHLCV")
	q.AddRestriction("Symbol", "AAPL")
	q.AddRestriction("Timeframe", "1Min")
	startDate := time.Date(2001, time.October, 15, 12, 0, 0, 0, time.UTC)
	endDate := time.Date(2001, time.October, 15, 12, 15, 0, 0, time.UTC)
	q.SetRange(startDate, endDate)
	parsed, err := q.Parse()
	assert.Nil(t, err)
	scanner, err := executor.NewReader(parsed)
	assert.Nil(t, err)
	csm, _ := scanner.Read()
	var output *io.ColumnSeries
	for _, cs := range csm {
		epoch := cs.GetEpoch()
		assert.Equal(t, time.Unix(epoch[0], 0).UTC(), startDate)
		assert.Equal(t, time.Unix(epoch[len(epoch)-1], 0).UTC(), endDate)
		output, err = cdl.Accum(*tbk, am, cs)
		assert.Nil(t, err)
	}
	assert.Equal(t, output.Len(), 4)
	vsum := output.GetColumn("Volume_SUM")
	vavg := output.GetColumn("Volume_AVG")
	/*
		There should be four 5Min candles in the interval 12:00 -> 12:15
		12:00, 12:05, 12:10 and 12:15
		Note that the 12:15 candle is incomplete, it is created from
		a single 1Min candle 12:15->12:16
	*/
	// Sum of volume and avg of volume
	cmpsum := []float64{2070015, 2070040, 2070065, 414016}
	cmpavg := []float64{414003, 414008, 414013, 414016}
	assert.True(t, reflect.DeepEqual(cmpsum, vsum))
	assert.True(t, reflect.DeepEqual(cmpavg, vavg))
}
