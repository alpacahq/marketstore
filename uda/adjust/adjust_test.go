package adjust

import (
	"fmt"
	"io/ioutil"
	"math"
	"testing"
	"time"

	"github.com/alpacahq/marketstore/v4/utils/functions"

	"github.com/stretchr/testify/assert"

	"github.com/alpacahq/marketstore/v4/utils/test"

	"github.com/alpacahq/marketstore/v4/contrib/ice/enum"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

func setup(t *testing.T, testName string,
) (tearDown func(), rootDir string, metadata *executor.InstanceMetadata) {
	t.Helper()

	rounderNum = math.Pow(10, 3)

	rootDir, _ = ioutil.TempDir("", fmt.Sprintf("adjust_test-%s", testName))
	metadata, _, _ = executor.NewInstanceSetup(rootDir, nil, nil, 5, true, true, false, true)

	return func() { test.CleanupDummyDataDir(rootDir) }, rootDir, metadata
}

type price struct {
	epoch int64
	price float64
}

type AdjustTestCase struct {
	description string
	rateChanges []RateChange
	input       []price
	expected    []price
}

func toColumnSeries(inputData []price) *io.ColumnSeries {
	epoch := make([]int64, len(inputData))
	price := make([]float64, len(inputData))
	for i := range inputData {
		epoch[i] = inputData[i].epoch
		price[i] = inputData[i].price
	}
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", epoch)
	cs.AddColumn("Price", price)
	return cs
}

func evalCase(t *testing.T, testCase AdjustTestCase) {
	t.Helper()

	symbol := "AAPL"
	tbkStr := symbol + "/1Min/OHLCV"
	tbk := io.NewTimeBucketKeyFromString(tbkStr)
	adj := Adjust{}
	am := functions.NewArgumentMap(adj.GetRequiredArgs(), adj.GetOptionalArgs()...)

	rateChangeCache[CacheKey{symbol, true, true}] = RateChangeCache{
		Changes:   testCase.rateChanges,
		Access:    0,
		CreatedAt: time.Now(),
	}

	inputCs := toColumnSeries(testCase.input)

	aggfunc, _ := adj.New(am)
	outputCs, _ := aggfunc.Accum(*tbk, am, inputCs)

	outEpochs := outputCs.GetColumn("Epoch").([]int64)
	outPrice := outputCs.GetColumn("Price").([]float64)

	assert.Equal(t, inputCs.Len(), outputCs.Len())

	for i := range outPrice {
		assert.Equal(t, outEpochs[i], testCase.expected[i].epoch, testCase.description, time.Unix(outEpochs[i], 0).Format("2006-01-02"))
		assert.Equal(t, outPrice[i], testCase.expected[i].price, testCase.description, time.Unix(outEpochs[i], 0).Format("2006-01-02"))
	}
}

var testDifferentEvents = []AdjustTestCase{
	{
		description: `Price should be adjusted prior to the StockSplit event. Assertion error at %s`,
		rateChanges: []RateChange{
			{1, unixDate(2020, time.January, 4), enum.StockSplit, 2},
		},
		input: []price{
			{unixDate(2020, time.January, 1), 1.0},
			{unixDate(2020, time.January, 2), 1.0},
			{unixDate(2020, time.January, 3), 1.0},
			{unixDate(2020, time.January, 4), 0.5},
			{unixDate(2020, time.January, 5), 0.5},
			{unixDate(2020, time.January, 6), 0.5},
		},
		expected: []price{
			{unixDate(2020, time.January, 1), 0.5},
			{unixDate(2020, time.January, 2), 0.5},
			{unixDate(2020, time.January, 3), 0.5},
			{unixDate(2020, time.January, 4), 0.5},
			{unixDate(2020, time.January, 5), 0.5},
			{unixDate(2020, time.January, 6), 0.5},
		},
	},
	{
		description: `Price should be adjusted prior to a ReverseStockSplit event. Assertion error at %s`,
		rateChanges: []RateChange{
			{1, unixDate(2020, time.January, 4), enum.ReverseStockSplit, 0.5},
		},
		input: []price{
			{unixDate(2020, time.January, 1), 1.0},
			{unixDate(2020, time.January, 2), 1.0},
			{unixDate(2020, time.January, 3), 1.0},
			{unixDate(2020, time.January, 4), 2.0},
			{unixDate(2020, time.January, 5), 2.0},
			{unixDate(2020, time.January, 6), 2.0},
		},
		expected: []price{
			{unixDate(2020, time.January, 1), 2.0},
			{unixDate(2020, time.January, 2), 2.0},
			{unixDate(2020, time.January, 3), 2.0},
			{unixDate(2020, time.January, 4), 2.0},
			{unixDate(2020, time.January, 5), 2.0},
			{unixDate(2020, time.January, 6), 2.0},
		},
	},
	{
		description: `Price should be adjusted prior to a ReverseStockSplit event. Assertion error at %s`,
		rateChanges: []RateChange{
			{1, unixDate(2020, time.January, 4), enum.StockDividend, 1.1},
		},
		input: []price{
			{unixDate(2020, time.January, 1), 1.0},
			{unixDate(2020, time.January, 2), 1.0},
			{unixDate(2020, time.January, 3), 1.0},
			{unixDate(2020, time.January, 4), 0.909},
			{unixDate(2020, time.January, 5), 0.909},
			{unixDate(2020, time.January, 6), 0.909},
		},
		expected: []price{
			{unixDate(2020, time.January, 1), 0.909},
			{unixDate(2020, time.January, 2), 0.909},
			{unixDate(2020, time.January, 3), 0.909},
			{unixDate(2020, time.January, 4), 0.909},
			{unixDate(2020, time.January, 5), 0.909},
			{unixDate(2020, time.January, 6), 0.909},
		},
	},
}

func TestCase1(t *testing.T) {
	tearDown, _, _ := setup(t, "TestCase1")
	defer tearDown()

	for _, testCase := range testDifferentEvents {
		evalCase(t, testCase)
	}
}

var testDifferentDates = []AdjustTestCase{
	{
		description: `When an event occurs after the requested price range, every price should be adjusted. Assertion error at %s`,
		rateChanges: []RateChange{
			{1, unixDate(2020, time.January, 20), enum.StockSplit, 2},
		},
		input: []price{
			{unixDate(2020, time.January, 1), 1.0},
			{unixDate(2020, time.January, 2), 1.0},
			{unixDate(2020, time.January, 3), 1.0},
			{unixDate(2020, time.January, 4), 1.0},
			{unixDate(2020, time.January, 5), 1.0},
			{unixDate(2020, time.January, 6), 1.0},
		},
		expected: []price{
			{unixDate(2020, time.January, 1), 0.5},
			{unixDate(2020, time.January, 2), 0.5},
			{unixDate(2020, time.January, 3), 0.5},
			{unixDate(2020, time.January, 4), 0.5},
			{unixDate(2020, time.January, 5), 0.5},
			{unixDate(2020, time.January, 6), 0.5},
		},
	},

	{
		description: `When an event occurs before the price range, no price should be adjusted. Assertion error at %s`,
		rateChanges: []RateChange{
			{1, unixDate(2019, time.December, 20), enum.StockSplit, 2},
		},
		input: []price{
			{unixDate(2020, time.January, 1), 1.0},
			{unixDate(2020, time.January, 2), 1.0},
			{unixDate(2020, time.January, 3), 1.0},
			{unixDate(2020, time.January, 4), 1.0},
			{unixDate(2020, time.January, 5), 1.0},
			{unixDate(2020, time.January, 6), 1.0},
		},
		expected: []price{
			{unixDate(2020, time.January, 1), 1.0},
			{unixDate(2020, time.January, 2), 1.0},
			{unixDate(2020, time.January, 3), 1.0},
			{unixDate(2020, time.January, 4), 1.0},
			{unixDate(2020, time.January, 5), 1.0},
			{unixDate(2020, time.January, 6), 1.0},
		},
	},
}

func TestCase2(t *testing.T) {
	tearDown, _, _ := setup(t, "TestCase1")
	defer tearDown()

	for _, testCase := range testDifferentDates {
		evalCase(t, testCase)
	}
}

var testMultipleEventsOnDifferentDates = []AdjustTestCase{
	{
		description: `Multiple events, one happened after the price range. Assertion error at %s`,
		rateChanges: []RateChange{
			{1, unixDate(2020, time.January, 3), enum.StockSplit, 2},
			{1, unixDate(2020, time.January, 6), enum.StockSplit, 2},
			{1, unixDate(2020, time.January, 20), enum.StockSplit, 2},
		},
		input: []price{
			{unixDate(2020, time.January, 1), 8.0},
			{unixDate(2020, time.January, 2), 8.0},
			{unixDate(2020, time.January, 3), 4.0},
			{unixDate(2020, time.January, 4), 4.0},
			{unixDate(2020, time.January, 5), 4.0},
			{unixDate(2020, time.January, 6), 2.0},
			{unixDate(2020, time.January, 7), 2.0},
			{unixDate(2020, time.January, 8), 2.0},
			// on Jan 20 another Split brings down the price to 1.0
		},
		expected: []price{
			{unixDate(2020, time.January, 1), 1.0},
			{unixDate(2020, time.January, 2), 1.0},
			{unixDate(2020, time.January, 3), 1.0},
			{unixDate(2020, time.January, 4), 1.0},
			{unixDate(2020, time.January, 5), 1.0},
			{unixDate(2020, time.January, 6), 1.0},
			{unixDate(2020, time.January, 7), 1.0},
			{unixDate(2020, time.January, 8), 1.0},
		},
	},

	{
		description: `Multiple events, two happen after the price range. Assertion error at %s`,
		rateChanges: []RateChange{
			{1, unixDate(2020, time.January, 3), enum.StockSplit, 2},
			{1, unixDate(2020, time.January, 6), enum.StockSplit, 2},
			{1, unixDate(2020, time.January, 20), enum.StockSplit, 2},
			{1, unixDate(2020, time.February, 10), enum.StockSplit, 2},
		},
		input: []price{
			{unixDate(2020, time.January, 1), 8.0},
			{unixDate(2020, time.January, 2), 8.0},
			{unixDate(2020, time.January, 3), 4.0},
			{unixDate(2020, time.January, 4), 4.0},
			{unixDate(2020, time.January, 5), 4.0},
			{unixDate(2020, time.January, 6), 2.0},
			{unixDate(2020, time.January, 7), 2.0},
			{unixDate(2020, time.January, 8), 2.0},
			// on Jan 20 another Split brings down the price to 1.0
		},
		expected: []price{
			{unixDate(2020, time.January, 1), 0.5},
			{unixDate(2020, time.January, 2), 0.5},
			{unixDate(2020, time.January, 3), 0.5},
			{unixDate(2020, time.January, 4), 0.5},
			{unixDate(2020, time.January, 5), 0.5},
			{unixDate(2020, time.January, 6), 0.5},
			{unixDate(2020, time.January, 7), 0.5},
			{unixDate(2020, time.January, 8), 0.5},
		},
	},

	{
		description: `Multiple events, one happens after, one before and one in the duration of the price range. Assertion error at %s`,
		rateChanges: []RateChange{
			{1, unixDate(2019, time.December, 30), enum.StockSplit, 2},
			{1, unixDate(2020, time.January, 6), enum.StockSplit, 2},
			{1, unixDate(2020, time.January, 20), enum.StockSplit, 2},
		},
		input: []price{
			{unixDate(2020, time.January, 1), 4.0},
			{unixDate(2020, time.January, 2), 4.0},
			{unixDate(2020, time.January, 3), 4.0},
			{unixDate(2020, time.January, 4), 4.0},
			{unixDate(2020, time.January, 5), 4.0},
			{unixDate(2020, time.January, 6), 2.0},
			{unixDate(2020, time.January, 7), 2.0},
			{unixDate(2020, time.January, 8), 2.0},
			// on Jan 20 another Split brings down the price to 1.0
		},
		expected: []price{
			{unixDate(2020, time.January, 1), 1.0},
			{unixDate(2020, time.January, 2), 1.0},
			{unixDate(2020, time.January, 3), 1.0},
			{unixDate(2020, time.January, 4), 1.0},
			{unixDate(2020, time.January, 5), 1.0},
			{unixDate(2020, time.January, 6), 1.0},
			{unixDate(2020, time.January, 7), 1.0},
			{unixDate(2020, time.January, 8), 1.0},
		},
	},

	{
		description: `Multiple events, split and reverse split testing. Assertion error at %s`,
		rateChanges: []RateChange{
			{1, unixDate(2020, time.January, 3), enum.StockSplit, 2},
			{1, unixDate(2020, time.January, 6), enum.ReverseStockSplit, 0.2},
		},
		input: []price{
			{unixDate(2020, time.January, 1), 4.0},
			{unixDate(2020, time.January, 2), 4.0},
			{unixDate(2020, time.January, 3), 2.0},
			{unixDate(2020, time.January, 4), 2.0},
			{unixDate(2020, time.January, 5), 2.0},
			{unixDate(2020, time.January, 6), 10.0},
			{unixDate(2020, time.January, 7), 10.0},
			{unixDate(2020, time.January, 8), 10.0},
		},
		expected: []price{
			{unixDate(2020, time.January, 1), 10.0},
			{unixDate(2020, time.January, 2), 10.0},
			{unixDate(2020, time.January, 3), 10.0},
			{unixDate(2020, time.January, 4), 10.0},
			{unixDate(2020, time.January, 5), 10.0},
			{unixDate(2020, time.January, 6), 10.0},
			{unixDate(2020, time.January, 7), 10.0},
			{unixDate(2020, time.January, 8), 10.0},
		},
	},
}

func TestMultipleEventsOnDifferentDates(t *testing.T) {
	tearDown, _, _ := setup(t, "TestMultipleEventsOnDifferentDates")
	defer tearDown()

	for _, testCase := range testMultipleEventsOnDifferentDates {
		evalCase(t, testCase)
	}
}
