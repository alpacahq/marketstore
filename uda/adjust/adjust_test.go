package adjust

import (
	"math"
	"testing"
	"time"

	"github.com/alpacahq/marketstore/v4/catalog"
	"github.com/alpacahq/marketstore/v4/contrib/ice/enum"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/utils/io"
	. "gopkg.in/check.v1"
)

func TestAdjust(t *testing.T) {
	rounderNum = math.Pow(10, 3)
	TestingT(t)
}

type TestSuiteAdjust struct {
	Rootdir string
	DataDir *catalog.Directory
}

func (s *TestSuiteAdjust) SetupSuite(c *C) {
	s.Rootdir = c.MkDir()
	metadata, _, _ := executor.NewInstanceSetup(s.Rootdir, nil, 5, true, true, false, true) // WAL Bypass
	s.DataDir = metadata.CatalogDir
}

var _ = Suite(&TestSuiteAdjust{})

type inputData []price

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

func evalCase(testCase AdjustTestCase, c *C, catDir *catalog.Directory) {
	symbol := "AAPL"
	tbkStr := symbol + "/1Min/OHLCV"
	tbk := io.NewTimeBucketKeyFromString(tbkStr)
	adj := Adjust{}
	aggfunc, _ := adj.New(false)
	aggfunc.SetTimeBucketKey(*tbk)

	rateChangeCache[CacheKey{symbol, true, true}] = RateChangeCache{
		Changes:   testCase.rateChanges,
		Access:    0,
		CreatedAt: time.Now(),
	}

	inputCs := toColumnSeries(testCase.input)

	aggfunc.Init()
	aggfunc.Accum(inputCs, catDir)

	outputCs := aggfunc.Output()

	outEpochs := outputCs.GetColumn("Epoch").([]int64)
	outPrice := outputCs.GetColumn("Price").([]float64)

	c.Assert(inputCs.Len(), Equals, outputCs.Len())

	for i := range outPrice {
		c.Assert(outEpochs[i], Equals, testCase.expected[i].epoch, Commentf(testCase.description, time.Unix(outEpochs[i], 0).Format("2006-01-02")))
		c.Assert(outPrice[i], Equals, testCase.expected[i].price, Commentf(testCase.description, time.Unix(outEpochs[i], 0).Format("2006-01-02")))
	}
}

var testDifferentEvents = []AdjustTestCase{
	AdjustTestCase{
		description: `Price should be adjusted prior to the StockSplit event. Assertion error at %s`,
		rateChanges: []RateChange{
			RateChange{1, unixDate(2020, time.January, 4), enum.StockSplit, 2},
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
	AdjustTestCase{
		description: `Price should be adjusted prior to a ReverseStockSplit event. Assertion error at %s`,
		rateChanges: []RateChange{
			RateChange{1, unixDate(2020, time.January, 4), enum.ReverseStockSplit, 0.5},
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
	AdjustTestCase{
		description: `Price should be adjusted prior to a ReverseStockSplit event. Assertion error at %s`,
		rateChanges: []RateChange{
			RateChange{1, unixDate(2020, time.January, 4), enum.StockDividend, 1.1},
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

func (t *TestSuiteAdjust) TestCase1(c *C) {
	for _, testCase := range testDifferentEvents {
		evalCase(testCase, c, t.DataDir)
	}
}

var testDifferentDates = []AdjustTestCase{
	AdjustTestCase{
		description: `When an event occurs after the requested price range, every price should be adjusted. Assertion error at %s`,
		rateChanges: []RateChange{
			RateChange{1, unixDate(2020, time.January, 20), enum.StockSplit, 2},
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

	AdjustTestCase{
		description: `When an event occurs before the price range, no price should be adjusted. Assertion error at %s`,
		rateChanges: []RateChange{
			RateChange{1, unixDate(2019, time.December, 20), enum.StockSplit, 2},
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

func (t *TestSuiteAdjust) TestCase2(c *C) {
	for _, testCase := range testDifferentDates {
		evalCase(testCase, c, t.DataDir)
	}
}

var testMultipleEventsOnDifferentDates = []AdjustTestCase{
	AdjustTestCase{
		description: `Multiple events, one happened after the price range. Assertion error at %s`,
		rateChanges: []RateChange{
			RateChange{1, unixDate(2020, time.January, 3), enum.StockSplit, 2},
			RateChange{1, unixDate(2020, time.January, 6), enum.StockSplit, 2},
			RateChange{1, unixDate(2020, time.January, 20), enum.StockSplit, 2},
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

	AdjustTestCase{
		description: `Multiple events, two happen after the price range. Assertion error at %s`,
		rateChanges: []RateChange{
			RateChange{1, unixDate(2020, time.January, 3), enum.StockSplit, 2},
			RateChange{1, unixDate(2020, time.January, 6), enum.StockSplit, 2},
			RateChange{1, unixDate(2020, time.January, 20), enum.StockSplit, 2},
			RateChange{1, unixDate(2020, time.February, 10), enum.StockSplit, 2},
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

	AdjustTestCase{
		description: `Multiple events, one happens after, one before and one in the duration of the price range. Assertion error at %s`,
		rateChanges: []RateChange{
			RateChange{1, unixDate(2019, time.December, 30), enum.StockSplit, 2},
			RateChange{1, unixDate(2020, time.January, 6), enum.StockSplit, 2},
			RateChange{1, unixDate(2020, time.January, 20), enum.StockSplit, 2},
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

	AdjustTestCase{
		description: `Multiple events, split and reverse split testing. Assertion error at %s`,
		rateChanges: []RateChange{
			RateChange{1, unixDate(2020, time.January, 3), enum.StockSplit, 2},
			RateChange{1, unixDate(2020, time.January, 6), enum.ReverseStockSplit, 0.2},
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

func (t *TestSuiteAdjust) TestMultipleEventsOnDifferentDates(c *C) {
	for _, testCase := range testMultipleEventsOnDifferentDates {
		evalCase(testCase, c, t.DataDir)
	}
}
