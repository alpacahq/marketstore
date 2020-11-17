package backfill

import (
	"testing"
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/polygon/api"
	"github.com/alpacahq/marketstore/v4/models"
	"github.com/alpacahq/marketstore/v4/utils/io"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

var _ = Suite(&BackfillTests{})

type BackfillTests struct{}

func (s *BackfillTests) SetUpSuite(c *C)    {}
func (s *BackfillTests) TearDownSuite(c *C) {}

func (s *BackfillTests) TestTicksToBars(c *C) {
	NY, _ := time.LoadLocation("America/New_York")

	// Without any condition
	{
		// Given a set of TradeTicks from three exchanges, a symbol and limited set of exchanges
		ticks := []api.TradeTick{
			{
				SipTimestamp: time.Date(2020, 1, 21, 9, 30, 0, 0, NY).UnixNano(),
				Price:        300,
				Size:         100,
				Exchange:     9,
			},
			{
				SipTimestamp: time.Date(2020, 1, 21, 9, 30, 1, 0, NY).UnixNano(),
				Price:        299.9,
				Size:         50,
				Exchange:     8,
			},
			{
				SipTimestamp: time.Date(2020, 1, 21, 9, 30, 3, 0, NY).UnixNano(),
				Price:        300.1,
				Size:         80,
				Exchange:     17,
			},
		}
		symbol := "AAPL"
		exchangeIDs := []int{9, 17}
		key := io.NewTimeBucketKeyFromString("AAPL/1Min/OHLCV")
		model := models.NewBar(symbol, "1Min", 1440)

		// When we call tradesToBars
		tradesToBars(ticks, model, exchangeIDs)

		csm := *model.BuildCsm()

		// Then the returned ColumnSeriesMarks should contain data from the two
		// specified exchanges, accumulated to minutes
		c.Assert(csm, NotNil)
		t, _ := csm[*key].GetTime()
		c.Assert(t, DeepEquals, []time.Time{
			time.Date(2020, 1, 21, 9, 30, 0, 0, NY).In(time.UTC),
		})
		c.Assert(csm[*key].GetColumn("Open").([]float64), DeepEquals, []float64{300})
		c.Assert(csm[*key].GetColumn("High").([]float64), DeepEquals, []float64{300.1})
		c.Assert(csm[*key].GetColumn("Low").([]float64), DeepEquals, []float64{300})
		c.Assert(csm[*key].GetColumn("Close").([]float64), DeepEquals, []float64{300.1})
		c.Assert(csm[*key].GetColumn("Volume").([]uint64), DeepEquals, []uint64{180})

		model = models.NewBar(symbol, "1Min", 1440)
		// And when we call tradesToBars with different set of exchanges
		tradesToBars(ticks, model, []int{8, 9})

		csm = *model.BuildCsm()

		// Then the returned ColumnSeriesMarks should contain data from the two new
		// specified exchanges, accumulated in minutes
		c.Assert(csm, NotNil)
		t, _ = csm[*key].GetTime()
		c.Assert(t, DeepEquals, []time.Time{
			time.Date(2020, 1, 21, 9, 30, 0, 0, NY).In(time.UTC),
		})
		c.Assert(csm[*key].GetColumn("Open").([]float64), DeepEquals, []float64{300})
		c.Assert(csm[*key].GetColumn("High").([]float64), DeepEquals, []float64{300})
		c.Assert(csm[*key].GetColumn("Low").([]float64), DeepEquals, []float64{299.9})
		c.Assert(csm[*key].GetColumn("Close").([]float64), DeepEquals, []float64{299.9})
		c.Assert(csm[*key].GetColumn("Volume").([]uint64), DeepEquals, []uint64{150})
	}

	// With one condition: No update on High/Low, Volume & Close
	{
		ticks := []api.TradeTick{
			{
				SipTimestamp: time.Date(2020, 1, 21, 9, 30, 0, 0, NY).UnixNano(),
				Price:        300,
				Size:         100,
				Exchange:     9,
				Conditions:   []int{15},
			},
		}

		symbol := "AAPL"
		exchangeIDs := []int{9}
		key := io.NewTimeBucketKeyFromString("AAPL/1Min/OHLCV")
		model := models.NewBar(symbol, "1Min", 1440)

		// When we call tradesToBars
		tradesToBars(ticks, model, exchangeIDs)

		csm := *model.BuildCsm()

		c.Assert(csm, NotNil)
		c.Assert(csm[*key].GetColumn("Open").([]float64), DeepEquals, []float64{})
		c.Assert(csm[*key].GetColumn("High").([]float64), DeepEquals, []float64{})
		c.Assert(csm[*key].GetColumn("Low").([]float64), DeepEquals, []float64{})
		c.Assert(csm[*key].GetColumn("Close").([]float64), DeepEquals, []float64{})
		c.Assert(csm[*key].GetColumn("Volume").([]uint64), DeepEquals, []uint64{})
	}

	// With conditions: Normal trade + No update on High/Low, Volume & Close
	{
		ticks := []api.TradeTick{
			{
				SipTimestamp: time.Date(2020, 1, 21, 9, 30, 0, 0, NY).UnixNano(),
				Price:        300,
				Size:         100,
				Exchange:     9,
				Conditions:   []int{0},
			},
			{
				SipTimestamp: time.Date(2020, 1, 21, 9, 30, 0, 4, NY).UnixNano(),
				Price:        305.2,
				Size:         10,
				Exchange:     9,
				Conditions:   []int{15},
			},
		}

		symbol := "AAPL"
		exchangeIDs := []int{9, 8}
		key := io.NewTimeBucketKeyFromString("AAPL/1Min/OHLCV")
		model := models.NewBar(symbol, "1Min", 1440)

		// When we call tradesToBars
		tradesToBars(ticks, model, exchangeIDs)

		csm := *model.BuildCsm()

		c.Assert(csm, NotNil)
		c.Assert(csm[*key].GetColumn("Open").([]float64), DeepEquals, []float64{300})
		c.Assert(csm[*key].GetColumn("High").([]float64), DeepEquals, []float64{300})
		c.Assert(csm[*key].GetColumn("Low").([]float64), DeepEquals, []float64{300})
		c.Assert(csm[*key].GetColumn("Close").([]float64), DeepEquals, []float64{300})
		c.Assert(csm[*key].GetColumn("Volume").([]uint64), DeepEquals, []uint64{100})
	}

	// With condition: Form-T, odd-lot and normal
	{
		ticks := []api.TradeTick{
			{ // Should be included
				SipTimestamp: time.Date(2020, 1, 21, 8, 30, 0, 0, NY).UnixNano(),
				Price:        300,
				Size:         100,
				Exchange:     9,
				Conditions:   []int{12},
			},
			{ // Should be excluded: odd-lot
				SipTimestamp: time.Date(2020, 1, 21, 8, 30, 2, 0, NY).UnixNano(),
				Price:        314,
				Size:         99,
				Exchange:     9,
				Conditions:   []int{12, 37},
			},
			{ // Should be included
				SipTimestamp: time.Date(2020, 1, 21, 8, 30, 2, 0, NY).UnixNano(),
				Price:        299,
				Size:         77,
				Exchange:     8,
				Conditions:   []int{12},
			},
			{
				SipTimestamp: time.Date(2020, 1, 21, 9, 30, 0, 4, NY).UnixNano(),
				Price:        305.2,
				Size:         10,
				Exchange:     9,
				Conditions:   []int{14},
			},
			{
				SipTimestamp: time.Date(2020, 1, 21, 9, 30, 1, 4, NY).UnixNano(),
				Price:        315.2,
				Size:         17,
				Exchange:     8,
				Conditions:   []int{},
			},
		}

		symbol := "AAPL"
		exchangeIDs := []int{9, 8}
		key := io.NewTimeBucketKeyFromString("AAPL/1Min/OHLCV")
		model := models.NewBar(symbol, "1Min", 1440)
		// When we call tradesToBars
		tradesToBars(ticks, model, exchangeIDs)

		csm := *model.BuildCsm()
		c.Assert(csm, NotNil)
		c.Assert(csm[*key].GetColumn("Open").([]float64), DeepEquals, []float64{300, 305.2})
		c.Assert(csm[*key].GetColumn("High").([]float64), DeepEquals, []float64{300, 315.2})
		c.Assert(csm[*key].GetColumn("Low").([]float64), DeepEquals, []float64{299, 305.2})
		c.Assert(csm[*key].GetColumn("Close").([]float64), DeepEquals, []float64{299, 315.2})
		c.Assert(csm[*key].GetColumn("Volume").([]uint64), DeepEquals, []uint64{276, 27})

	}
}
