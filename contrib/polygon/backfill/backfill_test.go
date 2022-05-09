package backfill

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/alpacahq/marketstore/v4/contrib/polygon/api"
	"github.com/alpacahq/marketstore/v4/models"
	"github.com/alpacahq/marketstore/v4/models/enum"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

const symbolAAPL = "AAPL"

func TestTicksToBars(t *testing.T) {
	t.Parallel()
	NY, _ := time.LoadLocation("America/New_York")

	// Without any condition
	{
		// Given a set of TradeTicks from three exchanges, a symbol and limited set of exchanges
		ticks := []api.TradeTick{
			{
				SIPTimestamp: time.Date(2020, 1, 21, 9, 30, 0, 0, NY).UnixNano(),
				Price:        300,
				Size:         100,
				Exchange:     9,
			},
			{
				SIPTimestamp: time.Date(2020, 1, 21, 9, 30, 1, 0, NY).UnixNano(),
				Price:        299.9,
				Size:         50,
				Exchange:     8,
			},
			{
				SIPTimestamp: time.Date(2020, 1, 21, 9, 30, 3, 0, NY).UnixNano(),
				Price:        300.1,
				Size:         80,
				Exchange:     17,
			},
		}
		symbol := symbolAAPL
		exchangeIDs := []int{9, 17}
		key := io.NewTimeBucketKeyFromString(fmt.Sprintf("%s/1Min/OHLCV", symbol))
		model := models.NewBar(symbol, "1Min", 1440)

		// When we call tradesToBars
		tradesToBars(ticks, model, exchangeIDs)

		csm := *model.BuildCsm()

		// Then the returned ColumnSeriesMarks should contain data from the two
		// specified exchanges, accumulated to minutes
		assert.NotNil(t, csm)
		ti, _ := csm[*key].GetTime()
		assert.Equal(t, ti, []time.Time{
			time.Date(2020, 1, 21, 9, 30, 0, 0, NY).In(time.UTC),
		})
		val, ok := csm[*key].GetColumn("Open").([]enum.Price)
		assert.True(t, ok)
		assert.Equal(t, val, []enum.Price{300})
		val2, ok := csm[*key].GetColumn("High").([]enum.Price)
		assert.True(t, ok)
		assert.Equal(t, val2, []enum.Price{300.1})
		val3, ok := csm[*key].GetColumn("Low").([]enum.Price)
		assert.True(t, ok)
		assert.Equal(t, val3, []enum.Price{300})
		val4, ok := csm[*key].GetColumn("Close").([]enum.Price)
		assert.True(t, ok)
		assert.Equal(t, val4, []enum.Price{300.1})
		val5, ok := csm[*key].GetColumn("Volume").([]enum.Size)
		assert.True(t, ok)
		assert.Equal(t, val5, []enum.Size{180})

		model = models.NewBar(symbol, "1Min", 1440)
		// And when we call tradesToBars with different set of exchanges
		tradesToBars(ticks, model, []int{8, 9})

		csm = *model.BuildCsm()

		// Then the returned ColumnSeriesMarks should contain data from the two new
		// specified exchanges, accumulated in minutes
		assert.NotNil(t, csm)
		ti, _ = csm[*key].GetTime()
		assert.Equal(t, ti, []time.Time{
			time.Date(2020, 1, 21, 9, 30, 0, 0, NY).In(time.UTC),
		})
		val, ok = csm[*key].GetColumn("Open").([]enum.Price)
		assert.True(t, ok)
		assert.Equal(t, val, []enum.Price{300})
		val2, ok = csm[*key].GetColumn("High").([]enum.Price)
		assert.True(t, ok)
		assert.Equal(t, val2, []enum.Price{300})
		val3, ok = csm[*key].GetColumn("Low").([]enum.Price)
		assert.True(t, ok)
		assert.Equal(t, val3, []enum.Price{299.9})
		val4, ok = csm[*key].GetColumn("Close").([]enum.Price)
		assert.True(t, ok)
		assert.Equal(t, val4, []enum.Price{299.9})
		val5, ok = csm[*key].GetColumn("Volume").([]enum.Size)
		assert.True(t, ok)
		assert.Equal(t, val5, []enum.Size{150})
	}

	// With one condition: No update on High/Low, Volume & Close
	{
		ticks := []api.TradeTick{
			{
				SIPTimestamp: time.Date(2020, 1, 21, 9, 30, 0, 0, NY).UnixNano(),
				Price:        300,
				Size:         100,
				Exchange:     9,
				Conditions:   []int{15},
			},
		}

		symbol := symbolAAPL
		exchangeIDs := []int{9}
		key := io.NewTimeBucketKeyFromString(fmt.Sprintf("%s/1Min/OHLCV", symbol))
		model := models.NewBar(symbol, "1Min", 1440)

		// When we call tradesToBars
		tradesToBars(ticks, model, exchangeIDs)

		csm := *model.BuildCsm()

		assert.NotNil(t, csm)
		val, ok := csm[*key].GetColumn("Open").([]enum.Price)
		assert.True(t, ok)
		assert.Equal(t, val, []enum.Price{})
		val2, ok := csm[*key].GetColumn("High").([]enum.Price)
		assert.True(t, ok)
		assert.Equal(t, val2, []enum.Price{})
		val3, ok := csm[*key].GetColumn("Low").([]enum.Price)
		assert.True(t, ok)
		assert.Equal(t, val3, []enum.Price{})
		val4, ok := csm[*key].GetColumn("Close").([]enum.Price)
		assert.True(t, ok)
		assert.Equal(t, val4, []enum.Price{})
		val5, ok := csm[*key].GetColumn("Volume").([]enum.Size)
		assert.True(t, ok)
		assert.Equal(t, val5, []enum.Size{})
	}

	// With conditions: Normal trade + No update on High/Low, Volume & Close
	{
		ticks := []api.TradeTick{
			{
				SIPTimestamp: time.Date(2020, 1, 21, 9, 30, 0, 0, NY).UnixNano(),
				Price:        300,
				Size:         100,
				Exchange:     9,
				Conditions:   []int{0},
			},
			{
				SIPTimestamp: time.Date(2020, 1, 21, 9, 30, 0, 4, NY).UnixNano(),
				Price:        305.2,
				Size:         10,
				Exchange:     9,
				Conditions:   []int{15},
			},
		}

		symbol := symbolAAPL
		exchangeIDs := []int{9, 8}
		key := io.NewTimeBucketKeyFromString(fmt.Sprintf("%s/1Min/OHLCV", symbol))
		model := models.NewBar(symbol, "1Min", 1440)

		// When we call tradesToBars
		tradesToBars(ticks, model, exchangeIDs)

		csm := *model.BuildCsm()

		assert.NotNil(t, csm)
		val, ok := csm[*key].GetColumn("Open").([]enum.Price)
		assert.True(t, ok)
		assert.Equal(t, val, []enum.Price{300})
		val2, ok := csm[*key].GetColumn("High").([]enum.Price)
		assert.True(t, ok)
		assert.Equal(t, val2, []enum.Price{300})
		val3, ok := csm[*key].GetColumn("Low").([]enum.Price)
		assert.True(t, ok)
		assert.Equal(t, val3, []enum.Price{300})
		val4, ok := csm[*key].GetColumn("Close").([]enum.Price)
		assert.True(t, ok)
		assert.Equal(t, val4, []enum.Price{300})
		val5, ok := csm[*key].GetColumn("Volume").([]enum.Size)
		assert.True(t, ok)
		assert.Equal(t, val5, []enum.Size{100})
	}

	// With condition: Form-T, odd-lot and normal
	{
		ticks := []api.TradeTick{
			{ // Should be included
				SIPTimestamp: time.Date(2020, 1, 21, 8, 30, 0, 0, NY).UnixNano(),
				Price:        300,
				Size:         100,
				Exchange:     9,
				Conditions:   []int{12},
			},
			{ // Should be excluded: odd-lot
				SIPTimestamp: time.Date(2020, 1, 21, 8, 30, 2, 0, NY).UnixNano(),
				Price:        314,
				Size:         99,
				Exchange:     9,
				Conditions:   []int{12, 37},
			},
			{ // Should be included
				SIPTimestamp: time.Date(2020, 1, 21, 8, 30, 2, 0, NY).UnixNano(),
				Price:        299,
				Size:         77,
				Exchange:     8,
				Conditions:   []int{12},
			},
			{
				SIPTimestamp: time.Date(2020, 1, 21, 9, 30, 0, 4, NY).UnixNano(),
				Price:        305.2,
				Size:         10,
				Exchange:     9,
				Conditions:   []int{14},
			},
			{
				SIPTimestamp: time.Date(2020, 1, 21, 9, 30, 1, 4, NY).UnixNano(),
				Price:        315.2,
				Size:         17,
				Exchange:     8,
				Conditions:   []int{},
			},
		}

		symbol := symbolAAPL
		exchangeIDs := []int{9, 8}
		key := io.NewTimeBucketKeyFromString(fmt.Sprintf("%s/1Min/OHLCV", symbol))
		model := models.NewBar(symbol, "1Min", 1440)
		// When we call tradesToBars
		tradesToBars(ticks, model, exchangeIDs)

		csm := *model.BuildCsm()
		assert.NotNil(t, csm)
		val, ok := csm[*key].GetColumn("Open").([]enum.Price)
		assert.True(t, ok)
		assert.Equal(t, val, []enum.Price{300, 305.2})
		val2, ok := csm[*key].GetColumn("High").([]enum.Price)
		assert.True(t, ok)
		assert.Equal(t, val2, []enum.Price{300, 315.2})
		val3, ok := csm[*key].GetColumn("Low").([]enum.Price)
		assert.True(t, ok)
		assert.Equal(t, val3, []enum.Price{299, 305.2})
		val4, ok := csm[*key].GetColumn("Close").([]enum.Price)
		assert.True(t, ok)
		assert.Equal(t, val4, []enum.Price{299, 315.2})
		val5, ok := csm[*key].GetColumn("Volume").([]enum.Size)
		assert.True(t, ok)
		assert.Equal(t, val5, []enum.Size{276, 27})
	}
}
