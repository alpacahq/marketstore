package calendar

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var NY, _ = time.LoadLocation("America/New_York")

func TestCalendar(t *testing.T) {
	t.Parallel()
	// Weekend
	weekend := time.Date(2017, 1, 1, 11, 0, 0, 0, NY)
	assert.Equal(t, Nasdaq.IsMarketOpen(weekend), false)
	assert.Equal(t, Nasdaq.IsMarketDay(weekend), false)

	// MLK day 2018
	mlk := time.Date(2018, 1, 15, 11, 0, 0, 0, NY)
	assert.Equal(t, Nasdaq.IsMarketOpen(mlk), false)
	assert.Equal(t, Nasdaq.IsMarketDay(mlk), false)

	// July 3rd 2019 (early close)
	julThirdAM := time.Date(2018, 7, 3, 11, 0, 0, 0, NY)
	julThirdPM := time.Date(2018, 7, 3, 15, 0, 0, 0, NY)
	assert.True(t, Nasdaq.IsMarketOpen(julThirdAM))
	assert.True(t, Nasdaq.EpochIsMarketOpen(julThirdAM.Unix()))
	assert.True(t, Nasdaq.IsMarketDay(julThirdAM))

	assert.False(t, Nasdaq.IsMarketOpen(julThirdPM))
	assert.True(t, Nasdaq.IsMarketDay(julThirdPM))

	// normal day
	bestDayMid := time.Date(2021, 8, 31, 11, 0, 0, 0, NY)
	bestDayEarly := time.Date(2021, 8, 31, 7, 0, 0, 0, NY)
	bestDayLate := time.Date(2021, 8, 31, 19, 0, 0, 0, NY)
	assert.True(t, Nasdaq.IsMarketOpen(bestDayMid))
	assert.True(t, Nasdaq.IsMarketDay(bestDayMid))

	assert.False(t, Nasdaq.IsMarketOpen(bestDayEarly))
	assert.True(t, Nasdaq.IsMarketDay(bestDayEarly))

	assert.False(t, Nasdaq.IsMarketOpen(bestDayLate))
	assert.True(t, Nasdaq.IsMarketDay(bestDayLate))

	assert.Equal(t, Nasdaq.Tz().String(), "America/New_York")
}
