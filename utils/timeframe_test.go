package utils

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTimeframeFromDuration(t *testing.T) {
	t.Parallel()
	tf := TimeframeFromDuration(22 * time.Minute)
	assert.Equal(t, tf.String, "22Min")
	assert.Equal(t, tf.Duration, 22*time.Minute)

	tf = TimeframeFromDuration(time.Hour)
	assert.Equal(t, tf.String, "1H")
	assert.Equal(t, tf.Duration, time.Hour)

	tf = TimeframeFromDuration(time.Nanosecond)
	assert.Nil(t, tf)

	tf = TimeframeFromDuration(5 * Year)
	assert.Nil(t, tf)
}

func TestTimeframeFromString(t *testing.T) {
	t.Parallel()
	tf := TimeframeFromString("15H")
	assert.Equal(t, tf.String, "15H")
	assert.Equal(t, tf.Duration, 15*time.Hour)

	tf = TimeframeFromString("xyz")
	assert.Nil(t, tf)

	tf = TimeframeFromString("0H")
	assert.Nil(t, tf)
}

func TestCandleDuration(t *testing.T) {
	t.Parallel()
	var cd *CandleDuration
	var val, start time.Time
	var within bool
	cd = CandleDurationFromString("5Min")
	val = time.Date(2017, 9, 10, 13, 47, 0, 0, time.UTC)
	assert.Equal(t, cd.Truncate(val), time.Date(2017, 9, 10, 13, 45, 0, 0, time.UTC))
	assert.Equal(t, cd.Ceil(val), time.Date(2017, 9, 10, 13, 50, 0, 0, time.UTC))
	start = cd.Truncate(val)
	within = cd.IsWithin(time.Date(2017, 9, 10, 13, 46, 0, 0, time.UTC), start)
	assert.Equal(t, within, true)
	within = cd.IsWithin(time.Date(2017, 9, 10, 13, 51, 0, 0, time.UTC), start)
	assert.Equal(t, within, false)

	cd = CandleDurationFromString("1M")
	val = time.Date(2017, 9, 10, 13, 47, 0, 0, time.UTC)
	assert.Equal(t, cd.Truncate(val), time.Date(2017, 9, 1, 0, 0, 0, 0, time.UTC))
	assert.Equal(t, cd.Ceil(val), time.Date(2017, 10, 1, 0, 0, 0, 0, time.UTC))
	start = cd.Truncate(val)
	within = cd.IsWithin(time.Date(2017, 9, 26, 0, 0, 0, 0, time.UTC), start)
	assert.Equal(t, within, true)
	within = cd.IsWithin(time.Date(2017, 10, 1, 0, 0, 0, 0, time.UTC), start)
	assert.Equal(t, within, false)
	within = cd.IsWithin(time.Date(2017, 8, 31, 0, 0, 0, 0, time.UTC), start)
	assert.Equal(t, within, false)

	val = time.Date(2017, 12, 10, 13, 47, 0, 0, time.UTC)
	assert.Equal(t, cd.Truncate(val), time.Date(2017, 12, 1, 0, 0, 0, 0, time.UTC))
	assert.Equal(t, cd.Ceil(val), time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC))
	start = cd.Truncate(val)
	within = cd.IsWithin(time.Date(2018, 1, 26, 0, 0, 0, 0, time.UTC), start)
	assert.Equal(t, within, false)
	within = cd.IsWithin(time.Date(2016, 12, 10, 0, 0, 0, 0, time.UTC), start)
	assert.Equal(t, within, false)

	cd = CandleDurationFromString("1W")
	val = time.Date(2017, 1, 8, 0, 0, 0, 0, time.UTC)
	start = time.Date(2018, 1, 7, 0, 0, 0, 0, time.UTC)
	within = cd.IsWithin(val, start)
	assert.Equal(t, within, false)
	val = time.Date(2018, 1, 8, 0, 0, 0, 0, time.UTC)
	start = time.Date(2018, 1, 8, 0, 0, 0, 0, time.UTC)
	within = cd.IsWithin(val, start)
	assert.Equal(t, within, true)

	loc, _ := time.LoadLocation("America/New_York")
	cd = CandleDurationFromString("1D")
	val = time.Date(2018, 1, 8, 0, 0, 0, 0, loc)
	start = cd.Truncate(val)
	assert.Equal(t, start.Hour(), 0)
	assert.Equal(t, start.Minute(), 0)
	assert.Equal(t, start.Day(), val.Day())
	assert.Equal(t, start.Month(), val.Month())
	assert.Equal(t, start.Year(), val.Year())

	assert.Equal(t, cd.IsWithin(val, time.Date(2018, 1, 8, 23, 59, 0, 0, loc)), true)
	assert.Equal(t, cd.IsWithin(val, time.Date(2018, 1, 8, 0, 0, 0, 0, loc)), true)
	assert.Equal(t, cd.IsWithin(val, time.Date(2018, 1, 8, 0, 0, 0, 0, time.UTC)), false)
	assert.Equal(t, cd.IsWithin(val, time.Date(2018, 1, 8, 23, 59, 0, 0, time.UTC)), true)

	cd = CandleDurationFromString("abc")
	assert.Nil(t, cd)
}
