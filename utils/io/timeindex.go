package io

import (
	"time"

	"github.com/alpacahq/marketstore/utils"
)

func ToSystemTimezone(t time.Time) time.Time {
	loc := utils.InstanceConfig.Timezone
	return t.In(loc)
}

func IndexToTime(index, intervalsPerDay int64, year int16) time.Time {
	secondsPerDay := int64(86400)
	totalSeconds := int64(float64(secondsPerDay*(index-1)) / (float64(intervalsPerDay)))
	loc := utils.InstanceConfig.Timezone
	t0 := time.Date(int(year), time.January, 1, 0, 0, 0, 0, loc)
	t0 = t0.Add(time.Duration(totalSeconds) * time.Second)
	return time.Date(t0.Year(), t0.Month(), t0.Day(), t0.Hour(), t0.Minute(), t0.Second(), t0.Nanosecond(), loc)
}

/*
func IndexToTime(index, intervalsPerDay int64, year int16) time.Time {
	secondsPerDay := float64(24 * 60 * 60)
	SecondOfYear := time.Duration((float64(index-1) / float64(intervalsPerDay)) * secondsPerDay)
	return time.Date(int(year), time.January, 1, 0, 0, 0, 0, time.UTC).Add(SecondOfYear * time.Second)
}
*/

func TimeToIndex(t time.Time, intervalsPerDay int64) int64 {
	intervalsPerSecond := float64(intervalsPerDay) / float64(24*60*60)
	seconds := float64(t.Hour()*3600 + t.Minute()*60 + t.Second())
	day := int64(t.YearDay() - 1)
	return 1 + int64(day*intervalsPerDay+int64(seconds*intervalsPerSecond))
}

func TimeToOffset(t time.Time, intervalsPerDay int64, recordSize int32) int64 {
	indx := TimeToIndex(t, intervalsPerDay)
	return (indx-1)*int64(recordSize) + Headersize
}

/*
This constant removes the need for inaccurate floating point division
It is equivalent to:
	float64(math.MaxUint32) / float64(24 * 60 * 60)
*/
const ticksPerIntervalDivSecsPerDay = float64(49710.269629629629629629629629629)

func GetIntervalTicks32Bit(ts time.Time, index, intervalsPerDay int64) uint32 {
	/*
		Returns the number of interval ticks between the timestamp and the base time
		Each interval has up to 2^32 ticks
	*/
	baseTime := IndexToTime(index, intervalsPerDay, int16(ts.Year()))
	seconds := ts.Sub(baseTime).Seconds()
	ticksPerSecond := float64(intervalsPerDay) * ticksPerIntervalDivSecsPerDay
	return uint32(ticksPerSecond * seconds)
}
