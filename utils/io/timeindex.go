package io

import (
	"time"

	"github.com/alpacahq/marketstore/utils"
)

// IndexToTime returns the time.Time represented by the given index
// in the system timezone (UTC by default).
func IndexToTime(index int64, tf time.Duration, year int16) time.Time {
	t0 := time.Date(
		int(year),
		time.January,
		1, 0, 0, 0, 0,
		utils.InstanceConfig.Timezone)
	if tf == utils.Day {
		return t0.AddDate(0, 0, int(index))
	}
	return t0.Add(tf * time.Duration(index-1))
}

// ToSystemTimezone converts the given time.Time to the system timezone.
func ToSystemTimezone(t time.Time) time.Time {
	return t.In(utils.InstanceConfig.Timezone)
}

// TimeToIndex converts a given time.Time to a file index based upon the supplied
// timeframe (time.Duration). TimeToIndex takes into account the system timzeone,
// and converts the supplied timestamp to the system timezone specified in the
// MarketStore configuration file (or UTC by default),
func TimeToIndex(t time.Time, tf time.Duration) int64 {
	tLocal := ToSystemTimezone(t)
	// special 1D case (maximum supported on-disk size)
	if tf == utils.Day {
		return int64(tLocal.YearDay() - 1)
	}
	return 1 + int64(tLocal.Sub(
		time.Date(
			tLocal.Year(),
			time.January,
			1, 0, 0, 0, 0,
			tLocal.Location())).Nanoseconds())/int64(tf.Nanoseconds())
}

func EpochToIndex(epoch int64, tf time.Duration) int64 {
	return TimeToIndex(time.Unix(epoch, 0), tf)
}

func TimeToOffset(t time.Time, tf time.Duration, recordSize int32) int64 {
	return (TimeToIndex(t, tf)-1)*int64(recordSize) + Headersize
}

func IndexToOffset(index int64, recordSize int32) int64 {
	return (index-1)*int64(recordSize) + Headersize
}

func EpochToOffset(epoch int64, tf time.Duration, recordSize int32) int64 {
	return IndexToOffset(EpochToIndex(epoch, tf), recordSize)
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
	baseTime := IndexToTimeDepr(index, intervalsPerDay, int16(ts.Year()))
	seconds := ts.Sub(baseTime).Seconds()
	ticksPerSecond := float64(intervalsPerDay) * ticksPerIntervalDivSecsPerDay
	return uint32(ticksPerSecond * seconds)
}

func IndexToTimeDepr(index, intervalsPerDay int64, year int16) time.Time {
	SecondOfYear := time.Duration(float64(index-1) * float64(24*60*60) / float64(intervalsPerDay))
	return time.Date(int(year), time.January, 1, 0, 0, 0, 0, time.UTC).Add(SecondOfYear * time.Second)
}
