package utils

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const Day = 24 * time.Hour
const Week = 7 * Day
const Year = 365 * Day

var timeframeDefs = []Timeframe{
	{"Sec", time.Second},
	{"Min", time.Minute},
	{"H", time.Hour},
	{"D", Day},
	{"W", Week},
	{"Y", Year},
}

var Timeframes = []*Timeframe{
	{"1Min", time.Minute},
	{"5Min", 5 * time.Minute},
	{"15Min", 15 * time.Minute},
	{"1H", time.Hour},
	{"4H", 4 * time.Hour},
	{"1D", Day},
}

type Timeframe struct {
	String   string
	Duration time.Duration
}

func (tf *Timeframe) PeriodsPerDay() int {
	return int(Day / tf.Duration)
}

func NewTimeframe(arg interface{}) (tf *Timeframe) {
	tf = new(Timeframe)
	//	switch reflect.TypeOf(arg).Kind() {
	switch v := arg.(type) {
	case string:
		return TimeframeFromString(v)
	case int64:
		return TimeframeFromDuration(time.Duration(v))
	}
	return nil
}

func TimeframeFromString(tf string) *Timeframe {
	for _, def := range timeframeDefs {
		if strings.Contains(tf, def.String) {
			t, err := strconv.ParseInt(strings.Split(tf, def.String)[0], 10, 32)
			if err != nil || t <= 0 {
				return nil
			} else {
				return &Timeframe{
					String:   tf,
					Duration: def.Duration * time.Duration(t),
				}
			}
		}
	}
	return nil
}

func TimeframeFromDuration(tf time.Duration) *Timeframe {
	lowerDur := time.Second
	lowerStr := "Sec"
	if tf < lowerDur {
		return nil
	}
	for _, def := range timeframeDefs {
		if def.Duration == tf {
			return &Timeframe{
				String:   fmt.Sprintf("%v%v", 1, def.String),
				Duration: tf,
			}
		} else if def.Duration > tf {
			coefficient := int(tf / lowerDur)
			return &Timeframe{
				String:   fmt.Sprintf("%v%v", coefficient, lowerStr),
				Duration: tf,
			}
		}
		lowerDur = def.Duration
		lowerStr = def.String
	}
	return nil
}

type CandleDuration struct {
	String     string
	duration   time.Duration
	suffix     string
	multiplier int
}

func (cd *CandleDuration) IsWithin(ts, start time.Time) bool {
	switch cd.suffix {
	case "W":
		tsY, tsW := ts.ISOWeek()
		sY, sW := start.ISOWeek()
		if tsY == sY && tsW == sW {
			return true
		}
	case "M":
		if ts.Year() == start.Year() {
			if ts.Month() == start.Month() {
				return true
			} else if ts.Month() < start.Month() {
				return false
			} else {
				if int(ts.Month())-int(start.Month()) < cd.multiplier {
					return true
				} else {
					return false
				}
			}
		} else if ts.Year() > start.Year() {
			if int(ts.Month())-(12-int(start.Month())) < cd.multiplier {
				return true
			} else {
				return false
			}
		} else {
			return false
		}
	case "Y":
		if (ts.Year() - start.Year()) <= cd.multiplier {
			return true
		}
	default:
		if ts.Truncate(cd.duration) == start {
			return true
		}
	}
	return false
}

// Truncate returns the lower boundary time of this candle window that
// ts belongs to.
func (cd *CandleDuration) Truncate(ts time.Time) time.Time {
	switch cd.suffix {
	case "M":
		return time.Date(ts.Year(), ts.Month(), 1, 0, 0, 0, 0, ts.Location())
	default:
		return ts.Truncate(cd.duration)
	}
}

// Ceil returns the upper boundary time of this candle window that
// ts belongs to.
func (cd *CandleDuration) Ceil(ts time.Time) time.Time {
	if cd.suffix == "M" {
		year := ts.Year()
		month := ts.Month()
		if month == time.December {
			year += 1
			month = time.January
		} else {
			month += 1
		}
		return time.Date(year, month, 1, 0, 0, 0, 0, ts.Location())
	}

	return (ts.Add(cd.duration)).Truncate(cd.duration)
}

func (cd *CandleDuration) QueryableTimeframe() string {
	if cd.suffix != "M" {
		for i := len(Timeframes) - 1; i >= 0; i-- {
			if cd.duration%Timeframes[i].Duration == time.Duration(0) {
				return Timeframes[i].String
			}
		}
	}
	return "1D"
}

func (cd *CandleDuration) QueryableNrecords(tf string, nrecords int) int {
	if cd.String == tf {
		return nrecords
	} else {
		if cd.suffix == "M" {
			return 31 * nrecords
		} else {
			return nrecords * int(cd.duration/TimeframeFromString(tf).Duration)
		}
	}
}

func CandleDurationFromString(tf string) (cd *CandleDuration) {
	re := regexp.MustCompile("([0-9]+)")
	prefix := re.FindAllString(tf, -1)[0]
	mult, err := strconv.Atoi(prefix)
	if err != nil {
		return nil
	}
	suffix := strings.Split(tf, prefix)[1]
	return &CandleDuration{
		String:     tf,
		multiplier: mult,
		suffix:     suffix,
		duration:   time.Duration(mult) * suffixDefs[suffix],
	}
}

var suffixDefs = map[string]time.Duration{
	"Sec": time.Second,
	"Min": time.Minute,
	"H":   time.Hour,
	"D":   Day,
	"W":   Week,
	"Y":   Year,
}
