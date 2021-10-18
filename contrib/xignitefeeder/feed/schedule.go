package feed

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/alpacahq/marketstore/v4/utils/log"
)

// ParseSchedule checks comma-separated numbers in a string format
// and returns the list of minutes that data feeding is executed.
// Examples:
// "0,15,30,45" -> [0,15,30,45] (the data feeding must be executed every 15 minutes)
// "50,10" -> [10, 50]
// "   20,   40   " -> [20, 40] (whitespaces are ignored)
// "20" -> [20] (00:10, 01:10, ..., 23:10)
// "100" -> error (minute must be between 0 and 59)
// "One" -> error (numbers must be used)
// "0-10" -> error (range is not supported)
func ParseSchedule(s string) ([]int, error) {
	if s == "" {
		log.Debug("[xignite] no schedule is set for off_hours")
		return []int{}, nil
	}
	s = strings.ReplaceAll(s, " ", "")
	strs := strings.Split(s, ",")

	ret := make([]int, len(strs))
	var err error
	for i, m := range strs {
		ret[i], err = strconv.Atoi(m)
		if err != nil {
			return nil, fmt.Errorf("parse %s for scheduling of xignite feeder: %w", m, err)
		}

		if ret[i] < 0 || ret[i] >= 60 {
			return nil, fmt.Errorf("off_hours_schedule[min] must be between 0 and 59: got=%d", ret[i])
		}
	}

	sort.Ints(ret)
	return ret, nil
}

// ScheduledMarketTimeChecker is used where periodic processing is needed to run even when the market is closed.
type ScheduledMarketTimeChecker struct {
	MarketTimeChecker
	// LastTime holds the last time that IntervalTimeChceker.IsOpen returned true.
	LastTime    time.Time
	ScheduleMin []int
}

func NewScheduledMarketTimeChecker(
	mtc MarketTimeChecker,
	scheduleMin []int,
) *ScheduledMarketTimeChecker {
	return &ScheduledMarketTimeChecker{
		MarketTimeChecker: mtc,
		LastTime:          time.Time{},
		ScheduleMin:       scheduleMin,
	}
}

// IsOpen returns true when the market is open or the interval elapsed since LastTime.
func (c *ScheduledMarketTimeChecker) IsOpen(t time.Time) bool {
	return c.MarketTimeChecker.IsOpen(t) || c.tick(t)
}

func (c *ScheduledMarketTimeChecker) tick(t time.Time) bool {
	m := t.Minute()
	for _, sche := range c.ScheduleMin {
		if m != sche {
			continue
		}

		// maximum frequency is once a minute
		if t.Sub(c.LastTime) < 1*time.Minute {
			continue
		}

		log.Debug(fmt.Sprintf("[Xignite Feeder] run data feed based on the schedule: %v(min)", c.ScheduleMin))
		c.LastTime = t
		return true
	}

	return false
}
