package feed

import "time"

type MarketTimeChecker interface {
	isOpen(t time.Time) bool
}

type DefaultMarketTimeChecker struct {
	CloseDates []time.Time
	IsDebug    bool
}

func NewDefaultMarketTimeChecker(closeDates []time.Time, isDebug bool) *DefaultMarketTimeChecker {
	return &DefaultMarketTimeChecker{
		CloseDates: closeDates,
		IsDebug: isDebug,
	}
}

// isOpen returns true on weekdays from 08:55 to 15:10.
// if closedDates are defined, return false on those days
func (m *DefaultMarketTimeChecker) isOpen(t time.Time) bool {
	if m.IsDebug {
		return true
	}
	w := t.Weekday()
	if w == time.Saturday || w == time.Sunday {
		return false
	}

	// true during 8:55 ~ 15:10
	minutesFrom12am := t.Hour()*60 + t.Minute()

	// 8 hour 55 min = 535 min,  15 hour 10 min = 910 min
	if minutesFrom12am < 535 || minutesFrom12am > 910 {
		return false
	}

	return true
}

// isClosedDate returns true if the specified time is on closedDates
func (m *DefaultMarketTimeChecker) isCloseDate(t time.Time) bool {
	for _, c := range m.CloseDates {
		if c.Year() == t.Year() && c.Month() == t.Month() && c.Day() == t.Day() {
			return true
		}
	}
	return false
}
