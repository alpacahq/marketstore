package feed

import (
	"fmt"
	"github.com/alpacahq/marketstore/utils/log"
	"time"
)

type MarketTimeChecker interface {
	isOpen(t time.Time) bool
}

type DefaultMarketTimeChecker struct {
	ClosedDaysOfTheWeek []string
	ClosedDays          []time.Time
	OpenTime            time.Time
	CloseTime           time.Time
}

func NewDefaultMarketTimeChecker(closedDaysOfTheWeek []string, closedDays []time.Time, openTime time.Time, closeTime time.Time) *DefaultMarketTimeChecker {
	return &DefaultMarketTimeChecker{
		ClosedDaysOfTheWeek: closedDaysOfTheWeek,
		ClosedDays:          closedDays,
		OpenTime:            openTime,
		CloseTime:           closeTime,
	}
}

// isOpen returns true on weekdays from 08:55 to 15:10.
// if closedDates are defined, return false on those days
func (m *DefaultMarketTimeChecker) isOpen(t time.Time) bool {
	return m.isOpenTime(t) && m.isOpenDay(t) && m.isOpenDate(t)
}

// isOpenTime returns true if the specified time is between the OpenTime and the CloseTime
func (m *DefaultMarketTimeChecker) isOpenTime(t time.Time) bool {
	minFrom12am := t.Hour()*60 + t.Minute()

	openMinFrom12am := m.OpenTime.Hour()*60 + m.OpenTime.Minute()
	closeMinFrom12am := m.CloseTime.Hour()*60 + m.CloseTime.Minute()

	// if the open hour is later than the close hour (i.e. open=23h, close=6h ), +1day
	if closeMinFrom12am < openMinFrom12am {
		minFrom12am += 24 * 60 * 60
		closeMinFrom12am += 24 * 60 * 60
	}

	if minFrom12am < openMinFrom12am || minFrom12am >= closeMinFrom12am {
		log.Debug(fmt.Sprintf("[Xignite Feeder] won't run because the market is not open." +
			"openTime=%v, closeTime=%v, now=%v", m.OpenTime, m.CloseTime, t))
		return false
	}
	return true
}

// isOpenDay returns true when the specified time is in the closedDaysOfTheWeek
func (m *DefaultMarketTimeChecker) isOpenDay(t time.Time) bool {
	w := t.Weekday()
	for _, closedDay := range m.ClosedDaysOfTheWeek {
		if w.String() == closedDay {
			log.Debug(fmt.Sprintf("[Xignite Feeder] won't run because today is %v", w.String()))
			return false
		}
	}
	return true
}

// isClosedDate returns true if the specified time is on closedDates
func (m *DefaultMarketTimeChecker) isOpenDate(t time.Time) bool {
	for _, c := range m.ClosedDays {
		if c.Year() == t.Year() && c.Month() == t.Month() && c.Day() == t.Day() {
			log.Debug(fmt.Sprintf("[Xignite Feeder] won't run because the market is not open today. %v", t))
			return false
		}
	}
	return true
}
