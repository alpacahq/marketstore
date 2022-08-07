package feed

import (
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/alpacahq/marketstore/v4/utils/log"
)

var ny, _ = time.LoadLocation("America/New_York")

// MarketTimeChecker is an interface to check if the market is open at the specified time or not.
type MarketTimeChecker interface {
	IsOpen(t time.Time) bool
	// Sub returns a date after X business day (= day which market is open). businessDay can be a negative value.
	Sub(date time.Time, businessDay int) (time.Time, error)
}

// DefaultMarketTimeChecker is an implementation for MarketTimeChecker object.
// this checker checks the followings:
// - the market is open at this days of the week
// - the market is open at this time
// - the market is open today (= check if today is a holiday or not)
// all those settings should be defined in this object.
type DefaultMarketTimeChecker struct {
	// i.e. []string{"Saturday", "Sunday"}
	ClosedDaysOfTheWeek        []time.Weekday
	ClosedDays                 []time.Time
	OpenHourNY, OpenMinuteNY   int
	CloseHourNY, CloseMinuteNY int
}

// NewDefaultMarketTimeChecker initializes the DefaultMarketTimeChecker object with the specifier parameters.s.
func NewDefaultMarketTimeChecker(
	closedDaysOfTheWeek []time.Weekday,
	closedDays []time.Time,
	openHourNY, openMinuteNY int,
	closeHourNY, closeMinuteNY int,
) *DefaultMarketTimeChecker {
	return &DefaultMarketTimeChecker{
		ClosedDaysOfTheWeek: closedDaysOfTheWeek,
		ClosedDays:          closedDays,
		OpenHourNY:          openHourNY, OpenMinuteNY: openMinuteNY,
		CloseHourNY: closeHourNY, CloseMinuteNY: closeMinuteNY,
	}
}

// IsOpen returns true on weekdays from 08:55 to 15:10.
// if closedDates are defined, return false on those days.
func (m *DefaultMarketTimeChecker) IsOpen(t time.Time) bool {
	tNY := t.In(ny)
	return m.isOpenDate(tNY) && m.isOpenWeekDay(tNY) && m.isOpenTime(tNY)
}

// isOpenTime returns true if the specified time is between the OpenTime and the CloseTime.
func (m *DefaultMarketTimeChecker) isOpenTime(nyT time.Time) bool {
	nyTYear, nyTMonth, nyTDay := nyT.Date()
	openTimeNY := time.Date(nyTYear, nyTMonth, nyTDay, m.OpenHourNY, m.OpenMinuteNY, 0, 0, ny)
	closeTimeNY := time.Date(nyTYear, nyTMonth, nyTDay, m.CloseHourNY, m.CloseMinuteNY, 0, 0, ny)

	if nyT.Before(openTimeNY) || nyT.After(closeTimeNY) {
		log.Debug(fmt.Sprintf("[Alpaca Broker Feeder] market is not open. "+
			"openTime(NewYork)=%02d:%02d, closeTime(NewYork)=%02d:%02d, now=%v",
			openTimeNY.Hour(), openTimeNY.Minute(), closeTimeNY.Hour(), closeTimeNY.Minute(), nyT))
		return false
	}
	return true
}

// isOpenWeekDay returns true when the specified time is in the closedDaysOfTheWeek.
func (m *DefaultMarketTimeChecker) isOpenWeekDay(t time.Time) bool {
	w := t.Weekday()
	for _, closedDay := range m.ClosedDaysOfTheWeek {
		if w == closedDay {
			return false
		}
	}
	return true
}

// isOpenDate returns true if the specified time is on closedDates.
func (m *DefaultMarketTimeChecker) isOpenDate(t time.Time) bool {
	for _, c := range m.ClosedDays {
		if c.Year() == t.Year() && c.Month() == t.Month() && c.Day() == t.Day() {
			return false
		}
	}
	return true
}

// Sub returns a date before X business days (= days which market is open). businessDay should be a positive value.
func (m *DefaultMarketTimeChecker) Sub(dateInJST time.Time, businessDay int) (time.Time, error) {
	if businessDay < 0 {
		return time.Time{}, errors.New("businessDay argument should be a positive integer")
	}

	if businessDay == 0 {
		return dateInJST, nil
	}

	count := businessDay
	d := dateInJST
	for count > 0 {
		d = d.Add(-24 * time.Hour)
		if m.isOpenDate(d) && m.isOpenWeekDay(d) {
			count--
		}
	}

	return d, nil
}
