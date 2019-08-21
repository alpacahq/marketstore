// (c) 2014 Rick Arnold. Licensed under the BSD license (see LICENSE).

package cal

import (
	"time"
)

// ObservedRule represents a rule for observing a holiday that falls
// on a weekend.
type ObservedRule int

// ObservedRule are the specific ObservedRules
const (
	ObservedNearest ObservedRule = iota // nearest weekday (Friday or Monday)
	ObservedExact                       // the exact day only
	ObservedMonday                      // Monday always
)

// HolidayFn calculates the occurrence of a holiday for the given year.
// This is useful for holidays like Easter that depend on complex rules.
type HolidayFn func(year int, loc *time.Location) (month time.Month, day int)

// Holiday holds information about the yearly occurrence of a holiday.
//
// A valid Holiday consists of one of the following:
// - Month and Day (such as March 14 for Pi Day)
// - Month, Weekday, and Offset (such as the second Monday of October for Columbus Day)
// - Offset (such as the 183rd day of the year for the start of the second half)
// - Month, Day, and Year (in case you want to specify holidays exactly for each year)
// - Func (to calculate the holiday)
type Holiday struct {
	Month   time.Month
	Weekday time.Weekday
	Day     int
	Offset  int
	Year    int
	Func    HolidayFn

	// last values used to calculate month and day with Func
	lastYear int
	lastLoc  *time.Location
}

// NewHoliday creates a new Holiday instance for an exact day of a month.
func NewHoliday(month time.Month, day int) Holiday {
	return Holiday{Month: month, Day: day}
}

// NewHolidayExact creates a new Holiday instance for an exact day of a month and year.
func NewHolidayExact(month time.Month, day int, year int) Holiday {
	return Holiday{Month: month, Day: day, Year: year}
}

// NewHolidayFloat creates a new Holiday instance for an offset-based day of
// a month.
func NewHolidayFloat(month time.Month, weekday time.Weekday, offset int) Holiday {
	return Holiday{Month: month, Weekday: weekday, Offset: offset}
}

// NewHolidayFunc creates a new Holiday instance that uses a function to
// calculate the day and month.
func NewHolidayFunc(fn HolidayFn) Holiday {
	return Holiday{Func: fn}
}

// matches determines whether the given date is the one referred to by the
// Holiday.
func (h *Holiday) matches(date time.Time) bool {

	if h.Func != nil && (date.Year() != h.lastYear || date.Location() != h.lastLoc) {
		h.Month, h.Day = h.Func(date.Year(), date.Location())
		h.lastYear = date.Year()
		h.lastLoc = date.Location()
	}

	if h.Month > 0 {
		if date.Month() != h.Month {
			return false
		}
		if h.Year > 0 && date.Year() != h.Year {
			return false
		}
		if h.Day > 0 {
			return date.Day() == h.Day
		}
		if h.Weekday > 0 && h.Offset != 0 {
			return IsWeekdayN(date, h.Weekday, h.Offset)
		}
	} else if h.Offset > 0 {
		return date.YearDay() == h.Offset
	}
	return false
}
