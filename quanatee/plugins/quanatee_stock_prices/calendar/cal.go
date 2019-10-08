// (c) 2014 Rick Arnold. Licensed under the BSD license (see LICENSE).

package cal

import (
	"math"
	"time"
)

// IsWeekend reports whether the given date falls on a weekend.
func IsWeekend(date time.Time) bool {
	day := date.Weekday()
	return day == time.Saturday || day == time.Sunday
}

// IsWeekdayN reports whether the given date is the nth occurrence of the
// day in the month.
//
// The value of n affects the direction of counting:
//   n > 0: counting begins at the first day of the month.
//   n == 0: the result is always false.
//   n < 0: counting begins at the end of the month.
func IsWeekdayN(date time.Time, day time.Weekday, n int) bool {
	cday := date.Weekday()
	if cday != day || n == 0 {
		return false
	}

	if n > 0 {
		return (date.Day()-1)/7 == (n - 1)
	}

	n = -n
	last := time.Date(date.Year(), date.Month()+1,
		1, 12, 0, 0, 0, date.Location())
	lastCount := 0
	for {
		last = last.AddDate(0, 0, -1)
		if last.Weekday() == day {
			lastCount++
		}
		if lastCount == n || last.Month() != date.Month() {
			break
		}
	}
	return lastCount == n && last.Month() == date.Month() &&
		last.Day() == date.Day()

}

// MonthStart reports the starting day of the month in t. The time portion is
// unchanged.
func MonthStart(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), 1, t.Hour(), t.Minute(), t.Second(),
		t.Nanosecond(), t.Location())
}

// MonthEnd reports the ending day of the month in t. The time portion is
// unchanged.
func MonthEnd(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month()+1, 0, t.Hour(), t.Minute(),
		t.Second(), t.Nanosecond(), t.Location())
}

// JulianDayNumber reports the Julian Day Number for t. Note that Julian days
// start at 12:00 UTC.
func JulianDayNumber(t time.Time) int {
	// algorithm from http://www.tondering.dk/claus/cal/julperiod.php#formula
	utc := t.UTC()
	a := (14 - int(utc.Month())) / 12
	y := utc.Year() + 4800 - a
	m := int(utc.Month()) + 12*a - 3

	jdn := utc.Day() + (153*m+2)/5 + 365*y + y/4 - y/100 + y/400 - 32045
	if utc.Hour() < 12 {
		jdn--
	}
	return jdn
}

// JulianDate reports the Julian Date (which includes time as a fraction) for t.
func JulianDate(t time.Time) float32 {
	utc := t.UTC()
	jdn := JulianDayNumber(t)
	if utc.Hour() < 12 {
		jdn++
	}
	return float32(jdn) + (float32(utc.Hour())-12.0)/24.0 +
		float32(utc.Minute())/1440.0 + float32(utc.Second())/86400.0
}

// WorkdayFn reports whether the given date is a workday.
// This is useful for situations where work days change throughout the year.
//
// If your workdays are fixed (Mon-Fri for example) then a WorkdayFn
// is not necessary and you can use cal.SetWorkday() instead.
type WorkdayFn func(date time.Time) bool

// Calendar represents a yearly calendar with a list of holidays.
type Calendar struct {
	holidays    [13][]Holiday // 0 for offset based holidays, 1-12 for month based
	workday     [7]bool       // flags to indicate a day of the week is a workday
	WorkdayFunc WorkdayFn     // optional function to override workday flags
	Observed    ObservedRule
}

// NewCalendar creates a new Calendar with no holidays defined
// and work days of Monday through Friday.
func NewCalendar() *Calendar {
	c := &Calendar{}
	for i := range c.holidays {
		c.holidays[i] = make([]Holiday, 0, 2)
	}
	c.workday[time.Monday] = true
	c.workday[time.Tuesday] = true
	c.workday[time.Wednesday] = true
	c.workday[time.Thursday] = true
	c.workday[time.Friday] = true
	return c
}

// AddHoliday adds a holiday to the calendar's list.
func (c *Calendar) AddHoliday(h ...Holiday) {
	for _, hd := range h {
		c.holidays[hd.Month] = append(c.holidays[hd.Month], hd)
	}
}

// SetWorkday changes the given day's status as a standard working day
func (c *Calendar) SetWorkday(day time.Weekday, workday bool) {
	c.workday[day] = workday
}

// IsHoliday reports whether a given date is a holiday. It does not account
// for the observation of holidays on alternate days.
func (c *Calendar) IsHoliday(date time.Time) bool {
	idx := date.Month()
	for i := range c.holidays[idx] {
		if c.holidays[idx][i].matches(date) {
			return true
		}
	}
	for i := range c.holidays[0] {
		if c.holidays[0][i].matches(date) {
			return true
		}
	}
	return false
}

// IsWorkday reports whether a given date is a work day (business day).
func (c *Calendar) IsWorkday(date time.Time) bool {
	day := date.Weekday()

	var workday bool
	if c.WorkdayFunc == nil {
		workday = c.workday[day]
	} else {
		workday = c.WorkdayFunc(date)
	}

	if !workday || c.IsHoliday(date) {
		return false
	}

	if c.Observed == ObservedExact {
		return true
	}

	if c.Observed == ObservedMonday && day == time.Monday {
		sun := date.AddDate(0, 0, -1)
		sat := date.AddDate(0, 0, -2)
		return !c.IsHoliday(sat) && !c.IsHoliday(sun)
	} else if c.Observed == ObservedNearest {
		if day == time.Friday {
			sat := date.AddDate(0, 0, 1)
			return !c.IsHoliday(sat)
		} else if day == time.Monday {
			sun := date.AddDate(0, 0, -1)
			return !c.IsHoliday(sun)
		}
	}

	return true
}

// countWorkdays reports the number of workdays from the given date to the end
// of the month.
func (c *Calendar) countWorkdays(dt time.Time, month time.Month) int {
	n := 0
	for ; month == dt.Month(); dt = dt.AddDate(0, 0, 1) {
		if c.IsWorkday(dt) {
			n++
		}
	}
	return n
}

// Workdays reports the total number of workdays for the given year and month.
func (c *Calendar) Workdays(year int, month time.Month) int {
	return c.countWorkdays(time.Date(year, month, 1, 12, 0, 0, 0, time.UTC), month)
}

// WorkdaysRemain reports the total number of remaining workdays in the month
// for the given date.
func (c *Calendar) WorkdaysRemain(date time.Time) int {
	return c.countWorkdays(date.AddDate(0, 0, 1), date.Month())
}

// WorkdayN reports the day of the month that corresponds to the nth workday
// for the given year and month.
//
// The value of n affects the direction of counting:
//   n > 0: counting begins at the first day of the month.
//   n == 0: the result is always 0.
//   n < 0: counting begins at the end of the month.
func (c *Calendar) WorkdayN(year int, month time.Month, n int) int {
	var date time.Time
	var add int
	if n == 0 {
		return 0
	}

	if n > 0 {
		date = time.Date(year, month, 1, 12, 0, 0, 0, time.UTC)
		add = 1
	} else {
		date = time.Date(year, month+1, 1, 12, 0, 0, 0, time.UTC).AddDate(0, 0, -1)
		add = -1
		n = -n
	}

	ndays := 0
	for ; month == date.Month(); date = date.AddDate(0, 0, add) {
		if c.IsWorkday(date) {
			ndays++
			if ndays == n {
				return date.Day()
			}
		}
	}
	return 0
}

// WorkdaysFrom reports the date of a workday that is offset days
// away from start.
//
// If n > 0, then the date returned is start + offset workdays.
// If n == 0, then the date is returned unchanged.
// If n < 0, then the date returned is start - offset workdays.
func (c *Calendar) WorkdaysFrom(start time.Time, offset int) time.Time {
	date := start
	var add int

	if offset == 0 {
		return start
	}

	if offset > 0 {
		add = 1
	} else {
		add = -1
		offset = -offset
	}

	for ndays := 0; ndays < offset; {
		date = date.AddDate(0, 0, add)
		if c.IsWorkday(date) {
			ndays++
		}
	}

	return date
}

// CountHolidayHoursWithOffset returns the number of working hours in a range starting from the consumed start date
// to the end date set by the offset
func (c *Calendar) CountHolidayHoursWithOffset(start time.Time, offsetHour int) int {
	days := int(math.Ceil(float64(offsetHour) / float64(24)))

	holidayHours := 0
	day := 0
	for day <= days {
		date := start.AddDate(0, 0, day)
		if !c.IsWorkday(date) {
			holidayHours += 24
			days++
		}
		day++
	}

	return holidayHours
}

//CountWorkdays return amount of workdays between start and end dates
func (c *Calendar) CountWorkdays(start, end time.Time) int64 {
	factor := 1
	if end.Before(start) {
		factor = -1
		start, end = end, start
	}
	result := 0
	var i time.Time
	for i = start; i.Before(end); i = i.AddDate(0, 0, 1) {
		if c.IsWorkday(i) {
			result++
		}
	}
	if i.Equal(end) && c.IsWorkday(end) {
		result++
	}
	return int64(factor * result)
}

// AddSkipNonWorkdays returns start time plus d working duration
func (c *Calendar) AddSkipNonWorkdays(start time.Time, d time.Duration) time.Time {
	const day = 24 * time.Hour
	s := start
	for {
		for !c.IsWorkday(s) {
			s = s.Add(day)
		}

		if d >= day {
			s = s.Add(day)
			d = d - day
		} else if d > 0 {
			s = s.Add(d)
			d = 0
		} else {
			break
		}
	}
	return s
}

// SubSkipNonWorkdays  returns start time minus d working duration
func (c *Calendar) SubSkipNonWorkdays(start time.Time, d time.Duration) time.Time {
	const day = 24 * time.Hour * -1
	s := start
	for {
		for !c.IsWorkday(s) {
			s = s.Add(day)
		}
		
		if (d >= day*-1){
			s = s.Add(day)
			d = d + day
		} else if d > 0 {
			s = s.Add(-d)
			d = 0
		} else {
				break
			}
	}
	return s
}
