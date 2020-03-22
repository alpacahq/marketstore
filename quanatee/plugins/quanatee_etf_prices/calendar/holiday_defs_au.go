// (c) 2014 Rick Arnold. Licensed under the BSD license (see LICENSE).

package cal

import (
	"time"
)

// Holidays in Australia
var (
	AUNewYear        = NewHolidayFunc(calculateNewYearOceania)
	AUAustralianDay  = NewHoliday(time.January, 26)
	AUGoodFriday     = GoodFriday
	AUChristmasDay   = NewHolidayFunc(calculateOcenaniaChristmasDay)
	AUBoxingDays     = Christmas2
	AUEasterMonday   = EasterMonday
	AUAnzacDay       = NewHolidayFunc(calculateAnzacDay)
	AUQueenBirthDay = NewHolidayFunc(calculateQueenBirthDay)
	AULabourDay      = NewHolidayFunc(calculateAULabourDay)
)

// AddAustralianHolidays adds all Australian holidays
func AddAustralianHolidays(c *Calendar) {
	c.AddHoliday(
		AUNewYear,
		AUAustralianDay,
		AUGoodFriday,
		AUEasterMonday,
		AUAnzacDay,
		AUChristmasDay,
		AUBoxingDays,
		AUQueenBirthDay,
		AULabourDay,
	)
}

//  Holidays associated with the start of the modern Gregorian calendar.
//
//  New Year's Day is on January 1 and is the first day of a new year in the Gregorian calendar,
//  which is used in Australia and many other countries. Due to its geographical position close
//  to the International Date Line, Australia is one of the first countries in the world to welcome the New Year.
//  If it falls on a weekend an additional public holiday is held on the next available weekday.
//
//  https://www.timeanddate.com/holidays/australia/new-year-day
func calculateNewYearOceania(year int, loc *time.Location) (time.Month, int) {
	d := time.Date(year, time.January, 1, 0, 0, 0, 0, loc)
	d = closestMonday(d)

	return d.Month(), d.Day()
}

// closestMonday returns the closest Monday from a giving date
func closestMonday(date time.Time) time.Time {
	wd := date.Weekday()
	if wd == 0 {
		date = date.AddDate(0, 0, 1)
	}

	if wd == 6 {
		date = date.AddDate(0, 0, 2)
	}

	return date
}

// Anzac Day is a national day of remembrance in Australia and New Zealand that broadly commemorates all Australians
// and New Zealanders "who served and died in all wars, conflicts, and peacekeeping operations"
// Observed on 25 April each year. Unlike most other Australian public holidays, If it falls on a weekend it is NOT moved
// to the next available weekday, nor is there an additional public holiday held. However, if it clashes with Easter,
// an additional public holiday is held for Easter.
//
// https://en.wikipedia.org/wiki/Anzac_Day
// https://www.timeanddate.com/holidays/australia/anzac-day
func calculateAnzacDay(year int, loc *time.Location) (time.Month, int) {
	d := time.Date(year, time.April, 25, 0, 0, 0, 0, loc)
	easter := calculateEaster(year, loc)
	emMonth, emDay := calculateEasterMonday(year, loc)
	easterMonday := time.Date(year, emMonth, emDay, 0, 0, 0, 0, loc)

	if d.Equal(easter) || d.Equal(easterMonday) {
		d = easterMonday.AddDate(0, 0, 1)
	}

	return d.Month(), d.Day()
}

// Christmas Day
//
// Christmas day is a public holidays in Australia,
// if it fall on the weekend an additional public holiday is held on the next available weekday.
//
// https://www.timeanddate.com/holidays/australia/christmas-day-holiday
//
func calculateOcenaniaChristmasDay(year int, loc *time.Location) (time.Month, int) {
	d := time.Date(year, time.December, 25, 0, 0, 0, 0, loc)
	wd := d.Weekday()
	if wd == 0 || wd == 6 {
		d = d.AddDate(0, 0, 2)
	}

	return d.Month(), d.Day()
}

// Boxing Day
//
// Boxing day is a public holidays in Australia,
// if it fall on the weekend an additional public holiday is held on the next available weekday.
//
// https://www.timeanddate.com/holidays/australia/boxing-day
//
func calculateOcenaniaBoxingDay(year int, loc *time.Location) (time.Month, int) {
	d := time.Date(year, time.December, 26, 0, 0, 0, 0, loc)
	wd := d.Weekday()
	if wd == 0 || wd == 6 {
		d = d.AddDate(0, 0, 2)
	}

	return d.Month(), d.Day()
}

// Queen's birth Day
//
// The Queen’s Birthday is a public holiday celebrated in most states and territories on the second Monday in June,
// making for a much-looked-forward-to June long weekend.
// WA QLD different
//
// https://publicholidays.com.au/queens-birthday/
// WA - Monday in last week of Sep.
// QLD -  1st. Monday of Oct
func calculateQueenBirthDay(year int, loc *time.Location) (time.Month, int) {
	if loc.String() == "Australia/West" || loc.String() == "Australia/Perth" {
		d := time.Date(year, time.September, 30, 0, 0, 0, 0, loc)

		wd := d.Weekday()
		if wd == 0 {
			d = d.AddDate(0, 0, -6)
		} else if wd == 1 {
		} else {
			d = d.AddDate(0, 0, -(int(d.Weekday()) - 1))
		}

		return d.Month(), d.Day()

	} else if loc.String() == "Australia/Queensland" {
		d := time.Date(year, time.October, 1, 0, 0, 0, 0, loc)

		wd := d.Weekday()
		if wd == 0 {
			d = d.AddDate(0, 0, 1)
		} else if wd == 1 {
		} else {
			d = d.AddDate(0, 0, 8-int(d.Weekday()))
		}

		return d.Month(), d.Day()

	} else {
		d := time.Date(year, time.June, 1, 0, 0, 0, 0, loc)

		wd := d.Weekday()
		if wd == 0 {
			d = d.AddDate(0, 0, 8)
		} else if wd == 1 {
			d = d.AddDate(0, 0, 8-int(d.Weekday()))
		} else {
			d = d.AddDate(0, 0, (8-int(d.Weekday()))+7)
		}

		return d.Month(), d.Day()

	}
}

// Australian Labour Day
//
// WA -  first Monday in March  Australia/Perth, Australia/West
// VIC, TAS - second Monday in March   Australia/Melbourne, Australia/Tasmania, Australia/Victoria
// QLD, NT - first Monday in May   Australia/Brisbane, Australia/Darwin, Australia/North
// ACT, NSW & SA - first Monday in Oct
func calculateAULabourDay(year int, loc *time.Location) (time.Month, int) {
	if loc.String() == "Australia/West" || loc.String() == "Australia/Perth" {
		d := time.Date(year, time.March, 1, 0, 0, 0, 0, loc)

		wd := d.Weekday()
		if wd == 0 {
			d = d.AddDate(0, 0, 1)
		} else if wd == 1 {
		} else {
			d = d.AddDate(0, 0, 8-int(d.Weekday()))
		}

		return d.Month(), d.Day()

	} else if loc.String() == "Australia/Melbourne" || loc.String() == "Australia/Tasmania" {
		d := time.Date(year, time.March, 1, 0, 0, 0, 0, loc)

		wd := d.Weekday()
		if wd == 0 {
			d = d.AddDate(0, 0, 8)
		} else if wd == 1 {
			d = d.AddDate(0, 0, 8-int(d.Weekday()))
		} else {
			d = d.AddDate(0, 0, (8-int(d.Weekday()))+7)
		}

		return d.Month(), d.Day()

	} else if loc.String() == "Australia/Brisbane" || loc.String() == "Australia/Darwin" {
		d := time.Date(year, time.May, 1, 0, 0, 0, 0, loc)

		wd := d.Weekday()
		if wd == 0 {
			d = d.AddDate(0, 0, 1)
		} else if wd == 1 {
		} else {
			d = d.AddDate(0, 0, 8-int(d.Weekday()))
		}

		return d.Month(), d.Day()

	} else {
		d := time.Date(year, time.October, 1, 0, 0, 0, 0, loc)

		wd := d.Weekday()
		if wd == 0 {
			d = d.AddDate(0, 0, 1)
		} else if wd == 1 {
		} else {
			d = d.AddDate(0, 0, 8-int(d.Weekday()))
		}

		return d.Month(), d.Day()

	}

}
