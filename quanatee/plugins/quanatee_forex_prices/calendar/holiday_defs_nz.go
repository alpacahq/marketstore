// (c) 2014 Rick Arnold. Licensed under the BSD license (see LICENSE).

package cal

import (
	"time"
)

// Holidays in Australia
var (
	NZNewYear         = NewHolidayFunc(calculateNewYearOceania)
	NZGoodFriday      = GoodFriday
	NZChristmasDay    = NewHolidayFunc(calculateOcenaniaChristmasDay)
	NZBoxingDays      = NewHolidayFunc(calculateOcenaniaBoxingDay)
	NZEasterMonday    = EasterMonday
	NZAnzacDay        = NewHolidayFunc(calculateNZAnzacDay)
	NZQueensBirthday  = NewHolidayFunc(calculateQueensBirthday)
	NZLabourDay       = NewHolidayFunc(calculateLabourDay)
	NZDayAfterNewYear = NewHolidayFunc(calculateDayAfterNewYear)
	NZWanagiDay       = NewHolidayFunc(calculateWarangiDay)
)

// AddNewZealandHoliday adds all New Zeland holidays
func AddNewZealandHoliday(c *Calendar) {
	c.AddHoliday(
		NZNewYear,
		NZDayAfterNewYear,
		NZGoodFriday,
		NZEasterMonday,
		NZAnzacDay,
		NZChristmasDay,
		NZBoxingDays,
		NZQueensBirthday,
		NZLabourDay,
		NZWanagiDay,
	)
}

// Holidays associated with the start of the modern Gregorian calendar.
//
// New Zealanders celebrate New Years Day and The Day After New Years Day,
// if either of these holidays occur on a weekend, the dates need to be adjusted.
//
// https://en.wikipedia.org/wiki/Public_holidays_in_New_Zealand#Statutory_holidays
// http://www.timeanddate.com/holidays/new-zealand/new-year-day
// http://www.timeanddate.com/holidays/new-zealand/day-after-new-years-day
// http://employment.govt.nz/er/holidaysandleave/publicholidays/mondayisation.asp
func calculateDayAfterNewYear(year int, loc *time.Location) (time.Month, int) {
	d := time.Date(year, time.January, 2, 0, 0, 0, 0, loc)
	wd := d.Weekday()

	// Saturday
	if wd == 6 {
		d = d.AddDate(0, 0, 2)
	}

	// Sunday
	if wd == 0 {
		d = d.AddDate(0, 0, 2)
	}

	// Monday
	if wd == 1 {
		d = d.AddDate(0, 0, 1)
	}

	return d.Month(), d.Day()
}

// ANZAC Day.
//
// Anzac Day is a national day of remembrance in Australia and New Zealand that broadly commemorates all Australians
// and New Zealanders "who served and died in all wars, conflicts, and peacekeeping operations"
// Observed on 25 April each year.
//
// https://www.officeholidays.com/countries/new_zealand/anzac-day.php
func calculateNZAnzacDay(year int, loc *time.Location) (time.Month, int) {
	d := time.Date(year, time.April, 25, 0, 0, 0, 0, loc)

	return d.Month(), d.Day()
}

// Queens Birthday.
//
// The official head of state of New Zealand is the Monarch of the Commonwealth Realms.
// The monarch's birthday is officially celebrated in many parts of New Zealand.
// On her accession in 1952 Queen Elizabeth II was proclaimed in New Zealand ‘Queen of this Realm and all her
// other Realms’.Her representative in New Zealand, the governor general, has symbolic and ceremonial roles
// and is not involved in the day-to-day running of the government, which is the domain of the prime minister.
//
// Her actual birthday is on April 21, but it's celebrated as a public holiday on the first Monday of June.
//
// http://www.timeanddate.com/holidays/new-zealand/queen-birthday
func calculateQueensBirthday(year int, loc *time.Location) (time.Month, int) {
	firstJuneDay := time.Date(year, time.June, 1, 0, 0, 0, 0, loc)
	weekDays := 7

	for i := 0; i < weekDays; i++ {
		d := firstJuneDay.AddDate(0, 0, i)

		// Monday
		if d.Weekday() == 1 {
			return d.Month(), d.Day()
		}
	}

	return firstJuneDay.Month(), firstJuneDay.Day()
}

// During the 19th century, workers in New Zealand tried to claim the right for an 8-hour working day.
// In 1840 carpenter Samuel Parnell fought for this right in Wellington, NZ, and won.
// Labour Day was first celebrated in New Zealand on October 28, 1890, when thousands of workers paraded in the
// main city centres.
// Government employees were given the day off to attend the parades and many businesses closed for at least part
// of the day.
//
// The first official Labour Day public holiday in New Zealand was celebrated on the
// second Wednesday in October in 1900. The holiday was moved to the fourth Monday of October in 1910
// has remained on this date since then.
//
// http://www.timeanddate.com/holidays/new-zealand/labour-day
func calculateLabourDay(year int, loc *time.Location) (time.Month, int) {
	octoberDays := 31
	firstDayOctober := time.Date(year, time.October, 1, 0, 0, 0, 0, loc)

	wedNr := 0
	monNr := 0
	for i := 0; i < octoberDays; i++ {
		d := firstDayOctober.AddDate(0, 0, i)
		if d.Weekday() == 1 {
			monNr++
		}

		if d.Weekday() == 3 {
			wedNr++
		}

		// 4th Monday  after 1910
		if d.Year() > 1910 && monNr == 4 {
			return d.Month(), d.Day()
		}

		// 2th Wednesday before 1910
		if d.Year() < 1910 && wedNr == 2 {
			return d.Month(), d.Day()
		}
	}

	return firstDayOctober.Month(), octoberDays
}

// Waitangi Day.
//
// Waitangi Day (named after Waitangi, where the Treaty of Waitangi was first signed)
// commemorates a significant day in the history of New Zealand. It is observed as a public holiday each
// year on 6 February to celebrate the signing of the Treaty of Waitangi, New Zealand's founding document,
// on that date in 1840. In recent legislation, if 6 February falls on a Saturday or Sunday,
// the Monday that immediately follows becomes a public holiday.
//
// https://en.wikipedia.org/wiki/Waitangi_Day
func calculateWarangiDay(year int, loc *time.Location) (time.Month, int) {
	d := time.Date(year, time.February, 6, 0, 0, 0, 0, loc)

	// Sunday
	if d.Weekday() == 0 {
		d = d.AddDate(0, 0, 1)
	}

	// Saturday
	if d.Weekday() == 6 {
		d = d.AddDate(0, 0, 2)
	}

	return d.Month(), d.Day()
}
