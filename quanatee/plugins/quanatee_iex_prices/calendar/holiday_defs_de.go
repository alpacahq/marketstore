// (c) 2014 Rick Arnold. Licensed under the BSD license (see LICENSE).

package cal

import "time"

// Holidays in Germany
var (
	DENeujahr                  = NewYear
	DEHeiligeDreiKoenige       = NewHoliday(time.January, 6)
	DEInternationalerFrauentag = NewHoliday(time.March, 8)
	DEKarFreitag               = GoodFriday
	DEOstersonntag             = NewHolidayFunc(calculateOstersonntag)
	DEOstermontag              = EasterMonday
	DETagderArbeit             = NewHoliday(time.May, 1)
	DEChristiHimmelfahrt       = NewHolidayFunc(calculateHimmelfahrt)
	DEPfingstsonntag           = NewHolidayFunc(calculatePfingstSonntag)
	DEPfingstmontag            = NewHolidayFunc(calculatePfingstMontag)
	DEFronleichnam             = NewHolidayFunc(calculateFronleichnam)
	DEMariaHimmelfahrt         = NewHoliday(time.August, 15)
	DETagderDeutschenEinheit   = NewHoliday(time.October, 3)
	DEReformationstag          = NewHoliday(time.October, 31)
	DEReformationstag2017      = NewHolidayExact(time.October, 31, 2017)
	DEAllerheiligen            = NewHoliday(time.November, 1)
	DEBußUndBettag             = NewHolidayFunc(calculateBußUndBettag)
	DEErsterWeihnachtstag      = Christmas
	DEZweiterWeihnachtstag     = Christmas2
)

// AddGermanHolidays adds all German holidays to the Calendar
func AddGermanHolidays(c *Calendar) {
	c.AddHoliday(
		DENeujahr,
		DEKarFreitag,
		DEOstermontag,
		DETagderArbeit,
		DEChristiHimmelfahrt,
		DEPfingstmontag,
		DETagderDeutschenEinheit,
		DEErsterWeihnachtstag,
		DEZweiterWeihnachtstag,
	)
}

// AddGermanyStateHolidays adds german state holidays to the calendar
func AddGermanyStateHolidays(c *Calendar, state string) {
	switch state {
	case "BB": // Brandenburg
		c.AddHoliday(
			DEOstersonntag,
			DEPfingstsonntag,
			DEReformationstag,
		)
	case "BE": // Berlin
		c.AddHoliday(
			DEInternationalerFrauentag,
		)
	case "BW": // Baden-Württemberg
		c.AddHoliday(
			DEHeiligeDreiKoenige,
			DEFronleichnam,
			DEAllerheiligen,
			DEReformationstag2017,
		)
	case "BY": // Bayern
		c.AddHoliday(
			DEHeiligeDreiKoenige,
			DEFronleichnam,
			DEMariaHimmelfahrt,
			DEAllerheiligen,
			DEReformationstag2017,
		)
	case "HE": // Hessen
		c.AddHoliday(DEFronleichnam)
	case "MV": // Mecklenburg-Vorpommern
		c.AddHoliday(DEReformationstag)
	case "NW": // Nordrhein-Westfalen
		c.AddHoliday(
			DEFronleichnam,
			DEAllerheiligen,
			DEReformationstag2017,
		)
	case "RP": // Rheinland-Pfalz
		c.AddHoliday(
			DEFronleichnam,
			DEAllerheiligen,
			DEReformationstag2017,
		)
	case "SA": // Sachsen
		c.AddHoliday(
			DEFronleichnam,
			DEReformationstag,
			DEBußUndBettag,
		)
	case "SL": // Saarland
		c.AddHoliday(
			DEFronleichnam,
			DEAllerheiligen,
			DEMariaHimmelfahrt,
			DEReformationstag2017,
		)
	case "ST": // Sachen-Anhalt
		c.AddHoliday(
			DEHeiligeDreiKoenige,
			DEReformationstag,
		)
	case "TH": // Thüringen
		c.AddHoliday(
			DEFronleichnam,
			DEReformationstag,
		)
	}
}

func calculateOstersonntag(year int, loc *time.Location) (time.Month, int) {
	easter := calculateEaster(year, loc)
	return easter.Month(), easter.Day()
}

func calculateHimmelfahrt(year int, loc *time.Location) (time.Month, int) {
	easter := calculateEaster(year, loc)
	// 39 days after Easter Sunday
	em := easter.AddDate(0, 0, +39)
	return em.Month(), em.Day()
}

func calculatePfingstSonntag(year int, loc *time.Location) (time.Month, int) {
	easter := calculateEaster(year, loc)
	// 50 days after Easter Sunday
	em := easter.AddDate(0, 0, +49)
	return em.Month(), em.Day()
}

func calculatePfingstMontag(year int, loc *time.Location) (time.Month, int) {
	easter := calculateEaster(year, loc)
	// 50 days after Easter Sunday
	em := easter.AddDate(0, 0, +50)
	return em.Month(), em.Day()
}

func calculateFronleichnam(year int, loc *time.Location) (time.Month, int) {
	easter := calculateEaster(year, loc)
	// 50 days after Easter Sunday
	em := easter.AddDate(0, 0, +60)
	return em.Month(), em.Day()
}

func calculateBußUndBettag(year int, loc *time.Location) (time.Month, int) {
	t := time.Date(year, 11, 23, 0, 0, 0, 0, loc)

	for i := -1; i > -10; i-- {
		d := t.Add(time.Hour * 24 * time.Duration(i))
		if d.Weekday() == time.Wednesday {
			t = d
			break
		}
	}
	return t.Month(), t.Day()
}
