// (c) 2014, 2017 Rick Arnold. Licensed under the BSD license (see LICENSE).

package cal

import "time"

// Holidays (official and traditional) in Denmark
// Reference https://da.wikipedia.org/wiki/Helligdag#Danske_helligdage
var (
	DKNytaarsdag           = NewYear
	DKSkaertorsdag         = NewHolidayFunc(calculateSkaertorsdag)
	DKLangfredag           = GoodFriday
	DKPaaskedag            = NewHolidayFunc(calculatePaaskedag)
	DKAndenPaaskedag       = EasterMonday
	DKStoreBededag         = NewHolidayFunc(calculateStoreBededag)
	DKKristiHimmelfartsdag = NewHolidayFunc(calculateKristiHimmelfartsdag)
	DKPinsedag             = NewHolidayFunc(calculatePinsedag)
	DKAndenPinsedag        = NewHolidayFunc(calculateAndenPinsedag)
	DKGrundlovsdag         = NewHoliday(time.June, 5)
	DKJuleaften            = NewHoliday(time.December, 24)
	DKJuledag              = Christmas
	DKAndenJuledag         = Christmas2
	DKNytaarsaften         = NewHoliday(time.December, 31)
)

// AddDanishHolidays adds all Danish holidays to the Calendar
func AddDanishHolidays(c *Calendar) {
	c.AddHoliday(
		DKNytaarsdag,
		DKSkaertorsdag,
		DKLangfredag,
		DKPaaskedag,
		DKAndenPaaskedag,
		DKStoreBededag,
		DKKristiHimmelfartsdag,
		DKPinsedag,
		DKAndenPinsedag,
		DKJuledag,
		DKAndenJuledag,
	)
}

// AddDanishTraditions adds Grundlovsdag (Constitution Day), Christmas
// Eve, and New Years Eve which are not official holidays.
func AddDanishTraditions(c *Calendar) {
	c.AddHoliday(
		DKGrundlovsdag,
		DKJuleaften,
		DKNytaarsaften,
	)
}

func calculateSkaertorsdag(year int, loc *time.Location) (time.Month, int) {
	easter := calculateEaster(year, loc)
	em := easter.AddDate(0, 0, -3)
	// 3 days before Easter Sunday
	return em.Month(), em.Day()
}

func calculatePaaskedag(year int, loc *time.Location) (time.Month, int) {
	easter := calculateEaster(year, loc)
	return easter.Month(), easter.Day()
}

func calculateStoreBededag(year int, loc *time.Location) (time.Month, int) {
	easter := calculateEaster(year, loc)
	// 26 days after Easter Sunday
	em := easter.AddDate(0, 0, +26)
	return em.Month(), em.Day()
}

func calculateKristiHimmelfartsdag(year int, loc *time.Location) (time.Month, int) {
	easter := calculateEaster(year, loc)
	// 39 days after Easter Sunday
	em := easter.AddDate(0, 0, +39)
	return em.Month(), em.Day()
}

func calculatePinsedag(year int, loc *time.Location) (time.Month, int) {
	easter := calculateEaster(year, loc)
	// 49 days after Easter Sunday
	em := easter.AddDate(0, 0, +49)
	return em.Month(), em.Day()
}

func calculateAndenPinsedag(year int, loc *time.Location) (time.Month, int) {
	easter := calculateEaster(year, loc)
	// 50 days after Easter Sunday
	em := easter.AddDate(0, 0, +50)
	return em.Month(), em.Day()
}
