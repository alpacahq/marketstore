// (c) 2014 Rick Arnold. Licensed under the BSD license (see LICENSE).

package cal

import "time"

// Holidays in Norway
// Reference https://no.wikipedia.org/wiki/Helligdager_i_Norge
var (
	NOFoersteNyttaarsdag   = NewYear
	NOSkjaertorsdag        = NewHolidayFunc(calculateSkaertorsdag)
	NOLangfredag           = GoodFriday
	NOFoerstePaaskedag     = NewHolidayFunc(calculatePaskdagen)
	NOAndrePaaskedag       = EasterMonday
	NOArbeiderenesdag      = NewHoliday(time.May, 1)
	NOGrunnlovsdag         = NewHoliday(time.May, 17)
	NOKristihimmelfartsdag = NewHolidayFunc(calculateKristiHimmelfardsdag)
	NOFoerstePinsedag      = NewHolidayFunc(calculatePingstdagen)
	NOAndrePinsedag        = NewHolidayFunc(calculateAndenPinsedag)
	NOFoersteJuledag       = Christmas
	NOAndreJuledag         = Christmas2
	// Half days
	NOJulaften      = NewHoliday(time.December, 24)
	NONyttaarsaften = NewHoliday(time.December, 31)
)

// AddNorwegianHolidays adds all Norwegian holidays to Calendar
func AddNorwegianHolidays(c *Calendar) {
	c.AddHoliday(
		NOFoersteNyttaarsdag,
		NOSkjaertorsdag,
		NOLangfredag,
		NOFoerstePaaskedag,
		NOAndrePaaskedag,
		NOArbeiderenesdag,
		NOGrunnlovsdag,
		NOKristihimmelfartsdag,
		NOFoerstePinsedag,
		NOAndrePinsedag,
		NOFoersteJuledag,
		NOAndreJuledag,
	)
}

// AddNorwegianHalfDays are note holidays, but often practiced as a half-business day.
func AddNorwegianHalfDays(c *Calendar) {
	c.AddHoliday(NOJulaften,
		NONyttaarsaften,
	)
}
