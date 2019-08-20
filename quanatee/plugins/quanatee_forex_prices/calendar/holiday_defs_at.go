// (c) 2019 Paul Zeinlinger. Licensed under the BSD license (see LICENSE).

package cal

import "time"

// Holidays in Austria
var (
	ATNeujahr            = NewYear
	ATHeiligeDreiKoenige = NewHoliday(time.January, 6)
	ATOstermontag        = EasterMonday
	ATTagderArbeit       = NewHoliday(time.May, 1)
	ATChristiHimmelfahrt = NewHolidayFunc(calculateHimmelfahrt)
	ATPfingstmontag      = NewHolidayFunc(calculatePfingstMontag)
	ATFronleichnam       = NewHolidayFunc(calculateFronleichnam)
	ATMariaHimmelfahrt   = NewHoliday(time.August, 15)
	ATNationalfeiertag   = NewHoliday(time.October, 26)
	ATAllerheiligen      = NewHoliday(time.November, 1)
	ATMariaEmpfaengnis   = NewHoliday(time.December, 8)
	ATChristtag          = Christmas
	ATStefanitag         = Christmas2
)

// AddAustrianHolidays adds all Austrian holidays to the Calendar
func AddAustrianHolidays(c *Calendar) {
	c.AddHoliday(
		ATNeujahr,
		ATHeiligeDreiKoenige,
		ATOstermontag,
		ATTagderArbeit,
		ATChristiHimmelfahrt,
		ATPfingstmontag,
		ATFronleichnam,
		ATMariaHimmelfahrt,
		ATNationalfeiertag,
		ATAllerheiligen,
		ATMariaEmpfaengnis,
		ATChristtag,
		ATStefanitag,
	)
}
