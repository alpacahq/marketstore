package cal

import "time"

// European Central Bank Target2 holidays
var (
	ECBGoodFriday       = GoodFriday
	ECBEasterMonday     = EasterMonday
	ECBNewYearsDay      = NewYear
	ECBLabourDay        = NewHoliday(time.May, 1)
	ECBChristmasDay     = Christmas
	ECBChristmasHoliday = Christmas2
)

// AddEcbHolidays adds all ECB Target2 holidays to the calendar
func AddEcbHolidays(c *Calendar) {
	c.AddHoliday(
		ECBGoodFriday,
		ECBEasterMonday,
		ECBNewYearsDay,
		ECBLabourDay,
		ECBChristmasDay,
		ECBChristmasHoliday,
	)
}
