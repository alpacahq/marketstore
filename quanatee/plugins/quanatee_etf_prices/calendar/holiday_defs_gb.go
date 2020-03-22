package cal

import "time"

// British holidays
var (
	GBNewYear       = NewHolidayFunc(calculateNewYearsHoliday)
	GBGoodFriday    = GoodFriday
	GBEasterMonday  = EasterMonday
	GBEarlyMay      = NewHolidayFloat(time.May, time.Monday, 1)
	GBSpringHoliday = NewHolidayFloat(time.May, time.Monday, -1)
	GBSummerHoliday = NewHolidayFloat(time.August, time.Monday, -1)
	GBChristmasDay  = Christmas
	GBBoxingDay     = Christmas2
)

// AddBritishHolidays adds all British holidays to the Calender
func AddBritishHolidays(c *Calendar) {
	c.AddHoliday(
		GBNewYear,
		GBGoodFriday,
		GBEasterMonday,
		GBEarlyMay,
		GBSpringHoliday,
		GBSummerHoliday,
		GBChristmasDay,
		GBBoxingDay,
	)
}

// NewYearsDay is the 1st of January unless the 1st is a Saturday or Sunday
// in which case it occurs on the following Monday.
func calculateNewYearsHoliday(year int, loc *time.Location) (time.Month, int) {
	day := time.Date(year, time.January, 1, 0, 0, 0, 0, loc)
	switch day.Weekday() {
	case time.Saturday:
		day = day.AddDate(0, 0, 2)
	case time.Sunday:
		day = day.AddDate(0, 0, 1)
	}
	return time.January, day.Day()
}
