package cal

import "time"

// US holidays
var (
	USNewYear      = NewYear
	USMLK          = NewHolidayFloat(time.January, time.Monday, 3)
	USPresidents   = NewHolidayFloat(time.February, time.Monday, 3)
	USMemorial     = NewHolidayFloat(time.May, time.Monday, -1)
	USIndependence = NewHoliday(time.July, 4)
	USLabor        = NewHolidayFloat(time.September, time.Monday, 1)
	USColumbus     = NewHolidayFloat(time.October, time.Monday, 2)
	USVeterans     = NewHoliday(time.November, 11)
	USThanksgiving = NewHolidayFloat(time.November, time.Thursday, 4)
	USChristmas    = Christmas
)

// AddUsHolidays adds all US holidays to the Calendar
func AddUsHolidays(cal *Calendar) {
	cal.AddHoliday(
		USNewYear,
		USMLK,
		USPresidents,
		USMemorial,
		USIndependence,
		USLabor,
		USColumbus,
		USVeterans,
		USThanksgiving,
		USChristmas,
	)
}
