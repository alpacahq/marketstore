package cal

import "time"

// Holidays in the Netherlands
var (
	NLNieuwjaar       = NewYear
	NLGoedeVrijdag    = GoodFriday
	NLPaasMaandag     = EasterMonday
	NLKoningsDag      = NewHolidayFunc(calculateKoningsDag)
	NLBevrijdingsDag  = NewHoliday(time.May, 5)
	NLHemelvaart      = DEChristiHimmelfahrt
	NLPinksterMaandag = DEPfingstmontag
	NLEersteKerstdag  = Christmas
	NLTweedeKerstdag  = Christmas2
)

// AddDutchHolidays adds all Dutch holidays to the Calendar
func AddDutchHolidays(c *Calendar) {
	c.AddHoliday(
		NLNieuwjaar,
		NLGoedeVrijdag,
		NLPaasMaandag,
		NLKoningsDag,
		NLBevrijdingsDag,
		NLHemelvaart,
		NLPinksterMaandag,
		NLEersteKerstdag,
		NLTweedeKerstdag,
	)
}

// KoningsDag (kingsday) is April 27th, 26th if the 27th is a Sunday
func calculateKoningsDag(year int, loc *time.Location) (time.Month, int) {
	koningsDag := time.Date(year, time.April, 27, 0, 0, 0, 0, loc)
	if koningsDag.Weekday() == time.Sunday {
		koningsDag = koningsDag.AddDate(0, 0, -1)
	}
	return koningsDag.Month(), koningsDag.Day()
}
