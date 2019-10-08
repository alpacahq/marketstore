package cal

import "time"

// Holidays in Belgium
var (
	BENieuwjaar                = NewYear
	BEPaasmaandag              = EasterMonday
	BEDagVanDeArbeid           = ECBLabourDay
	BEOnzeLieveHeerHemelvaart  = NewHolidayFunc(calculateOnzeLieveHeerHemelvaart)
	BEPinkstermaandag          = NewHolidayFunc(calculatePinkstermaandag)
	BENationaleFeestdag        = NewHoliday(time.July, 21)
	BEOnzeLieveVrouwHemelvaart = NewHoliday(time.August, 15)
	BEAllerheiligen            = NewHoliday(time.November, 1)
	BEWapenstilstand           = NewHoliday(time.November, 11)
	BEKerstmis                 = Christmas
)

// AddBelgiumHolidays adds all Belgium holidays to the Calendar
func AddBelgiumHolidays(c *Calendar) {
	c.AddHoliday(
		BENieuwjaar,
		BEPaasmaandag,
		BEDagVanDeArbeid,
		BEOnzeLieveHeerHemelvaart,
		BEPinkstermaandag,
		BENationaleFeestdag,
		BEOnzeLieveVrouwHemelvaart,
		BEAllerheiligen,
		BEWapenstilstand,
		BEKerstmis,
	)
}

func calculateOnzeLieveHeerHemelvaart(year int, loc *time.Location) (time.Month, int) {
	easter := calculateEaster(year, loc)
	// 39 days after Easter Sunday
	t := easter.AddDate(0, 0, +39)
	return t.Month(), t.Day()
}

func calculatePinkstermaandag(year int, loc *time.Location) (time.Month, int) {
	easter := calculateEaster(year, loc)
	// 50 days after Easter Sunday
	t := easter.AddDate(0, 0, +50)
	return t.Month(), t.Day()
}
