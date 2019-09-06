package cal

import "time"

// Holidays in France
var (
	FRNouvelAn          = NewYear
	FRLundiDePâques     = EasterMonday
	FRFêteDuTravail     = ECBLabourDay
	FRArmistice1945     = NewHoliday(time.May, 8)
	FRJeudiDeLAscension = NewHolidayFunc(calculateJeudiDeLAscension)
	FRLundiDePentecôte  = NewHolidayFunc(calculateLundiDePentecôte)
	FRFêteNationale     = NewHoliday(time.July, 14)
	FRAssomption        = NewHoliday(time.August, 15)
	FRToussaint         = NewHoliday(time.November, 1)
	FRArmistice1918     = NewHoliday(time.November, 11)
	FRNoël              = Christmas
)

// AddFranceHolidays adds all France holidays to the Calendar
func AddFranceHolidays(c *Calendar) {
	c.AddHoliday(
		FRNouvelAn,
		FRLundiDePâques,
		FRFêteDuTravail,
		FRArmistice1945,
		FRJeudiDeLAscension,
		FRLundiDePentecôte,
		FRFêteNationale,
		FRAssomption,
		FRToussaint,
		FRArmistice1918,
		FRNoël,
	)
}

func calculateJeudiDeLAscension(year int, loc *time.Location) (time.Month, int) {
	easter := calculateEaster(year, loc)
	// 39 days after Easter Sunday
	t := easter.AddDate(0, 0, +39)
	return t.Month(), t.Day()
}

func calculateLundiDePentecôte(year int, loc *time.Location) (time.Month, int) {
	easter := calculateEaster(year, loc)
	// 50 days after Easter Sunday
	t := easter.AddDate(0, 0, +50)
	return t.Month(), t.Day()
}
