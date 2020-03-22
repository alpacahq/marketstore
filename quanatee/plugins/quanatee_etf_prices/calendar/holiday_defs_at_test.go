package cal

import (
	"testing"
	"time"
)

func TestAustrianHolidays(t *testing.T) {
	c := NewCalendar()
	c.Observed = ObservedExact
	AddAustrianHolidays(c)

	tests := []testStruct{
		{time.Date(2019, 1, 1, 12, 0, 0, 0, time.UTC), true, "Neujahr"},
		{time.Date(2019, 4, 22, 12, 0, 0, 0, time.UTC), true, "Ostermontag"},
		{time.Date(2016, 5, 1, 12, 0, 0, 0, time.UTC), true, "Tag der Arbeit"},
		{time.Date(2019, 5, 30, 12, 0, 0, 0, time.UTC), true, "Christi Himmelfahrt"},
		{time.Date(2019, 6, 10, 12, 0, 0, 0, time.UTC), true, "Pfingstmontag"},
		{time.Date(2000, 6, 12, 12, 0, 0, 0, time.UTC), true, "Pfingstmontag"},
		{time.Date(2019, 6, 20, 12, 0, 0, 0, time.UTC), true, "Fronleichnam"},
		{time.Date(2019, 8, 15, 12, 0, 0, 0, time.UTC), true, "Maria Himmelfahrt"},
		{time.Date(2016, 10, 26, 12, 0, 0, 0, time.UTC), true, "Nationalfeiertag"},
		{time.Date(2016, 11, 1, 12, 0, 0, 0, time.UTC), true, "Allerheiligen"},
		{time.Date(2019, 12, 8, 12, 0, 0, 0, time.UTC), true, "Maria Empfängnis"},
		{time.Date(2016, 12, 25, 12, 0, 0, 0, time.UTC), true, "Christtag"},
		{time.Date(2016, 12, 26, 12, 0, 0, 0, time.UTC), true, "Stefanitag"},

		{time.Date(2017, 1, 1, 12, 0, 0, 0, time.UTC), true, "Neujahr"},
	}

	for _, test := range tests {
		got := c.IsHoliday(test.t)
		if got != test.want {
			t.Errorf("got: %t for %s; want: %t (%s)", got, test.name, test.want, test.t)
		}
	}
}
