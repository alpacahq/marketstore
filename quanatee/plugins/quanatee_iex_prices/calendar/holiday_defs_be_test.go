package cal

import (
	"testing"
	"time"
)

func TestBelgiumHolidays(t *testing.T) {
	c := NewCalendar()
	c.Observed = ObservedExact
	AddBelgiumHolidays(c)

	tests := []testStruct{
		{time.Date(2017, 1, 1, 12, 0, 0, 0, time.UTC), true, "Nieuwjaar"},
		{time.Date(2017, 4, 17, 12, 0, 0, 0, time.UTC), true, "Paasmaandag"},
		{time.Date(2017, 5, 1, 12, 0, 0, 0, time.UTC), true, "DagVanDeArbeid"},
		{time.Date(2017, 5, 25, 12, 0, 0, 0, time.UTC), true, "OnzeLieveHeerHemelvaart"},
		{time.Date(2017, 6, 5, 12, 0, 0, 0, time.UTC), true, "Pinkstermaandag"},
		{time.Date(2017, 7, 21, 12, 0, 0, 0, time.UTC), true, "NationaleFeestdag"},
		{time.Date(2017, 8, 15, 12, 0, 0, 0, time.UTC), true, "OnzeLieveVrouwHemelvaart"},
		{time.Date(2017, 11, 1, 12, 0, 0, 0, time.UTC), true, "Allerheiligen"},
		{time.Date(2017, 11, 11, 12, 0, 0, 0, time.UTC), true, "Wapenstilstand"},
		{time.Date(2017, 12, 25, 12, 0, 0, 0, time.UTC), true, "Kerstmis"},

		{time.Date(2018, 1, 1, 12, 0, 0, 0, time.UTC), true, "Nieuwjaar"},
		{time.Date(2018, 4, 2, 12, 0, 0, 0, time.UTC), true, "Paasmaandag"},
		{time.Date(2018, 5, 1, 12, 0, 0, 0, time.UTC), true, "DagVanDeArbeid"},
		{time.Date(2018, 5, 10, 12, 0, 0, 0, time.UTC), true, "OnzeLieveHeerHemelvaart"},
		{time.Date(2018, 5, 21, 12, 0, 0, 0, time.UTC), true, "Pinkstermaandag"},
		{time.Date(2018, 7, 21, 12, 0, 0, 0, time.UTC), true, "NationaleFeestdag"},
		{time.Date(2018, 8, 15, 12, 0, 0, 0, time.UTC), true, "OnzeLieveVrouwHemelvaart"},
		{time.Date(2018, 11, 1, 12, 0, 0, 0, time.UTC), true, "Allerheiligen"},
		{time.Date(2018, 11, 11, 12, 0, 0, 0, time.UTC), true, "Wapenstilstand"},
		{time.Date(2018, 12, 25, 12, 0, 0, 0, time.UTC), true, "Kerstmis"},
	}

	for _, test := range tests {
		got := c.IsHoliday(test.t)
		if got != test.want {
			t.Errorf("got: %t for %s; want: %t (%s)", got, test.name, test.want, test.t)
		}
	}
}
