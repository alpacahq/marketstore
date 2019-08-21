package cal

import (
	"testing"
	"time"
)

type testStruct struct {
	t    time.Time
	want bool
	name string
}

func TestGermanHolidays(t *testing.T) {
	c := NewCalendar()
	c.Observed = ObservedExact
	AddGermanHolidays(c)

	tests := []testStruct{
		{time.Date(2016, 1, 1, 12, 0, 0, 0, time.UTC), true, "Neujahr"},
		{time.Date(2016, 3, 25, 12, 0, 0, 0, time.UTC), true, "Karfreitag"},
		{time.Date(2016, 3, 28, 12, 0, 0, 0, time.UTC), true, "Ostermontag"},
		{time.Date(2016, 5, 1, 12, 0, 0, 0, time.UTC), true, "Tag der Arbeit"},
		{time.Date(2016, 5, 5, 12, 0, 0, 0, time.UTC), true, "Himmelfahrt"},
		{time.Date(2000, 6, 1, 12, 0, 0, 0, time.UTC), true, "Himmelfahrt"},
		{time.Date(2016, 5, 16, 12, 0, 0, 0, time.UTC), true, "Pfingstmontag"},
		{time.Date(2000, 6, 12, 12, 0, 0, 0, time.UTC), true, "Pfingstmontag"},
		{time.Date(2016, 10, 3, 12, 0, 0, 0, time.UTC), true, "Tag der deutschen Einheit"},
		{time.Date(2016, 12, 25, 12, 0, 0, 0, time.UTC), true, "1. Weihnachtstag"},
		{time.Date(2016, 12, 26, 12, 0, 0, 0, time.UTC), true, "2. Weihnachtstag"},

		{time.Date(2017, 1, 1, 12, 0, 0, 0, time.UTC), true, "Neujahr"},
	}

	for _, test := range tests {
		got := c.IsHoliday(test.t)
		if got != test.want {
			t.Errorf("got: %t for %s; want: %t (%s)", got, test.name, test.want, test.t)
		}
	}
}

func TestAddGermanyStateHolidays(t *testing.T) {
	tests := []struct {
		state string
		tests []testStruct
	}{
		{
			"BB",
			[]testStruct{
				{time.Date(2017, 4, 16, 12, 0, 0, 0, time.UTC), true, "Ostersonntag"},
				{time.Date(2017, 6, 4, 12, 0, 0, 0, time.UTC), true, "Pfingstsonntag"},
				{time.Date(2017, 10, 31, 12, 0, 0, 0, time.UTC), true, "Reformationstag"},
			},
		},
		{
			"BW",
			[]testStruct{
				{time.Date(2017, 1, 6, 12, 0, 0, 0, time.UTC), true, "Heilige Drei Könige"},
				{time.Date(2017, 6, 15, 12, 0, 0, 0, time.UTC), true, "Fronleichnam"},
				{time.Date(2017, 11, 1, 12, 0, 0, 0, time.UTC), true, "Allerheiligen"},
			},
		},
		{
			"BY",
			[]testStruct{
				{time.Date(2017, 1, 6, 12, 0, 0, 0, time.UTC), true, "Heilige Drei Könige"},
				{time.Date(2017, 6, 15, 12, 0, 0, 0, time.UTC), true, "Fronleichnam"},
				{time.Date(2017, 8, 15, 12, 0, 0, 0, time.UTC), true, "Mariä Himmelfahrt"},
				{time.Date(2017, 11, 1, 12, 0, 0, 0, time.UTC), true, "Allerheiligen"},
			},
		},
		{
			"HE",
			[]testStruct{
				{time.Date(2017, 6, 15, 12, 0, 0, 0, time.UTC), true, "Fronleichnam"},
			},
		},
		{
			"MV",
			[]testStruct{
				{time.Date(2017, 10, 31, 12, 0, 0, 0, time.UTC), true, "Reformationstag"},
			},
		},
		{
			"NW",
			[]testStruct{
				{time.Date(2017, 6, 15, 12, 0, 0, 0, time.UTC), true, "Fronleichnam"},
				{time.Date(2017, 11, 1, 12, 0, 0, 0, time.UTC), true, "Allerheiligen"},
			},
		},
		{
			"RP",
			[]testStruct{
				{time.Date(2017, 6, 15, 12, 0, 0, 0, time.UTC), true, "Fronleichnam"},
				{time.Date(2017, 11, 1, 12, 0, 0, 0, time.UTC), true, "Allerheiligen"},
			},
		},
		{
			"SA",
			[]testStruct{
				{time.Date(2017, 10, 31, 12, 0, 0, 0, time.UTC), true, "Reformationstag"},
				{time.Date(2017, 11, 22, 12, 0, 0, 0, time.UTC), true, "Buß- und Bettag"},
			},
		},
		{
			"SL",
			[]testStruct{
				{time.Date(2017, 6, 15, 12, 0, 0, 0, time.UTC), true, "Fronleichnam"},
				{time.Date(2017, 8, 15, 12, 0, 0, 0, time.UTC), true, "Mariä Himmelfahrt"},
				{time.Date(2017, 11, 1, 12, 0, 0, 0, time.UTC), true, "Allerheiligen"},
			},
		},
		{
			"ST",
			[]testStruct{
				{time.Date(2017, 1, 6, 12, 0, 0, 0, time.UTC), true, "Heilige Drei Könige"},
				{time.Date(2017, 10, 31, 12, 0, 0, 0, time.UTC), true, "Reformationstag"},
			},
		},
		{
			"TH",
			[]testStruct{
				{time.Date(2017, 10, 31, 12, 0, 0, 0, time.UTC), true, "Reformationstag"},
			},
		},
	}

	for _, test := range tests {
		c := NewCalendar()
		c.Observed = ObservedExact
		AddGermanyStateHolidays(c, test.state)

		for _, day := range test.tests {
			got := c.IsHoliday(day.t)
			if got != day.want {
				t.Errorf("state: %s got: %t for %s; want: %t (%s)", test.state, got, day.name, day.want, day.t)
			}
		}
	}
}
