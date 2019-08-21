package cal

import (
	"testing"
	"time"
)

func TestNorwegianHolidays(t *testing.T) {
	c := NewCalendar()
	c.Observed = ObservedExact
	AddNorwegianHolidays(c)

	tests := []testStruct{
		{time.Date(2018, 1, 1, 12, 0, 0, 0, time.UTC), true, "Nyttårsdag"},
		{time.Date(2018, 3, 29, 12, 0, 0, 0, time.UTC), true, "Skjærtorsdag"},
		{time.Date(2018, 3, 30, 12, 0, 0, 0, time.UTC), true, "Langfredag"},
		{time.Date(2018, 4, 1, 12, 0, 0, 0, time.UTC), true, "1. påskedag"},
		{time.Date(2018, 4, 2, 12, 0, 0, 0, time.UTC), true, "2. påskedag"},
		{time.Date(2018, 5, 1, 12, 0, 0, 0, time.UTC), true, "Arbeidernes dag"},
		{time.Date(2018, 5, 10, 12, 0, 0, 0, time.UTC), true, "Kristi himmelfartsdag"},
		{time.Date(2018, 5, 17, 12, 0, 0, 0, time.UTC), true, "Grunnlovsdagen"},
		{time.Date(2018, 5, 20, 12, 0, 0, 0, time.UTC), true, "1. pinsedag"},
		{time.Date(2018, 5, 21, 12, 0, 0, 0, time.UTC), true, "2. pinsedag"},
		{time.Date(2018, 12, 25, 12, 0, 0, 0, time.UTC), true, "1. juledag"},
		{time.Date(2018, 12, 26, 12, 0, 0, 0, time.UTC), true, "2. juledag"},

		{time.Date(2019, 1, 1, 12, 0, 0, 0, time.UTC), true, "Nyttårsdag"},
		{time.Date(2019, 4, 18, 12, 0, 0, 0, time.UTC), true, "Skjærtorsdag"},
		{time.Date(2019, 4, 19, 12, 0, 0, 0, time.UTC), true, "Langfredag"},
		{time.Date(2019, 4, 21, 12, 0, 0, 0, time.UTC), true, "1. påskedag"},
		{time.Date(2019, 4, 22, 12, 0, 0, 0, time.UTC), true, "2. påskedag"},
		{time.Date(2019, 5, 1, 12, 0, 0, 0, time.UTC), true, "Arbeidernes dag"},
		{time.Date(2019, 5, 30, 12, 0, 0, 0, time.UTC), true, "Kristi himmelfartsdag"},
		{time.Date(2019, 5, 17, 12, 0, 0, 0, time.UTC), true, "Grunnlovsdagen"},
		{time.Date(2019, 6, 9, 12, 0, 0, 0, time.UTC), true, "1. pinsedag"},
		{time.Date(2019, 6, 10, 12, 0, 0, 0, time.UTC), true, "2. pinsedag"},
		{time.Date(2019, 12, 25, 12, 0, 0, 0, time.UTC), true, "1. juledag"},
		{time.Date(2019, 12, 26, 12, 0, 0, 0, time.UTC), true, "2. juledag"},

		// Negative test
		{time.Date(2018, 10, 31, 12, 0, 0, 0, time.UTC), false, "Halloween"},
	}

	for _, test := range tests {
		got := c.IsHoliday(test.t)
		if got != test.want {
			t.Errorf("got: %t for %s; want: %t (%s)", got, test.name, test.want, test.t)
		}
	}
}

func TestNorwegianHalfDays(t *testing.T) {
	c := NewCalendar()
	c.Observed = ObservedExact
	AddNorwegianHalfDays(c)

	tests := []testStruct{
		{time.Date(2018, 12, 24, 12, 0, 0, 0, time.UTC), true, "Julaften"},
		{time.Date(2018, 12, 31, 12, 0, 0, 0, time.UTC), true, "Nyttårsaften"},
		{time.Date(2019, 12, 24, 12, 0, 0, 0, time.UTC), true, "Julaften"},
		{time.Date(2019, 12, 31, 12, 0, 0, 0, time.UTC), true, "Nyttårsaften"},
	}

	for _, test := range tests {
		got := c.IsHoliday(test.t)
		if got != test.want {
			t.Errorf("got: %t for %s; want: %t (%s)", got, test.name, test.want, test.t)
		}
	}
}
