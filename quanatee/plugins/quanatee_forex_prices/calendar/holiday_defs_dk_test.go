package cal

import (
	"testing"
	"time"
)

func TestDanishHolidays(t *testing.T) {
	c := NewCalendar()
	c.Observed = ObservedExact
	AddDanishHolidays(c)

	tests := []testStruct{
		{time.Date(2016, 1, 1, 12, 0, 0, 0, time.UTC), true, "Nytårsdag"},
		{time.Date(2016, 3, 25, 12, 0, 0, 0, time.UTC), true, "Skærtorsdag"},
		{time.Date(2016, 3, 25, 12, 0, 0, 0, time.UTC), true, "Langfredagen"},
		{time.Date(2016, 3, 27, 12, 0, 0, 0, time.UTC), true, "Påskedag"},
		{time.Date(2016, 3, 28, 12, 0, 0, 0, time.UTC), true, "2. påskedag"},
		{time.Date(2016, 4, 22, 12, 0, 0, 0, time.UTC), true, "Store Bededag"},
		{time.Date(2016, 5, 5, 12, 0, 0, 0, time.UTC), true, "Kristi Himmelfartsdag"},
		{time.Date(2016, 5, 15, 12, 0, 0, 0, time.UTC), true, "Pinsedag"},
		{time.Date(2016, 5, 16, 12, 0, 0, 0, time.UTC), true, "2. pinsedag"},
		{time.Date(2016, 6, 5, 12, 0, 0, 0, time.UTC), false, "Grundlovsdag"},
		{time.Date(2016, 12, 24, 12, 0, 0, 0, time.UTC), false, "Juleaften"},
		{time.Date(2016, 12, 25, 12, 0, 0, 0, time.UTC), true, "Juledag"},
		{time.Date(2016, 12, 26, 12, 0, 0, 0, time.UTC), true, "2. juledag"},
		{time.Date(2016, 12, 31, 12, 0, 0, 0, time.UTC), false, "Nytårsaften"},

		{time.Date(2017, 1, 1, 12, 0, 0, 0, time.UTC), true, "Nytårsdag"},
		{time.Date(2017, 4, 13, 12, 0, 0, 0, time.UTC), true, "Skærtorsdag"},
		{time.Date(2017, 4, 14, 12, 0, 0, 0, time.UTC), true, "Langfredag"},
		{time.Date(2017, 4, 16, 12, 0, 0, 0, time.UTC), true, "Påskedag"},
		{time.Date(2017, 4, 17, 12, 0, 0, 0, time.UTC), true, "2. påskedag"},
		{time.Date(2017, 5, 12, 12, 0, 0, 0, time.UTC), true, "Store Bededag"},
		{time.Date(2017, 5, 25, 12, 0, 0, 0, time.UTC), true, "Kristi Himmelfartsdag"},
		{time.Date(2017, 6, 4, 12, 0, 0, 0, time.UTC), true, "Pinsedag"},
		{time.Date(2017, 6, 5, 13, 0, 0, 0, time.UTC), true, "2. pinsedag"},
		{time.Date(2017, 12, 24, 12, 0, 0, 0, time.UTC), false, "Juleaften"},
		{time.Date(2017, 12, 25, 12, 0, 0, 0, time.UTC), true, "Juledag"},
		{time.Date(2017, 12, 26, 12, 0, 0, 0, time.UTC), true, "2. juledag"},
		{time.Date(2017, 12, 31, 12, 0, 0, 0, time.UTC), false, "Nytårsaften"},

		{time.Date(2018, 1, 1, 12, 0, 0, 0, time.UTC), true, "Nytårsdag"},
	}

	for _, test := range tests {
		got := c.IsHoliday(test.t)
		if got != test.want {
			t.Errorf("got: %t for %s; want: %t (%s)", got, test.name, test.want, test.t)
		}
	}
}

func TestDanishTraditions(t *testing.T) {
	c := NewCalendar()
	c.Observed = ObservedExact
	AddDanishTraditions(c)

	tests := []testStruct{
		{time.Date(2016, 6, 5, 12, 0, 0, 0, time.UTC), true, "Grundlovsdag"},
		{time.Date(2016, 12, 24, 12, 0, 0, 0, time.UTC), true, "Juleaften"},
		{time.Date(2016, 12, 31, 12, 0, 0, 0, time.UTC), true, "Nytårsaften"},

		{time.Date(2017, 6, 5, 12, 0, 0, 0, time.UTC), true, "Grundlovsdag"},
		{time.Date(2017, 12, 24, 12, 0, 0, 0, time.UTC), true, "Juleaften"},
		{time.Date(2017, 12, 31, 12, 0, 0, 0, time.UTC), true, "Nytårsaften"},
	}

	for _, test := range tests {
		got := c.IsHoliday(test.t)
		if got != test.want {
			t.Errorf("got: %t for %s; want: %t (%s)", got, test.name, test.want, test.t)
		}
	}
}
