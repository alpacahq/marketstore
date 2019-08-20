package cal

import (
	"testing"
	"time"
)

func TestSwedishHolidays(t *testing.T) {
	c := NewCalendar()
	c.Observed = ObservedExact
	AddSwedishHolidays(c)

	tests := []testStruct{
		{time.Date(2016, 1, 6, 12, 0, 0, 0, time.UTC), true, "Trettondedag Jul"},
		{time.Date(2016, 3, 25, 12, 0, 0, 0, time.UTC), true, "Långfredagen"},
		{time.Date(2016, 3, 27, 12, 0, 0, 0, time.UTC), true, "Påskdagen"},
		{time.Date(2016, 3, 28, 12, 0, 0, 0, time.UTC), true, "Annandag påsk"},
		{time.Date(2016, 5, 1, 12, 0, 0, 0, time.UTC), true, "Första maj"},
		{time.Date(2016, 5, 5, 12, 0, 0, 0, time.UTC), true, "Kristi himmelfärdsdag"},
		{time.Date(2016, 5, 15, 12, 0, 0, 0, time.UTC), true, "Pingstdagen"},
		{time.Date(2016, 6, 6, 12, 0, 0, 0, time.UTC), true, "Sveriges nationaldag"},
		{time.Date(2016, 6, 24, 12, 0, 0, 0, time.UTC), true, "Midsommarafton"},
		{time.Date(2016, 6, 25, 12, 0, 0, 0, time.UTC), true, "Midsommardagen"},
		{time.Date(2016, 11, 5, 12, 0, 0, 0, time.UTC), true, "Alla helgons dag"},
		{time.Date(2016, 12, 24, 12, 0, 0, 0, time.UTC), true, "Julafton"},
		{time.Date(2016, 12, 25, 12, 0, 0, 0, time.UTC), true, "Juldagen"},
		{time.Date(2016, 12, 26, 12, 0, 0, 0, time.UTC), true, "Annandag jul"},
		{time.Date(2016, 12, 31, 12, 0, 0, 0, time.UTC), true, "Nyårsafton"},

		{time.Date(2017, 1, 1, 12, 0, 0, 0, time.UTC), true, "Nyårsdagen"},
		{time.Date(2017, 1, 6, 12, 0, 0, 0, time.UTC), true, "Trettondedag Jul"},
		{time.Date(2017, 4, 14, 12, 0, 0, 0, time.UTC), true, "Långfredagen"},
		{time.Date(2017, 4, 16, 12, 0, 0, 0, time.UTC), true, "Påskdagen"},
		{time.Date(2017, 4, 17, 12, 0, 0, 0, time.UTC), true, "Annandag påsk"},
		{time.Date(2017, 5, 1, 12, 0, 0, 0, time.UTC), true, "Första maj"},
		{time.Date(2017, 5, 25, 12, 0, 0, 0, time.UTC), true, "Kristi himmelfärdsdag"},
		{time.Date(2017, 6, 4, 12, 0, 0, 0, time.UTC), true, "Pingstdagen"},
		{time.Date(2017, 6, 6, 12, 0, 0, 0, time.UTC), true, "Sveriges nationaldag"},
		{time.Date(2017, 6, 23, 12, 0, 0, 0, time.UTC), true, "Midsommarafton"},
		{time.Date(2017, 6, 24, 12, 0, 0, 0, time.UTC), true, "Midsommardagen"},
		{time.Date(2017, 11, 4, 12, 0, 0, 0, time.UTC), true, "Alla helgons dag"},
		{time.Date(2017, 12, 24, 12, 0, 0, 0, time.UTC), true, "Julafton"},
		{time.Date(2017, 12, 25, 12, 0, 0, 0, time.UTC), true, "Juldagen"},
		{time.Date(2017, 12, 26, 12, 0, 0, 0, time.UTC), true, "Annandag jul"},
		{time.Date(2017, 12, 31, 12, 0, 0, 0, time.UTC), true, "Nyårsafton"},

		{time.Date(2018, 1, 1, 12, 0, 0, 0, time.UTC), true, "Nyårsdagen"},
	}

	for _, test := range tests {
		got := c.IsHoliday(test.t)
		if got != test.want {
			t.Errorf("got: %t for %s; want: %t (%s)", got, test.name, test.want, test.t)
		}
	}
}
