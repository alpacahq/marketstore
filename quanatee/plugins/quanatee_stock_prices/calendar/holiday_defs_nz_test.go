package cal

import (
	"reflect"
	"testing"
	"time"
)

func TestNewZelandHolidays(t *testing.T) {
	c := NewCalendar()
	c.Observed = ObservedExact
	AddNewZealandHoliday(c)
	loc, err := time.LoadLocation("Australia/Sydney")
	if err != nil {
		t.Fatal(err)
	}

	tests := []testStruct{
		{time.Date(2016, time.January, 1, 0, 0, 0, 0, loc), true, "New year"},
		{time.Date(2016, time.January, 4, 0, 0, 0, 0, loc), true, "Day after New year"},
		{time.Date(2017, time.January, 2, 0, 0, 0, 0, loc), true, "New year on Sunday next day"},
		{time.Date(2017, time.January, 3, 0, 0, 0, 0, loc), true, "New year on Sunday day after New year"},
		{time.Date(2018, time.January, 1, 0, 0, 0, 0, loc), true, "New year on week day"},
		{time.Date(2018, time.January, 2, 0, 0, 0, 0, loc), true, "Next day after New year on week day"},

		{time.Date(2016, time.December, 27, 0, 0, 0, 0, loc), true, "Christmas on Sunday"},
		{time.Date(2016, time.December, 26, 0, 0, 0, 0, loc), true, "Boxing day on week day"},

		{time.Date(2015, time.December, 25, 0, 0, 0, 0, loc), true, "Christmas on week day"},
		{time.Date(2015, time.December, 28, 0, 0, 0, 0, loc), true, "Boxing day on lieu"},

		{time.Date(2010, time.December, 27, 0, 0, 0, 0, loc), true, "Christmas on Saturday"},
		{time.Date(2010, time.December, 28, 0, 0, 0, 0, loc), true, "Boxing day on Sunday"},

		{time.Date(2019, time.April, 19, 0, 0, 0, 0, loc), true, "Good Friday"},
		{time.Date(2019, time.April, 22, 0, 0, 0, 0, loc), true, "Easter Monday"},

		{time.Date(2019, time.April, 25, 0, 0, 0, 0, loc), true, "Anzac Day"},

		{time.Date(2016, time.June, 6, 0, 0, 0, 0, loc), true, "Queen Birthday 2016"},
		{time.Date(2019, time.June, 3, 0, 0, 0, 0, loc), true, "Queen Birthday 2019"},
		{time.Date(2020, time.June, 1, 0, 0, 0, 0, loc), true, "Queen Birthday 2020"},

		{time.Date(1909, time.October, 13, 0, 0, 0, 0, loc), true, "Labour day 1909"},
		{time.Date(2017, time.October, 23, 0, 0, 0, 0, loc), true, "Labour day 2017"},
		{time.Date(2019, time.October, 28, 0, 0, 0, 0, loc), true, "Labour day 2017"},

		{time.Date(2017, time.February, 6, 0, 0, 0, 0, loc), true, "Warangi day 2017"},
		{time.Date(2016, time.February, 8, 0, 0, 0, 0, loc), true, "Warangi day 2016 on Sunday"},
		{time.Date(2011, time.February, 7, 0, 0, 0, 0, loc), true, "Warangi day 2011 on Saturday"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := c.IsHoliday(tc.t); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("Calendar.AddSkipNonWorkdays() = %v, want %v", got, tc.want)
			}
		})
	}
}
