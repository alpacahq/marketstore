package cal

import (
	"reflect"
	"testing"
	"time"
)

func TestAustralianHolidays(t *testing.T) {
	c := NewCalendar()
	c.Observed = ObservedExact
	AddAustralianHolidays(c)
	loc, err := time.LoadLocation("Australia/Sydney")
	if err != nil {
		t.Fatal(err)
	}
	currentTime := time.Now()
	year := currentTime.Year()

	tests := []testStruct{
		{time.Date(year, time.January, 1, 0, 0, 0, 0, loc), true, "New year"},
		{time.Date(2017, time.January, 2, 0, 0, 0, 0, loc), true, "New year Sunday"},
		{time.Date(year, time.January, 2, 0, 0, 0, 0, loc), false, "New year Sunday invalid"},
		{time.Date(year, time.December, 25, 0, 0, 0, 0, loc), true, "Christmas"},
		{time.Date(2010, time.December, 27, 0, 0, 0, 0, loc), true, "Christmas on Saturday"},
		{time.Date(2011, time.December, 27, 0, 0, 0, 0, loc), true, "Christmas on Sunday"},
		{time.Date(2018, time.December, 26, 0, 0, 0, 0, loc), true, "Boxing Day"},
		{time.Date(2019, time.April, 19, 0, 0, 0, 0, loc), true, "Good Friday"},
		{time.Date(2019, time.April, 22, 0, 0, 0, 0, loc), true, "Easter Monday"},
		{time.Date(2019, time.April, 25, 0, 0, 0, 0, loc), true, "Anzac Day"},
		{time.Date(2004, time.April, 25, 0, 0, 0, 0, loc), true, "Anzac Day"},
		{time.Date(2018, time.June, 11, 0, 0, 0, 0, loc), true, "Queen's birth Day"},
		{time.Date(2019, time.June, 10, 0, 0, 0, 0, loc), true, "Queen's birth Day"},
		{time.Date(2020, time.June, 8, 0, 0, 0, 0, loc), true, "Queen's birth Day"},
		{time.Date(2019, time.October, 7, 0, 0, 0, 0, loc), true, "Labour Day"},
		{time.Date(2018, time.October, 1, 0, 0, 0, 0, loc), true, "Labour Day"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := c.IsHoliday(tc.t); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("Calendar.AddSkipNonWorkdays() = %v, want %v", got, tc.want)
			}
		})
	}
}
