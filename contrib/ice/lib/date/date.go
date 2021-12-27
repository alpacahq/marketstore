package date

import (
	"time"

	"cloud.google.com/go/civil"
)

type Date civil.Date

func DateOf(t time.Time) Date {
	return Date(civil.DateOf(t))
}

func ParseDate(s string) (Date, error) {
	d, err := civil.ParseDate(s)
	return Date(d), err
}

func Parse(layout, value string) (Date, error) {
	t, err := time.Parse(layout, value)
	if err != nil {
		return Date{}, err
	}
	return DateOf(t), nil
}

func (d Date) c() civil.Date {
	return civil.Date(d)
}

func (d Date) AddDays(n int) Date {
	return Date(d.c().AddDays(n))
}

func (d Date) After(d2 Date) bool {
	return d.c().After(d2.c())
}

func (d Date) Before(d2 Date) bool {
	return d.c().Before(d2.c())
}

func (d Date) DaysSince(s Date) int {
	return d.c().DaysSince(s.c())
}

func (d Date) In(loc *time.Location) time.Time {
	return d.c().In(loc)
}

func (d Date) IsValid() bool {
	return d.c().IsValid()
}

func (d Date) MarshalText() ([]byte, error) {
	return d.c().MarshalText()
}

func (d Date) String() string {
	return d.c().String()
}

func (d *Date) UnmarshalText(data []byte) error {
	return (*civil.Date)(d).UnmarshalText(data)
}

func (d Date) Date() (year int, month time.Month, day int) {
	return d.Year, d.Month, d.Day
}

func (d Date) Format(layout string) string {
	return d.In(time.UTC).Format(layout)
}

type NullDate struct {
	Date  Date
	Valid bool
}

func MakeNullDate(d *Date) NullDate {
	if d != nil {
		return NullDate{
			Date:  *d,
			Valid: true,
		}
	}

	return NullDate{}
}
