package configs

import (
	"strings"
	"time"

	"github.com/json-iterator/go"
	"github.com/pkg/errors"
)

// json iter supports marshal/unmarshal of map[interface{}]interface{] type.
// when the config file contains (a) nested structure(s) like follows:
//
// backfill:
//   enabled: true
//
// the standard "encoding/json" library cannot marshal the structure
// because the config is parsed from a yaml file (mkts.yaml) to map[string]interface{} and passed to this file,
// and config["backfill"] object has map[interface{}]interface{} type.
var json = jsoniter.ConfigCompatibleWithStandardLibrary

// DefaultConfig is the configuration for XigniteFeeder you can define in
// marketstore's config file through bgworker extension.
type DefaultConfig struct {
	Exchanges           []string `json:"exchanges"`
	IndexGroups			[]string `json:"index_groups"`
	UpdatingHour        int      `json:"updatingHour"`
	Timeframe           string   `json:"timeframe"`
	APIToken            string   `json:"token"`
	Timeout             int      `json:"timeout"`
	OpenTime            time.Time
	CloseTime           time.Time
	ClosedDaysOfTheWeek []time.Weekday
	ClosedDays          []time.Time
	Interval            int `json:"interval"`
	Backfill            struct {
		Enabled   bool      `json:"enabled"`
		Since     CustomDay `json:"since"`
		Timeframe string    `json:"timeframe"`
	} `json:"backfill"`
}

// NewConfig casts a map object to Config struct and returns it through json marshal->unmarshal
func NewConfig(config map[string]interface{}) (*DefaultConfig, error) {
	data, err := json.Marshal(config)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse the config file through json marshal->unmarshal")
	}

	ret := DefaultConfig{}
	if err := json.Unmarshal(data, &ret); err != nil {
		return nil, err
	}

	if len(ret.Exchanges) < 1 && len(ret.IndexGroups) < 1 {
		return nil, errors.New("must have 1 or more stock exchanges or index group in the config file")
	}

	return &ret, nil
}

// CustomTime is a date time object in the ctLayout format
type CustomTime time.Time

// Custom Time. hh:mm:ss only
const ctLayout = "15:04:05"

// UnmarshalJSON parses the config data to the DefaultConfig object.
// Because some parameters (OpenTime, ClosedDaysOfTheWeek, etc) have their original types and unmarshal methods,
// but it's troublesome for other business logic to use those not-general types,
// so this method parses the data to an auxiliary struct and cast the types first, then parse to the DefaultConfig object.
func (c *DefaultConfig) UnmarshalJSON(input []byte) error {
	type Alias DefaultConfig

	aux := &struct {
		OpenTime            CustomTime  `json:"openTime"`
		CloseTime           CustomTime  `json:"closeTime"`
		ClosedDaysOfTheWeek []weekday   `json:"closedDaysOfTheWeek"`
		ClosedDays          []CustomDay `json:"closedDays"`
		*Alias
	}{Alias: (*Alias)(c)}

	if err := json.Unmarshal(input, &aux); err != nil {
		return err
	}
	c.OpenTime = time.Time(aux.OpenTime)
	c.CloseTime = time.Time(aux.CloseTime)
	c.ClosedDaysOfTheWeek = convertTime(aux.ClosedDaysOfTheWeek)
	c.ClosedDays = convertDate(aux.ClosedDays)

	return nil
}

// convertSliceType converts a slice of weekday to a slice of time.weekday
func convertTime(w []weekday) []time.Weekday {
	d := make([]time.Weekday, 1)
	for _, v := range w {
		d = append(d, time.Weekday(v))
	}
	return d
}

func convertDate(cd []CustomDay) []time.Time {
	d := make([]time.Time, 1)
	for _, v := range cd {
		d = append(d, time.Time(v))
	}
	return d
}

// UnmarshalJSON parses a string in the ctLayout
func (ct *CustomTime) UnmarshalJSON(input []byte) error {
	s := strings.Trim(string(input), "\"")
	if s == "null" {
		*ct = CustomTime(time.Time{})
		return nil
	}
	t, err := time.Parse(ctLayout, s)
	if err != nil {
		return err
	}
	*ct = CustomTime(t)
	return nil
}

// CustomDay is a date time object in the cdLayout format
type CustomDay time.Time

// Custom Date. yyyy-mm-dd only
const cdLayout = "2006-01-02"

// UnmarshalJSON parses a string in the cdLayout
func (cd *CustomDay) UnmarshalJSON(input []byte) error {
	s := strings.Trim(string(input), "\"")
	if s == "null" {
		*cd = CustomDay(time.Time{})
		return nil
	}
	t, err := time.Parse(cdLayout, s)
	if err != nil {
		return err
	}
	*cd = CustomDay(t)
	return nil
}

type weekday time.Weekday

// UnmarshalJSON parses a string for a day of the week
func (wd *weekday) UnmarshalJSON(input []byte) error {
	s := strings.Trim(string(input), "\"")

	if d, ok := daysOfWeek[s]; ok {
		*wd = weekday(d)
		return nil
	}

	return errors.Errorf("invalid weekday '%s'.", s)
}

var daysOfWeek = map[string]time.Weekday{
	"Sunday":    time.Sunday,
	"Monday":    time.Monday,
	"Tuesday":   time.Tuesday,
	"Wednesday": time.Wednesday,
	"Thursday":  time.Thursday,
	"Friday":    time.Friday,
	"Saturday":  time.Saturday,
}
