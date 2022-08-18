package configs

import (
	"strings"
	"time"

	"github.com/alpacahq/marketstore/v4/utils/log"

	jsoniter "github.com/json-iterator/go"
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

// DefaultConfig is the configuration for Alpaca Broker API Feeder you can define in
// marketstore's config file through bgworker extension.
type DefaultConfig struct {
	Exchanges                  []Exchange `json:"exchanges"`
	SymbolsUpdateTime          time.Time  `json:"symbols_update_time"`
	UpdateTime                 time.Time  `json:"update_time"`
	StocksJSONURL              string     `json:"stocks_json_url"`
	StocksJSONBasicAuth        string     `json:"stocks_json_basic_auth"`
	Timeframe                  string     `json:"timeframe"`
	APIKeyID                   string     `json:"api_key_id"`
	APISecretKey               string     `json:"api_secret_key"`
	OpenHourNY, OpenMinuteNY   int
	CloseHourNY, CloseMinuteNY int
	ExtendedHours              bool `json:"extended_hours"`
	ClosedDaysOfTheWeek        []time.Weekday
	ClosedDays                 []time.Time
	Interval                   int `json:"interval"`
	// The data-feeding is executed when 'minute' of the current time matches off_hours_schedule
	// even when the market is cloded. Example: "10" -> execute at 00:10, 01:10, 02:10,...,23:10
	// Numbers separated by commas are allowed.  Example: "0,15,30,45" -> execute every 15 minutes.
	// Whitespaces are ignored.
	OffHoursSchedule string `json:"off_hours_schedule"`
	Backfill         struct {
		Enabled bool `json:"enabled"`
		// Since has only year, month, and day (00:00:00, UTC)
		Since     CustomDay `json:"since"`
		Timeframe string    `json:"timeframe"`
	} `json:"backfill"`
}

// NewConfig casts a map object to Config struct and returns it through json marshal->unmarshal.
func NewConfig(config map[string]interface{}) (*DefaultConfig, error) {
	data, err := json.Marshal(config)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse the config file through json marshal->unmarshal")
	}

	ret := &DefaultConfig{}
	if err2 := json.Unmarshal(data, &ret); err2 != nil {
		return nil, err2
	}

	if len(ret.Exchanges) < 1 {
		return nil, errors.New("must have 1 or more stock exchanges in the config file")
	}

	return envOverride(ret)
}

// CustomTime is a date time object in the ctLayout format.
type CustomTime time.Time

// Custom Time. hh:mm:ss only.
const ctLayout = "15:04:05"

// UnmarshalJSON parses the config data to the DefaultConfig object.
// Because some parameters (OpenTime, ClosedDaysOfTheWeek, etc) have their original types and unmarshal methods
// but it's troublesome for other business logic to use those not-general types,
// so this method parses the data to an auxiliary struct and cast the types first,
// then parse to the DefaultConfig object.
func (c *DefaultConfig) UnmarshalJSON(input []byte) error {
	type Alias DefaultConfig

	aux := &struct {
		SymbolsUpdateTime   CustomTime  `json:"symbols_update_time"`
		UpdateTime          CustomTime  `json:"update_time"`
		OpenTime            CustomTime  `json:"openTime"`  // deprecated
		CloseTime           CustomTime  `json:"closeTime"` // deprecated
		OpenTimeNY          CustomTime  `json:"open_time_NY"`
		CloseTimeNY         CustomTime  `json:"close_time_NY"`
		ClosedDaysOfTheWeek []weekday   `json:"closedDaysOfTheWeek"`
		ClosedDays          []CustomDay `json:"closedDays"`
		*Alias
	}{Alias: (*Alias)(c)}

	if err := json.Unmarshal(input, &aux); err != nil {
		return err
	}
	c.SymbolsUpdateTime = time.Time(aux.SymbolsUpdateTime)
	c.UpdateTime = time.Time(aux.UpdateTime)
	if !time.Time(aux.OpenTime).IsZero() || !time.Time(aux.CloseTime).IsZero() {
		log.Error("!!!!!!!!open_time and close_time config are DEPRECATED!!!!!!!! " +
			"Please use open_time_NY and close_time_NY instead.")
		return errors.New("!!!!!!!!open_time and close_time config are DEPRECATED!!!!!!!! " +
			"Please use open_time_NY and close_time_NY instead.")
	}
	c.OpenHourNY, c.OpenMinuteNY, _ = time.Time(aux.OpenTimeNY).Clock()
	c.CloseHourNY, c.CloseMinuteNY, _ = time.Time(aux.CloseTimeNY).Clock()
	c.ClosedDaysOfTheWeek = convertTime(aux.ClosedDaysOfTheWeek)
	c.ClosedDays = convertDate(aux.ClosedDays)

	return nil
}

// convertSliceType converts a slice of weekday to a slice of time.weekday.
func convertTime(w []weekday) []time.Weekday {
	d := make([]time.Weekday, len(w))
	for i := range w {
		d[i] = time.Weekday(w[i])
	}
	return d
}

func convertDate(cd []CustomDay) []time.Time {
	d := make([]time.Time, len(cd))
	for i := range cd {
		d[i] = time.Time(cd[i])
	}
	return d
}

// UnmarshalJSON parses a string in the ctLayout.
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

// CustomDay is a date time object (UTC) in the cdLayout format.
type CustomDay time.Time

// Custom Date. yyyy-mm-dd only.
const cdLayout = "2006-01-02"

// UnmarshalJSON parses a string in the cdLayout.
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

// UnmarshalJSON parses a string for a day of the week.
func (wd *weekday) UnmarshalJSON(input []byte) error {
	s := strings.Trim(string(input), "\"")

	if d, ok := daysOfWeek[s]; ok {
		*wd = weekday(d)
		return nil
	}

	return errors.Errorf("invalid weekday '%s'", s)
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
