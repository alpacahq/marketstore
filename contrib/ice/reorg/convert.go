package reorg

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/alpacahq/marketstore/v4/utils/log"
)

var TIME = reflect.TypeOf(time.Time{}).Name()
var INT = reflect.TypeOf(1).Name()
var INT64 = reflect.TypeOf(int64(1)).Name()
var FLOAT = reflect.TypeOf(1.2).Name()
var STRING = reflect.TypeOf("").Name()

type TypeConverter func(str string, v reflect.Value, format string) error

var converters = map[string]TypeConverter{
	INT:    stringToInt,
	INT64:  stringToInt,
	FLOAT:  stringToFloat,
	STRING: stringToString,
	TIME:   stringToTime,
}

var formatDefaults = map[string]string{
	INT:   "%d",
	INT64: "%d",
	FLOAT: "%f",
	TIME:  "01/02/06",
}

func stringToInt(str string, v reflect.Value, format string) error {
	var val int
	n, err := fmt.Sscanf(str, format, &val)
	if n == 1 {
		v.SetInt(int64(val))
	}
	return err
}

func stringToFloat(str string, v reflect.Value, format string) error {
	var f float64
	n, err := fmt.Sscanf(str, format, &f)
	if n == 1 {
		v.SetFloat(f)
	}
	return err
}

func stringToString(str string, v reflect.Value, format string) error {
	v.SetString(str)
	return nil
}

func stringToTime(str string, v reflect.Value, format string) error {
	// allow blank time fields
	if str == "99/99/99" || str == "/  /" || str == "" {
		return nil
	}
	t, err := time.Parse(format, str)
	if err == nil {
		v.Set(reflect.ValueOf(t))
	}
	return err
}

func Convert(input string, format string, def string, v reflect.Value) {
	cleanInput := strings.TrimSpace(input)
	if len(cleanInput) == 0 && len(def) > 0 {
		cleanInput = strings.TrimSpace(def)
	}
	if cleanInput == "" {
		return
	}
	defer func() {
		err := recover()
		if err != nil {
			log.Error("panic at conversion: %+v\n input: %s\n", err, input)
		}
	}()
	iv := reflect.Indirect(v)
	if iv.CanSet() {
		f := converters[iv.Type().Name()]
		if f != nil {
			if format == "" {
				format = formatDefaults[iv.Type().Name()]
			}
			err := f(cleanInput, iv, format)
			if err != nil {
				log.Error("type conversion error: %+v, %s\n", err, input)
			}
		} else {
			println("converter not found for", input, v.Type().Name(), "kind:", iv.Kind())
		}
	} else {
		println("value is read only!!!! ", input, " to ", v.Type().Name())
	}
}
