package reorg

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/alpacahq/marketstore/v4/utils/log"
)

var TIME = reflect.TypeOf(time.Time{}).Name()

var UINT = reflect.TypeOf(uint(1)).Name()
var UINT8 = reflect.TypeOf(uint8(1)).Name()
var UINT16 = reflect.TypeOf(uint16(1)).Name()
var UINT32 = reflect.TypeOf(uint32(1)).Name()
var UINT64 = reflect.TypeOf(uint64(1)).Name()
var INT = reflect.TypeOf(1).Name()
var INT8 = reflect.TypeOf(int8(1)).Name()
var INT16 = reflect.TypeOf(int16(1)).Name()
var INT32 = reflect.TypeOf(int32(1)).Name()
var INT64 = reflect.TypeOf(int64(1)).Name()
var FLOAT = reflect.TypeOf(1.2).Name()
var STRING = reflect.TypeOf("").Name()

type typeConverter func(str string, v reflect.Value, format string) error

var converters = map[string]typeConverter{
	UINT:   stringToUint,
	UINT8:  stringToUint,
	UINT16: stringToUint,
	UINT32: stringToUint,
	UINT64: stringToUint,
	INT:    stringToInt,
	INT8:   stringToInt,
	INT16:  stringToInt,
	INT64:  stringToInt,
	INT64:  stringToInt,
	FLOAT:  stringToFloat,
	STRING: stringToString,
	TIME:   stringToTime,
}

var formatDefaults = map[string]string{
	INT:    "%d",
	INT8:   "%d",
	INT16:  "%d",
	INT32:  "%d",
	INT64:  "%d",
	UINT:   "%d",
	UINT8:  "%d",
	UINT16: "%d",
	UINT32: "%d",
	UINT64: "%d",
	INT64:  "%d",
	FLOAT:  "%f",
	TIME:   "01/02/06",
}

func stringToUint(str string, v reflect.Value, format string) error {
	var val uint64
	n, err := fmt.Sscanf(str, format, &val)
	if n == 1 {
		v.SetUint(val)
	}
	return err
}

func stringToInt(str string, v reflect.Value, format string) error {
	var val int64
	n, err := fmt.Sscanf(str, format, &val)
	if n == 1 {
		v.SetInt(val)
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

func getConverter(t reflect.Type) typeConverter {
	f := converters[t.Name()]
	if f == nil {
		f = converters[t.Kind().String()]
	}
	return f
}

func getFormatter(t reflect.Type) string {
	format := formatDefaults[t.Name()]
	if format == "" {
		format = formatDefaults[t.Kind().String()]
	}
	return format
}

func convert(input string, format string, def string, v reflect.Value) {
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
		f := getConverter(iv.Type())
		if f != nil {
			if format == "" {
				format = getFormatter(iv.Type())
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
