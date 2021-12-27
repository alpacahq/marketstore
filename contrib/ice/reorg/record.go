package reorg

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/alpacahq/marketstore/v4/utils/log"
)

// last \n intentionally not included, as it would include an extra empy line after split.
var recordMatcher = regexp.MustCompile(`\.{59} [[:ascii:]]+?[^\*]\*\* +?\n`)

var fileEndMatcher = regexp.MustCompile(`^\*+? +\n`)

func safeCall(v reflect.Value, fun string, lines []string) (out string) {
	defer func() {
		err := recover()
		if err != nil {
			log.Error("%+v died while parsing: %s", fun, strings.Join(lines, "\n"))
			out = ""
		}
	}()
	if fn := v.MethodByName(strings.TrimSpace(fun)); fn.IsValid() {
		args := []reflect.Value{reflect.ValueOf(lines)}
		outValues := fn.Call(args)
		out = outValues[0].String()
	} else {
		log.Error("custom parser method not found: %s", strings.TrimSpace(fun))
		out = ""
	}
	return out
}

func readRecord(lines []string, it interface{}) {
	var v reflect.Value
	switch t := it.(type) {
	case reflect.Value:
		v = t.Elem()
	default:
		if reflect.TypeOf(it).Kind() != reflect.Ptr {
			log.Error("readRecord: use & when passing a struct for readRecord")
		}
		v = reflect.Indirect(reflect.ValueOf(t))
	}
	parseDef := getParseDef(it)
	var input string
	for _, parse := range parseDef {
		field := v.Field(parse.fieldNo)
		input = ""
		if parse.fun != "" {
			input = safeCall(v, parse.fun, lines)
		} else {
			input = lines[parse.line]
			if len(parse.positions) > 0 {
				content := ""
				for _, parsePos := range parse.positions {
					substr := input[parsePos.begin:parsePos.end]
					content += substr
				}
				input = content
			}
		}
		convert(input, parse.format, parse.defaultValue, field)
	}
}

func readRecords(content string, slicePtr interface{}) error {
	if !(reflect.TypeOf(slicePtr).Kind() == reflect.Ptr &&
		reflect.TypeOf(slicePtr).Elem().Kind() == reflect.Slice) {
		return fmt.Errorf("readRecords: target must be a pointer to a slice")
	}
	sliceValue := reflect.ValueOf(slicePtr).Elem()
	elementType := sliceValue.Type().Elem()
	for {
		if fileEndMatcher.MatchString(content) {
			break
		}
		result := recordMatcher.FindString(content)
		if len(result) > 1 {
			rec := reflect.New(elementType)
			lines := strings.Split(result, "\n")
			lines = lines[:len(lines)-1]
			readRecord(lines, rec)
			sliceValue.Set(reflect.Append(sliceValue, rec.Elem()))
			content = content[len(result):]
		} else {
			return fmt.Errorf("file parsing error, please check for file corruption or format change: %v", content)
		}
	}
	return nil
}
