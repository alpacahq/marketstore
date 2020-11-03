package reorg

import (
	"reflect"
	"strings"
	"regexp"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

// last \n intentionally not included, as it would include an extra empy line after split
var record_matcher = regexp.MustCompile(`\.{59} [[:ascii:]]+?[^\*]\*\* +?\n`)  

var file_end_matcher = regexp.MustCompile(`^\*+? +\n`)

func safeCall(v reflect.Value, fn string, lines []string) (out string) {
	defer func() {
		err := recover()
		if err != nil {
			log.Fatal("%+v died while parsing: %s", fn, strings.Join(lines, "\n"))
			out = ""
		}
	}()
	if fn := v.MethodByName(strings.TrimSpace(fn)); fn.IsValid() {
		args := []reflect.Value{reflect.ValueOf(lines)}
		out_values := fn.Call(args)
		out = out_values[0].String()
	} else {
		out = ""
	}
	return out
}

func ReadRecord(lines []string, it interface{}) {
	var v reflect.Value 
	switch t := it.(type) {
	case reflect.Value:
		v = t.Elem()
	default:
		if reflect.TypeOf(it).Kind() != reflect.Ptr {
			panic("use & when passing a struct for Parse!!")
		}
		v = reflect.Indirect(reflect.ValueOf(t))
	}	
	parse_def := GetParseDef(it)
	var input string 
	for _, parse := range parse_def {
		field := v.Field(parse.FieldNo)
		input = ""
		if parse.Func != "" {
			input = safeCall(v, parse.Func, lines)
		} else {
			input = lines[parse.Line]
			if len(parse.Positions) > 0 {
				content := ""
				for _, parse_pos := range(parse.Positions) {
					substr := input[parse_pos.Begin:parse_pos.End]
					content += substr
				}
				input = content
			} 
		}
		Convert(input, parse.Format, parse.Default, field)
	}
}


func ReadRecords(content string, slicePtr interface{}) {
	if !(reflect.TypeOf(slicePtr).Kind() == reflect.Ptr && 
		reflect.TypeOf(slicePtr).Elem().Kind() == reflect.Slice) {
		panic("target must be a ptr to slice!!")
	}
	slice_value := reflect.ValueOf(slicePtr).Elem()
	elementType := slice_value.Type().Elem()

	record_no := 1
	for {
		if file_end_matcher.MatchString(content) {
			break
		}
		result := record_matcher.FindString(content)
		if len(result) > 1 {
			rec := reflect.New(elementType)
			lines := strings.Split(result, "\n")
			lines = lines[:len(lines)-1]
			ReadRecord(lines, rec)
			slice_value.Set(reflect.Append(slice_value, rec.Elem()))
			content = content[len(result):len(content)]
			record_no++
		} else {
			log.Error(content)
			panic("something went wrong, it should ALWAYS match")
		}
	}
} 
