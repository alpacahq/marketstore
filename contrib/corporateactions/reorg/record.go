package reorg

import (
	"reflect"
	"strings"
	"regexp"
	"fmt"
)


var record_matcher = regexp.MustCompile(`\.{59} [[:ascii:]]+? \*\* +\n`)
var file_end_matcher = regexp.MustCompile(`^\*+? +\n`)


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
		// field_name := v.Type().Field(parse.FieldNo).Name
		if parse.Func != "" {
			if fn := v.MethodByName(strings.TrimSpace(parse.Func)); fn.IsValid() {
				out_values := fn.Call([]reflect.Value{reflect.ValueOf(lines)})
				input = out_values[0].String()
			} else {
				fmt.Printf("cannot find function '%s' on %s\n", parse.Func, v.Type().Name())
				input = ""
			}
		} else {
			input := lines[parse.Line]
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
			println("EOF reached", len(content), "bytes left")
			break
		}
		result := record_matcher.FindString(content)
		if len(result) > 0 {
			rec := reflect.New(elementType)
			ReadRecord(strings.Split(result, "\n"), rec)
			slice_value.Set(reflect.Append(slice_value, rec.Elem()))
			content = content[len(result):len(content)]
			record_no++
		} else {
			println(content)
			panic("something went wrong, it shoult ALWAYS match")
		}
	}
} 
