
package reorg 

import (
	"regexp"
	"fmt"
	"strings"
	"reflect"
)

type ParsePosition struct {
	Begin int
	End int
}

type FieldParserFunc func([]string) string 

type ParseInfo struct {
	FieldNo int
	Line int
	Positions []ParsePosition
	Format string
	Default string
	Func string
}



var ParseMap = map[reflect.Type][]ParseInfo{}

var line_matcher = regexp.MustCompile(`line:([0-9]+)`)
var pos_matcher = regexp.MustCompile(`pos:([^\s]+)`)
var format_matcher = regexp.MustCompile(`format:([^\s]+)`)
var default_matcher = regexp.MustCompile(`default:([^\s]+)`)
var func_matcher = regexp.MustCompile(`func:([^\s]+)`)


func NewParseInfo() ParseInfo {
	return ParseInfo{}
}

func (pi *ParseInfo) parseLine(tag string) {
	line_matches := line_matcher.FindStringSubmatch(tag)
	if len(line_matches) > 1 {
		line_no := 0
		n, _ := fmt.Sscanf(line_matches[1], "%d", &line_no)
		if n == 1{
			pi.Line = line_no
		} else {
			panic("Can't parse line number from tag!")
		}
	}
} 

func (pi *ParseInfo) parsePos(tag string) {
	pos_matches := pos_matcher.FindStringSubmatch(tag)
	if len(pos_matches) > 1 {
		positions := strings.Split(pos_matches[1], ",")
		for _, p := range positions {
			p = strings.TrimSpace(p)
			var pstart, pend int
			_, err := fmt.Sscanf(p, "%d-%d", &pstart, &pend)
			if err != nil {
				_, _ = fmt.Sscanf(p, "%d", &pstart)
				pend = pstart+1
			} 
			pi.Positions = append(pi.Positions, ParsePosition{Begin: pstart, End: pend})
		}
	}
}

func (pi *ParseInfo) parseFormat(tag string) {
	matches := format_matcher.FindStringSubmatch(tag)
	if len(matches) > 1 {
		pi.Format = strings.TrimSpace(matches[1])
	}
}

func (pi *ParseInfo) parseDefault(tag string) {
	matches := default_matcher.FindStringSubmatch(tag)
	if len(matches) > 1 {
		pi.Default = strings.TrimSpace(matches[1])
	}
}


func (pi *ParseInfo) parseFunc(tag string) {
	matches := func_matcher.FindStringSubmatch(tag)
	if len(matches) > 1 {
		pi.Func = strings.TrimSpace(matches[1])
	}
}

func (pi *ParseInfo) ParseTag(tag string) {
	pi.parseLine(tag)
	pi.parsePos(tag)
	pi.parseFormat(tag)
	pi.parseDefault(tag)
	pi.parseFunc(tag)
	return
}

func GetParseDef(item interface{}) []ParseInfo {
	var t reflect.Type
	switch it := item.(type) {
	case reflect.Value:
		t = it.Type().Elem()
	default:
		t = reflect.TypeOf(it).Elem()
	}
	parse_infos, ok := ParseMap[t]
	if !ok {
		parse_infos = make([]ParseInfo, 0, t.NumField())
		for fn:=0; fn<t.NumField(); fn++ {
			f := t.Field(fn)
			if f.Tag != "" {
				tag, exists := f.Tag.Lookup("reorg")	
				if exists {
					pi := NewParseInfo()
					pi.FieldNo = fn
					pi.ParseTag(tag)
					parse_infos = append(parse_infos, pi)
				}
			}
		}
		ParseMap[t] = parse_infos
	}
	return parse_infos
}
