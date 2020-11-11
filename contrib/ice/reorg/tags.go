package reorg

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"
)

type ParsePosition struct {
	Begin int
	End   int
}

type FieldParserFunc func([]string) string

type ParseInfo struct {
	FieldNo   int
	Line      int
	Positions []ParsePosition
	Format    string
	Default   string
	Func      string
}

var ParseMap = map[reflect.Type][]ParseInfo{}

var lineMatcher = regexp.MustCompile(`line:([0-9]+)`)
var posMatcher = regexp.MustCompile(`pos:([^\s]+)`)
var formatMatcher = regexp.MustCompile(`format:([^\s]+)`)
var defaultMatcher = regexp.MustCompile(`default:([^\s]+)`)
var funcMatcher = regexp.MustCompile(`func:([^\s]+)`)

func NewParseInfo() ParseInfo {
	return ParseInfo{}
}

func (pi *ParseInfo) parseLine(tag string) {
	lineMatches := lineMatcher.FindStringSubmatch(tag)
	if len(lineMatches) > 1 {
		lineNo := 0
		n, _ := fmt.Sscanf(lineMatches[1], "%d", &lineNo)
		if n == 1 {
			pi.Line = lineNo
		} else {
			panic("Can't parse line number from tag!")
		}
	}
}

func (pi *ParseInfo) parsePos(tag string) {
	posMatches := posMatcher.FindStringSubmatch(tag)
	if len(posMatches) > 1 {
		positions := strings.Split(posMatches[1], ",")
		for _, p := range positions {
			p = strings.TrimSpace(p)
			var pstart, pend int
			_, err := fmt.Sscanf(p, "%d-%d", &pstart, &pend)
			if err != nil {
				_, _ = fmt.Sscanf(p, "%d", &pstart)
				pend = pstart + 1
			}
			pi.Positions = append(pi.Positions, ParsePosition{Begin: pstart, End: pend})
		}
	}
}

func (pi *ParseInfo) parseFormat(tag string) {
	matches := formatMatcher.FindStringSubmatch(tag)
	if len(matches) > 1 {
		pi.Format = strings.TrimSpace(matches[1])
	}
}

func (pi *ParseInfo) parseDefault(tag string) {
	matches := defaultMatcher.FindStringSubmatch(tag)
	if len(matches) > 1 {
		pi.Default = strings.TrimSpace(matches[1])
	}
}

func (pi *ParseInfo) parseFunc(tag string) {
	matches := funcMatcher.FindStringSubmatch(tag)
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
	parseInfos, ok := ParseMap[t]
	if !ok {
		parseInfos = make([]ParseInfo, 0, t.NumField())
		for fn := 0; fn < t.NumField(); fn++ {
			f := t.Field(fn)
			if f.Tag != "" {
				tag, exists := f.Tag.Lookup("reorg")
				if exists {
					pi := NewParseInfo()
					pi.FieldNo = fn
					pi.ParseTag(tag)
					parseInfos = append(parseInfos, pi)
				}
			}
		}
		ParseMap[t] = parseInfos
	}
	return parseInfos
}
