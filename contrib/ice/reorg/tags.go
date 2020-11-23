package reorg

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"
)

type parsePosition struct {
	begin int
	end   int
}

type fieldParserFunc func([]string) string

type parseInfo struct {
	fieldNo      int
	line         int
	positions    []parsePosition
	format       string
	defaultValue string
	fun          string
}

var parseMap = map[reflect.Type][]parseInfo{}

var lineMatcher = regexp.MustCompile(`line:([0-9]+)`)
var posMatcher = regexp.MustCompile(`pos:([^\s]+)`)
var formatMatcher = regexp.MustCompile(`format:([^\s]+)`)
var defaultMatcher = regexp.MustCompile(`default:([^\s]+)`)
var funcMatcher = regexp.MustCompile(`func:([^\s]+)`)

func newParseInfo() parseInfo {
	return parseInfo{}
}

func (pi *parseInfo) parseLine(tag string) {
	lineMatches := lineMatcher.FindStringSubmatch(tag)
	if len(lineMatches) > 1 {
		lineNo := 0
		n, _ := fmt.Sscanf(lineMatches[1], "%d", &lineNo)
		if n == 1 {
			pi.line = lineNo
		} else {
			panic("Can't parse line number from tag!")
		}
	}
}

func (pi *parseInfo) parsePos(tag string) {
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
			pi.positions = append(pi.positions, parsePosition{begin: pstart, end: pend})
		}
	}
}

func (pi *parseInfo) parseFormat(tag string) {
	matches := formatMatcher.FindStringSubmatch(tag)
	if len(matches) > 1 {
		pi.format = strings.TrimSpace(matches[1])
	}
}

func (pi *parseInfo) parseDefault(tag string) {
	matches := defaultMatcher.FindStringSubmatch(tag)
	if len(matches) > 1 {
		pi.defaultValue = strings.TrimSpace(matches[1])
	}
}

func (pi *parseInfo) parseFunc(tag string) {
	matches := funcMatcher.FindStringSubmatch(tag)
	if len(matches) > 1 {
		pi.fun = strings.TrimSpace(matches[1])
	}
}

func (pi *parseInfo) ParseTag(tag string) {
	pi.parseLine(tag)
	pi.parsePos(tag)
	pi.parseFormat(tag)
	pi.parseDefault(tag)
	pi.parseFunc(tag)
	return
}

func getParseDef(item interface{}) []parseInfo {
	var t reflect.Type
	switch it := item.(type) {
	case reflect.Value:
		t = it.Type().Elem()
	default:
		t = reflect.TypeOf(it).Elem()
	}
	parseInfos, ok := parseMap[t]
	if !ok {
		parseInfos = make([]parseInfo, 0, t.NumField())
		for fn := 0; fn < t.NumField(); fn++ {
			f := t.Field(fn)
			if f.Tag != "" {
				tag, exists := f.Tag.Lookup("reorg")
				if exists {
					pi := newParseInfo()
					pi.fieldNo = fn
					pi.ParseTag(tag)
					parseInfos = append(parseInfos, pi)
				}
			}
		}
		parseMap[t] = parseInfos
	}
	return parseInfos
}
