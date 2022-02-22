package streamtrigger

import (
	"encoding/json"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/calendar"
	"github.com/alpacahq/marketstore/v4/contrib/stream/shelf"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/frontend/stream"
	"github.com/alpacahq/marketstore/v4/planner"
	"github.com/alpacahq/marketstore/v4/plugins/trigger"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

type StreamTriggerConfig struct {
	Filter string `json:"filter"`
}

var _ trigger.Trigger = &StreamTrigger{}

func recast(config map[string]interface{}) *StreamTriggerConfig {
	data, _ := json.Marshal(config)
	ret := StreamTriggerConfig{}
	json.Unmarshal(data, &ret)
	return &ret
}

// NewTrigger returns a new on-disk aggregate trigger based on the configuration.
func NewTrigger(conf map[string]interface{}) (trigger.Trigger, error) {
	config := recast(conf)

	filter := config.Filter
	if filter != "" && filter != "nasdaq" {
		log.Warn("[streamtrigger] filter value \"%s\" is not recognized", filter)
		filter = ""
	}

	return &StreamTrigger{
		shelf.NewShelf(shelf.NewShelfHandler(stream.Push)), filter,
	}, nil
}

type StreamTrigger struct {
	shelf  *shelf.Shelf
	filter string
}

func maxInt64(values []int64) int64 {
	max := values[0]
	for _, v := range values[1:] {
		if v > max {
			max = v
		}
	}
	return max
}

// Fire is the hook to retrieve the latest written data
// and stream it over the websocket.
func (s *StreamTrigger) Fire(keyPath string, records []trigger.Record) {
	indexes := make([]int64, len(records))
	for i, record := range records {
		indexes[i] = record.Index()
	}

	tail := maxInt64(indexes)

	cDir := executor.ThisInstance.CatalogDir

	elements := strings.Split(keyPath, "/")
	tbkString := strings.Join(elements[:len(elements)-1], "/")
	tf := utils.NewTimeframe(elements[1])
	fileName := elements[len(elements)-1]

	year, _ := strconv.Atoi(strings.Replace(fileName, ".bin", "", 1))
	tbk := io.NewTimeBucketKey(tbkString)
	end := io.IndexToTime(tail, tf.Duration, int16(year))

	q := planner.NewQuery(cDir)
	q.AddTargetKey(tbk)
	q.SetEnd(end)
	q.SetRowLimit(io.LAST, 1)

	parsed, err := q.Parse()
	if err != nil {
		log.Error("[streamtrigger] query parse failure (%v)", err)
		return
	}

	scanner, err := executor.NewReader(parsed)
	if err != nil {
		log.Error("[streamtrigger] new scanner failure (%v)", err)
		return
	}

	csm, err := scanner.Read()
	if err != nil {
		log.Error("[streamtrigger] scanner read failure (%v)", err)
		return
	}

	cs := csm[*tbk]

	if cs == nil || cs.Len() == 0 {
		return
	}

	if tf.Duration > time.Minute {
		// push aggregates to shelf and let them get handled
		// asynchronously when they are completed or expire
		timeWindow := utils.CandleDurationFromString(tf.String)

		var deadline *time.Time

		// handle the 1D bar case to aggregate based on calendar
		if tf.Duration >= 24*time.Hour && strings.EqualFold(s.filter, "nasdaq") {
			deadline = calendar.Nasdaq.MarketClose(end)
		} else {
			ceiling := timeWindow.Ceil(end)
			deadline = &ceiling
		}

		if deadline != nil && deadline.After(time.Now()) {
			s.shelf.Store(tbk, ColumnSeriesForPayload(cs), deadline)
		}
	} else if err2 := stream.Push(*tbk, ColumnSeriesForPayload(cs)); err2 != nil {
		// push minute bars immediately
		log.Error("[streamtrigger] failed to stream %s (%v)", tbk.String(), err2)
	}
}

// ColumnSeriesForPayload extracts the single row from the column
// series that is queried by the trigger, to prepare it for a
// streaming payload.
// nolint:gocritic // TODO: refactor (change *map -> map and related code using lots of reflection0
func ColumnSeriesForPayload(cs *io.ColumnSeries) *map[string]interface{} {
	m := map[string]interface{}{}

	if cs == nil {
		return nil
	}

	for key, col := range cs.GetColumns() {
		s := reflect.ValueOf(col)
		if s.Len() > 0 {
			m[key] = s.Index(0).Interface()
		}
	}

	return &m
}
