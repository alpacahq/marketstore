package streamtrigger

import (
	"encoding/json"
	"fmt"
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

type Config struct {
	Filter string `json:"filter"`
}

var _ trigger.Trigger = &StreamTrigger{}

func recast(config map[string]interface{}) (*Config, error) {
	data, err := json.Marshal(config)
	if err != nil {
		return nil, fmt.Errorf("[streamtrigger] marshal config for recasting: %w", err)
	}
	ret := Config{}
	err = json.Unmarshal(data, &ret)
	if err != nil {
		return nil, fmt.Errorf("[streamtrigger] unmarshal config for recasting: %w", err)
	}
	return &ret, nil
}

// NewTrigger returns a new on-disk aggregate trigger based on the configuration.
func NewTrigger(conf map[string]interface{}) (trigger.Trigger, error) {
	config, err := recast(conf)
	if err != nil {
		return nil, fmt.Errorf("[streamtrigger] recast config: %w", err)
	}

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

	year, err := strconv.ParseInt(strings.Replace(fileName, ".bin", "", 1), 10, 32)
	if err != nil {
		log.Error("[streamtrigger] get year from filename (%v)", err)
		return
	}
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
		s.storeColumnSeriesToShelf(tbk, tf, cs, end)
		return
	}

	// if tf.Duration <= time.Minute, push minute bars immediately
	if err2 := stream.Push(*tbk, ColumnSeriesForPayload(cs)); err2 != nil {
		log.Error("[streamtrigger] failed to stream %s (%v)", tbk.String(), err2)
	}
}

// push aggregates to shelf and let them get handled
// asynchronously when they are completed or expire.
func (s *StreamTrigger) storeColumnSeriesToShelf(tbk *io.TimeBucketKey, tf *utils.Timeframe,
	cs *io.ColumnSeries, end time.Time,
) {
	timeWindow, err2 := utils.CandleDurationFromString(tf.String)
	if err2 != nil {
		log.Error("[streamtrigger] timeframe extraction failure (tf=%s) (err=%v)", tf.String, err2)
		return
	}

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
