package main

import (
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/golang/glog"

	"github.com/alpacahq/marketstore/catalog"
	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/planner"
	"github.com/alpacahq/marketstore/plugins/trigger"
	"github.com/alpacahq/marketstore/utils"
	"github.com/alpacahq/marketstore/utils/io"
)

type SimpleAggTrigger struct {
	config       map[string]interface{}
	destinations []string
}

var _ trigger.Trigger = &SimpleAggTrigger{}

var loadError = errors.New("plugin load error")

func NewTrigger(config map[string]interface{}) (trigger.Trigger, error) {
	glog.Infof("NewTrigger")

	destIns, ok := config["destinations"]
	if !ok {
		glog.Errorf("no destinations are configured")
		return nil, loadError
	}

	params, ok := destIns.([]interface{})
	if !ok {
		glog.Errorf("destinations do not look like an array")
		return nil, loadError
	}
	destinations := []string{}
	for _, ifval := range params {
		timeframe, ok := ifval.(string)
		if !ok {
			glog.Errorf("destination %v does not look like string", ifval)
		}
		destinations = append(destinations, timeframe)
	}
	glog.Infof("%d destination(s) configured", len(destinations))
	return &SimpleAggTrigger{
		config:       config,
		destinations: destinations,
	}, nil
}

func (s *SimpleAggTrigger) Fire(keyPath string, indexes []int64) {
	glog.Infof("keyPath=%s len(indexes)=%d", keyPath, len(indexes))

	headIndex := indexes[0]
	tailIndex := indexes[len(indexes)-1]

	for _, timeframe := range s.destinations {
		processFor(timeframe, keyPath, headIndex, tailIndex)
	}
}

func processFor(timeframe, keyPath string, headIndex, tailIndex int64) {
	theInstance := executor.ThisInstance
	catalogDir := theInstance.CatalogDir
	elements := strings.Split(keyPath, "/")
	tbkString := strings.Join(elements[:len(elements)-1], "/")
	tf := utils.NewTimeframe(elements[1])
	fileName := elements[len(elements)-1]
	year, _ := strconv.Atoi(strings.Replace(fileName, ".bin", "", 1))
	tbk := io.NewTimeBucketKey(tbkString)
	headTs := io.IndexToTime(headIndex, int64(tf.PeriodsPerDay()), int16(year))
	tailTs := io.IndexToTime(tailIndex, int64(tf.PeriodsPerDay()), int16(year))
	timeWindow := utils.CandleDurationFromString(timeframe)
	start := timeWindow.Truncate(headTs)
	end := timeWindow.Ceil(tailTs)
	// TODO: this is not needed once we support "<" operator
	end = end.Add(-time.Second)

	// Scan
	q := planner.NewQuery(catalogDir)
	q.AddTargetKey(tbk)
	q.SetRange(start, end)
	parsed, err := q.Parse()
	if err != nil {
		glog.Errorf("%v", err)
		return
	}
	scanner, err := executor.NewReader(parsed)
	if err != nil {
		glog.Errorf("%v", err)
		return
	}
	csm, _, err := scanner.Read()
	if err != nil {
		glog.Errorf("%v", err)
		return
	}
	cs := csm[*tbk]
	if cs.Len() == 0 {
		// Nothing to do.  Really?
		return
	}
	rs := aggregate(cs, tbk)

	targetTbkString := elements[0] + "/" + timeframe + "/" + elements[2]
	targetTbk := io.NewTimeBucketKey(targetTbkString)

	w, err := getWriter(theInstance, targetTbk, int16(year), cs.GetDataShapes())
	if err != nil {
		glog.Errorf("Failed to get Writer for %s/%d: %v", targetTbk.String(), year, err)
		return
	}
	w.WriteRecords(rs.GetTime(), rs.GetData())

	wal := theInstance.WALFile
	tgc := theInstance.TXNPipe
	wal.FlushToWAL(tgc)
	wal.FlushToPrimary()
}

func aggregate(cs *io.ColumnSeries, tbk *io.TimeBucketKey) *io.RowSeries {
	timeWindow := utils.CandleDurationFromString(tbk.GetItemInCategory("Timeframe"))
	ts := cs.GetTime()
	// TODO: generalize aggregate later
	scanOpen := cs.GetColumn("Open").([]float32)
	scanHigh := cs.GetColumn("High").([]float32)
	scanLow := cs.GetColumn("Low").([]float32)
	scanClose := cs.GetColumn("Close").([]float32)
	scanVolume := cs.GetColumn("Volume").([]float32)

	outEpoch := make([]int64, 0)
	outOpen := make([]float32, 0)
	outHigh := make([]float32, 0)
	outLow := make([]float32, 0)
	outClose := make([]float32, 0)
	outVolume := make([]float32, 0)

	groupKey := ts[0]
	groupStart := 0
	for i, t := range ts {
		if !timeWindow.IsWithin(t, groupKey) {
			// Emit new row and re-init aggState

			o := firstFloat32(scanOpen[groupStart:i])
			h := maxFloat32(scanHigh[groupStart:i])
			l := minFloat32(scanLow[groupStart:i])
			c := lastFloat32(scanClose[groupStart:i])
			v := sumFloat32(scanVolume[groupStart:i])
			outEpoch = append(outEpoch, groupKey.Unix())
			outOpen = append(outOpen, o)
			outHigh = append(outHigh, h)
			outLow = append(outLow, l)
			outClose = append(outClose, c)
			outVolume = append(outVolume, v)
			groupKey = timeWindow.Truncate(t)
			groupStart = i
		}
	}
	o := firstFloat32(scanOpen[groupStart:])
	h := maxFloat32(scanHigh[groupStart:])
	l := minFloat32(scanLow[groupStart:])
	c := lastFloat32(scanClose[groupStart:])
	v := sumFloat32(scanVolume[groupStart:])
	outEpoch = append(outEpoch, groupKey.Unix())
	outOpen = append(outOpen, o)
	outHigh = append(outHigh, h)
	outLow = append(outLow, l)
	outClose = append(outClose, c)
	outVolume = append(outVolume, v)

	outCsm := io.NewColumnSeries()
	outCsm.AddColumn("Epoch", outEpoch)
	outCsm.AddColumn("Open", outOpen)
	outCsm.AddColumn("High", outHigh)
	outCsm.AddColumn("Low", outLow)
	outCsm.AddColumn("Close", outClose)
	outCsm.AddColumn("Volume", outVolume)

	// TODO: create RowSeries without proxying via ColumnSeries
	rs := cs.ToRowSeries(*tbk)
	return rs
}

func getWriter(theInstance *executor.InstanceMetadata, tbk *io.TimeBucketKey, year int16, dataShapes []io.DataShape) (*executor.Writer, error) {
	catalogDir := theInstance.CatalogDir
	tbi, err := catalogDir.GetLatestTimeBucketInfoFromKey(tbk)
	// TODO: refactor to common code
	// TODO: check existing file with new dataShapes
	if err != nil {
		tf, err := tbk.GetTimeFrame()
		if err != nil {
			return nil, err
		}

		tbi = io.NewTimeBucketInfo(
			*tf, tbk.GetPathToYearFiles(catalogDir.GetPath()),
			"Created By Trigger", year,
			dataShapes, io.FIXED,
		)
		err = catalogDir.AddTimeBucket(tbk, tbi)
		if err != nil {
			if _, ok := err.(catalog.FileAlreadyExists); !ok {
				return nil, err
			}
		}
	}
	q := planner.NewQuery(theInstance.CatalogDir)
	q.AddTargetKey(tbk)
	parsed, err := q.Parse()
	if err != nil {
		return nil, err
	}
	return executor.NewWriter(parsed, theInstance.TXNPipe, theInstance.CatalogDir)
}

func firstFloat32(values []float32) float32 {
	return values[0]
}

func minFloat32(values []float32) float32 {
	min := values[0]
	for _, val := range values[1:] {
		if val < min {
			min = val
		}
	}
	return min
}

func maxFloat32(values []float32) float32 {
	max := values[0]
	for _, val := range values[1:] {
		if val > max {
			max = val
		}
	}
	return max
}

func lastFloat32(values []float32) float32 {
	return values[len(values)-1]
}

func sumFloat32(values []float32) float32 {
	sum := float32(0)
	for _, val := range values {
		sum += val
	}
	return sum
}

func main() {
}
