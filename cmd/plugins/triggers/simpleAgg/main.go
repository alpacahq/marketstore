package main

import (
	"strconv"
	"strings"
	"time"

	"github.com/golang/glog"

	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/planner"
	"github.com/alpacahq/marketstore/plugins/trigger"
	"github.com/alpacahq/marketstore/utils"
	"github.com/alpacahq/marketstore/utils/io"
)

type SimpleAggTrigger struct {
	config map[string]interface{}
}

var _ trigger.Trigger = &SimpleAggTrigger{}

func NewTrigger(config map[string]interface{}) (trigger.Trigger, error) {
	glog.Infof("NewTrigger")
	return &SimpleAggTrigger{
		config: config,
	}, nil
}

func (s *SimpleAggTrigger) Fire(keyPath string, indexes []int64) {
	glog.Infof("keyPath=%s len(indexes)=%d", keyPath, len(indexes))

	headIndex := indexes[0]
	tailIndex := indexes[len(indexes)-1]

	// TODO precheck on loading
	destinations, ok := s.config["destinations"].([]string)
	if !ok {
		glog.Errorf("")
		return
	}
	for _, timeframe := range destinations {
		processFor(timeframe, keyPath, headIndex, tailIndex)
	}
}

func processFor(timeframe, keyPath string, headIndex, tailIndex int64) {
	theInstance := executor.ThisInstance
	catalogDir := theInstance.CatalogDir
	elements := strings.Split(keyPath, "/")
	tbkString := strings.Join(elements[:len(elements)-1], "/")
	tf := utils.NewTimeframe(elements[1])
	year, _ := strconv.Atoi(elements[len(elements)-1])
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
	w, err := getWriter(theInstance, targetTbk)
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
	scanVolume := cs.GetColumn("Volume").([]float64)

	outEpoch := make([]int64, 0)
	outOpen := make([]float32, 0)
	outHigh := make([]float32, 0)
	outLow := make([]float32, 0)
	outClose := make([]float32, 0)
	outVolume := make([]float64, 0)

	groupKey := ts[0]
	groupStart := 0
	for i, t := range ts {
		if !timeWindow.IsWithin(t, groupKey) {
			// Emit new row and re-init aggState

			o := firstFloat32(scanOpen[groupStart:i])
			h := maxFloat32(scanHigh[groupStart:i])
			l := minFloat32(scanLow[groupStart:i])
			c := lastFloat32(scanClose[groupStart:i])
			v := sumFloat64(scanVolume[groupStart:i])
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
	v := sumFloat64(scanVolume[groupStart:])
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

func getWriter(theInstance *executor.InstanceMetadata, tbk *io.TimeBucketKey) (*executor.Writer, error) {
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

func sumFloat64(values []float64) float64 {
	sum := float64(0)
	for _, val := range values {
		sum += val
	}
	return sum
}

func main() {
}
