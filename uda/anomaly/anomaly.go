package anomaly

import (
	"fmt"
	"github.com/alpacahq/marketstore/v4/uda"
	"github.com/alpacahq/marketstore/v4/utils/functions"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"gonum.org/v1/gonum/floats"
	"gonum.org/v1/gonum/stat"
	"math"
	"sort"
	"strconv"
	"strings"
)

var (
	requiredColumns = []io.DataShape{}
	optionalColumns = []io.DataShape{}
	initArgs        = []io.DataShape{}
)

const (
	DetectByZScore         = "z_score"
	DetectByFixedPct       = "fixed_pct"
	DefaultZScoreThreshold = 3.0
)

type Anomaly struct {
	uda.AggInterface

	ArgMap        *functions.ArgumentMap
	Columns       []string
	DetectionType string
	Threshold     float64

	AnomalyIdxsByColumn map[int64]uint64
	Input               *io.ColumnInterface
}

func (a Anomaly) New() (out uda.AggInterface, am *functions.ArgumentMap) {
	gx := NewAnomaly(requiredColumns, optionalColumns)
	return gx, gx.ArgMap
}

func NewAnomaly(inputColumns, optionalInputColumns []io.DataShape) (g *Anomaly) {
	g = new(Anomaly)
	g.ArgMap = functions.NewArgumentMap(inputColumns, optionalInputColumns...)
	return g
}

func (a *Anomaly) GetRequiredArgs() []io.DataShape {
	return requiredColumns
}
func (a *Anomaly) GetOptionalArgs() []io.DataShape {
	return optionalColumns
}
func (a *Anomaly) GetInitArgs() []io.DataShape {
	return initArgs
}

func (a *Anomaly) Init(args ...interface{}) error {
	a.Reset()

	// select anomaly('bid,ask', 'fixed_pct', 0.15) from `ORCL/1Sec/TRADE`;
	// select anomaly('price,qty', 'z_score', 3.0) from `ORCL/1Sec/TRADE`;

	if len(args) != 1 && len(args[0].([]string)) != 3 {
		return fmt.Errorf("not enough parameters. expected: columns, detectionType, threshold")
	}

	argz := args[0].([]string)
	columns := argz[0]
	detectionType := argz[1]
	threshold := argz[2]

	a.Columns = strings.Split(columns, ",")

	switch detectionType {
	case DetectByFixedPct:
		fallthrough
	case DetectByZScore:
		break
	default:
		return fmt.Errorf("invalid detection type: %v", detectionType)
	}
	a.DetectionType = detectionType

	var err error
	a.Threshold, err = strconv.ParseFloat(threshold, 10)
	if err != nil {
		return fmt.Errorf("error parsing threshold: %w", err)
	}

	return nil
}

func (a *Anomaly) Reset() {
	a.AnomalyIdxsByColumn = make(map[int64]uint64)
	a.Input = nil
	a.DetectionType = DetectByZScore
	a.Threshold = DefaultZScoreThreshold
}

func (a *Anomaly) Accum(cols io.ColumnInterface) (err error) {
	a.Input = &cols

	if cols.Len() == 0 {
		return nil
	}

	for columnNr, columnName := range a.Columns {
		err = a.detect(cols, columnName, columnNr)
		if err != nil {
			return err
		}
	}

	return nil
}

func (a Anomaly) detect(cols io.ColumnInterface, columnName string, columnNr int) error {
	epochs := cols.GetColumn("Epoch").([]int64)
	columnData, err := uda.ColumnToFloat64(cols, columnName)
	if err != nil {
		return err
	}

	if columnData == nil {
		return fmt.Errorf("no data available")
	}

	if len(columnData) < 2 {
		return fmt.Errorf("not enough data available")
	}

	switch a.DetectionType {
	case DetectByZScore:
		a.detectByZSCore(epochs, columnData, columnNr)
	case DetectByFixedPct:
		size := len(columnData)

		// pctChange = (a - b)/a
		pctChange := make([]float64, size-1)
		floats.SubTo(pctChange, columnData[:size-1], columnData[1:])
		floats.DivTo(pctChange, pctChange, columnData[:size-1])

		a.detectByFixedPct(epochs[1:], pctChange, columnNr)
	default:
		return fmt.Errorf("invalid detection type: %v", a.DetectionType)
	}

	return nil
}

func (a *Anomaly) detectByZSCore(epochs []int64, series []float64, columnNr int) {
	m := stat.Mean(series, nil)
	s := stat.StdDev(series, nil)
	if s == 0 {
		// no deviation
		return
	}

	for i, x := range series {
		if math.Abs(stat.StdScore(x, m, s)) >= a.Threshold {
			epoch := epochs[i]
			previousValue := uint64(0)
			if _, ok := a.AnomalyIdxsByColumn[epoch]; ok {
				previousValue = a.AnomalyIdxsByColumn[epoch]
			}
			a.AnomalyIdxsByColumn[epoch] = previousValue | 1<<columnNr
		}
	}
}

func (a *Anomaly) detectByFixedPct(epochs []int64, series []float64, columnNr int) {
	for i, x := range series {
		if math.Abs(x) >= a.Threshold {
			epoch := epochs[i]
			previousValue := uint64(0)
			if _, ok := a.AnomalyIdxsByColumn[epoch]; ok {
				previousValue = a.AnomalyIdxsByColumn[epoch]
			}
			a.AnomalyIdxsByColumn[epoch] = previousValue | 1<<columnNr
		}
	}
}

// Returns `Epoch, ColumnsBitmap` where ColumnsBitmap represents
// anomalies found in the columns by using the column index as
// bitmap position for the signal. Example:
//   - Given three columns: open,high,close
//   - When anomalies found at Epoch 0,1,2: open, open+high, open+close
//   - Then the returned bitmap looks as follows:
//     Epoch 0:  1 << 0          = 1
//     Epoch 1:  1 << 0 | 1 << 2 = 3
//     Epoch 2:  1 << 0 | 1 << 3 = 5
func (a *Anomaly) Output() *io.ColumnSeries {
	cs := io.NewColumnSeries()

	resultRows := len(a.AnomalyIdxsByColumn)
	epochs := make([]int64, resultRows)
	columns := make([]uint64, resultRows)

	if len(a.AnomalyIdxsByColumn) > 0 && a.Input != nil {
		var anomalyEpochsOrdered []int64
		for k := range a.AnomalyIdxsByColumn {
			anomalyEpochsOrdered = append(anomalyEpochsOrdered, k)
		}
		sort.Slice(anomalyEpochsOrdered,
			func(i, j int) bool {
				return anomalyEpochsOrdered[i] < anomalyEpochsOrdered[j]
			})

		for i, epoch := range anomalyEpochsOrdered {
			epochs[i] = epoch
			columns[i] = a.AnomalyIdxsByColumn[epoch]
		}
	}

	cs.AddColumn("Epoch", epochs)
	cs.AddColumn("ColumnsBitmap", columns)

	return cs
}
