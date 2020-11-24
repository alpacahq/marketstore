package adjust

import (
	"errors"
	"math"
	"strings"

	"github.com/alpacahq/marketstore/v4/uda"
	"github.com/alpacahq/marketstore/v4/utils/functions"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

const calcSplit = "split"
const calcDividend = "dividend"
const roundToDecimals = 4

var (
	requiredColumns = []io.DataShape{}

	optionalColumns = []io.DataShape{}

	initArgs = []io.DataShape{}

	rounderNum = math.Pow(10, roundToDecimals)
)

type Adjust struct {
	uda.AggInterface
	ArgMap *functions.ArgumentMap

	AdjustDividend bool
	AdjustSplit    bool

	epochs         []int64
	output         map[io.DataShape]interface{}
	skippedColumns map[string]interface{}

	tbk io.TimeBucketKey
}

func (adj *Adjust) GetRequiredArgs() []io.DataShape {
	return requiredColumns
}
func (adj *Adjust) GetOptionalArgs() []io.DataShape {
	return optionalColumns
}
func (adj *Adjust) GetInitArgs() []io.DataShape {
	return initArgs
}

func (adj *Adjust) New() (uda.AggInterface, *functions.ArgumentMap) {
	rn := new(Adjust)

	rn.ArgMap = functions.NewArgumentMap(requiredColumns, optionalColumns...)
	rn.output = map[io.DataShape]interface{}{}
	rn.skippedColumns = map[string]interface{}{}
	return rn, rn.ArgMap
}

func (adj *Adjust) Init(args ...interface{}) error {
	adj.Reset()
	if len(args) == 0 {
		adj.AdjustSplit = true
		adj.AdjustDividend = true
		return nil
	}
	adj.AdjustSplit = false
	adj.AdjustDividend = false
	for _, arg := range args {
		switch _arg := arg.(type) {
		case []string:
			for _, p := range _arg {
				switch strings.ToLower(p) {
				case calcSplit:
					adj.AdjustSplit = true
				case calcDividend:
					adj.AdjustDividend = true
				}
			}
		case string:
			switch strings.ToLower(_arg) {
			case calcSplit:
				adj.AdjustSplit = true
			case calcDividend:
				adj.AdjustDividend = true
			}
		}
	}
	return nil
}

func (adj *Adjust) SetTimeBucketKey(tbk io.TimeBucketKey) {
	adj.tbk = tbk
}

func (adj *Adjust) Reset() {
	// intentionally left empty
}

func (adj *Adjust) Accum(cols io.ColumnInterface) error {
	epochs, ok := cols.GetColumn("Epoch").([]int64)
	if !ok {
		return errors.New("adjust: Input data must have an Epoch column")
	}
	adj.epochs = epochs
	for _, ds := range cols.GetDataShapes() {
		if ds.Name == "Epoch" || ds.Name == "Nanoseconds" {
			continue
		}
		// hacky, hacky...
		if ds.Type == io.FLOAT64 || ds.Name == "Volume" {
			adj.output[ds] = cols.GetColumn(ds.Name)
		} else {
			adj.skippedColumns[ds.Name] = cols.GetColumn(ds.Name)
		}
	}

	symbol := adj.tbk.GetItemInCategory("Symbol")
	rateChanges := GetRateChanges(symbol, adj.AdjustSplit, adj.AdjustDividend)
	if len(rateChanges) == 0 {
		return nil
	}

	// always append a default no-op rate change to help avoid handling edge cases below
	rateChanges = append(rateChanges, RateChange{Epoch: math.MaxInt64, Rate: 1, Textnumber: 0, Type: 0})

	// start with the default no-op rate 1.0
	ri := len(rateChanges) - 1
	rate := rateChanges[ri].Rate

	// start from the end of the buffer and iterate backwards toward the beginning, applying rate changes as they occur in time
	for i := len(epochs) - 1; i >= 0; i-- {
		// check if the current epoch is before the next rate change action, and if it is, then accumulate their rate changes
		// 	- mainly for taking care of events occurred after the last epoch in the current dataseet
		// 	- also handles a highly unlikely case when multiple rate change events occurs at the same time (eg. split and dividend)
		for ; ri > 0 && (epochs[i] < rateChanges[ri-1].Epoch); ri-- {
			rate *= rateChanges[ri-1].Rate
		}
		for _, col := range adj.output {
			switch c := col.(type) {
			case []float64:
				c[i] = math.Round((c[i]/rate)*rounderNum) / rounderNum
			case []int64:
				c[i] = int64(float64(c[i]) * rate)
			}
		}
	}
	return nil
}

func (adj *Adjust) Output() *io.ColumnSeries {
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", adj.epochs)
	for ds, column := range adj.output {
		cs.AddColumn(ds.Name, column)
	}
	for name, column := range adj.skippedColumns {
		cs.AddColumn(name, column)
	}
	return cs
}
