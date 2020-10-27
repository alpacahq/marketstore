package reorg

import (
	"time"
	"errors"

	"github.com/alpacahq/marketstore/v4/uda"
	"github.com/alpacahq/marketstore/v4/utils/functions"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

const bucketkeySuffix = "/1D/ACTIONS"

var (
	requiredColumns = []io.DataShape{}

	optionalColumns = []io.DataShape{}

	initArgs = []io.DataShape{}
)

// const (
// 	NewRecord = iota
// 	UpdateRecord
// 	DeleteRecord
// )


type Reorg struct {
	uda.AggInterface
	ArgMap *functions.ArgumentMap

	epochs []int64
	output map[string][]float64
	tbk io.TimeBucketKey
}


func (r *Reorg) GetRequiredArgs() []io.DataShape {
	return requiredColumns
}
func (r *Reorg) GetOptionalArgs() []io.DataShape {
	return optionalColumns
}
func (r *Reorg) GetInitArgs() []io.DataShape {
	return initArgs
}


func (r *Reorg) New() (uda.AggInterface, *functions.ArgumentMap) {
	rn := new(Reorg)
	rn.ArgMap = functions.NewArgumentMap(requiredColumns, optionalColumns...)
	return rn, rn.ArgMap
}


func (r *Reorg) Init(args ...interface{}) error {
	r.Reset()
	return nil
}

func (r *Reorg) SetTimeBucketKey(tbk io.TimeBucketKey) {
	r.tbk = tbk
} 

func (r *Reorg) Reset() {
	// rreset some inner state here
}

func (r *Reorg) Accum(cols io.ColumnInterface) error {
	epochs, ok := cols.GetColumn("Epoch").([]int64)
	log.Info("Loaded from %+v to %+v", time.Unix(epochs[0], 0), time.Unix(epochs[len(epochs)-1], 0))
	if !ok {
		return errors.New("Reorg: Input data must have an Epoch column!")
	}
	r.epochs = epochs
	for _, ds := range cols.GetDataShapes() {
		if ds.Type == io.FLOAT64 {
			r.output[ds.Name] = cols.GetColumn(ds.Name).([]float64)
		}
	}

	cusip := r.tbk.GetItemInCategory("Symbol")
	rate_changes := GetRateChanges(cusip)
	log.Info("Rate change events: %d", len(rate_changes))
	// rate changes always contains 1.0 at the maximum available time
	ri := len(rate_changes) - 1
	rate := rate_changes[ri].Rate
	
	// beware, GetTime converts each unix timestamp to the system date, which 
	// times = cols.GetTime()
	for i:=len(epochs)-1; i >= 0; i-- {
		if ri > 0 && epochs[i] < rate_changes[ri-1].Epoch {
			ri--
			rate *= rate_changes[ri].Rate
			log.Info("%+v Rate changed to %.3f (%d)", time.Unix(epochs[i], 0), rate, rate_changes[ri].Textnumber)
		}
		for _, c := range r.output {
			c[i] *= rate
		}
	}

	return nil
}

func (r *Reorg) Output() *io.ColumnSeries {
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", r.epochs)
	for name, column := range r.output {
		cs.AddColumn(name, column)
	}
	return cs
}
