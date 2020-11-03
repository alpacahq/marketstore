package adjust

import (
	"errors"
	"strings"	

	"github.com/alpacahq/marketstore/v4/uda"
	"github.com/alpacahq/marketstore/v4/utils/functions"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

const bucketkeySuffix = "/1D/ACTIONS"
const calcSplit = "split"
const calcDividend = "dividend"


var (
	requiredColumns = []io.DataShape{}

	optionalColumns = []io.DataShape{}

	initArgs = []io.DataShape{}
)

type Adjust struct {
	uda.AggInterface
	ArgMap *functions.ArgumentMap

	AdjustDividend bool
	AdjustSplit bool 

	epochs []int64
	output map[string][]float32
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
	rn.output = map[string][]float32{}	
	return rn, rn.ArgMap
}


func (adj *Adjust) Init(args ...interface{}) error {
	adj.Reset()
	if len(args) == 0 {
		adj.AdjustSplit = true
		adj.AdjustDividend = true
		return nil
	} else {
		adj.AdjustSplit = false
		adj.AdjustDividend = false
	}
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
	// reset some inner state here
}

func (adj *Adjust) Accum(cols io.ColumnInterface) error {
	epochs, ok := cols.GetColumn("Epoch").([]int64)
	if !ok {
		return errors.New("Adjust: Input data must have an Epoch column!")
	}
	adj.epochs = epochs
	for _, ds := range cols.GetDataShapes() {
		if ds.Name != "Epoch" {
			col := cols.GetColumn(ds.Name)
			switch c := col.(type){
			case []float64: 
				tmp := make([]float32, len(c))
				for i, f := range c {
					tmp[i] = float32(f)
				}
				adj.output[ds.Name] = tmp
			case []float32:
				adj.output[ds.Name] = c
			}
		}
	}

	cusip := adj.tbk.GetItemInCategory("Symbol")
	rate_changes := GetRateChanges(cusip, adj.AdjustSplit, adj.AdjustDividend)
	log.Info("# of rate change events: %d", len(rate_changes))
	// rate changes always contains 1.0 at the maximum available time
	ri := len(rate_changes) - 1
	rate := float32(rate_changes[ri].Rate)
	
	// beware, GetTime converts each unix timestamp to the system date, which 
	// times = cols.GetTime()
	for i:=len(epochs)-1; i >= 0; i-- {
		if ri > 0 && epochs[i] < rate_changes[ri-1].Epoch {
			ri--
			rate *= float32(rate_changes[ri].Rate)
		}
		for _, c := range adj.output {
			c[i] /= rate
		}
	}

	return nil
}

func (adj *Adjust) Output() *io.ColumnSeries {
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", adj.epochs)
	for name, column := range adj.output {
		cs.AddColumn(name, column)
	}
	return cs
}
