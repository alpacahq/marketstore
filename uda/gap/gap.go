/*
 * File: /Users/robi/Documents/git.hub/marketstore/uda/gap/gap.go
 * Created Date: Thursday, February 28th 2019, 4:42:41 pm
 * Author: Robi Lin
 * -----
 * Last Modified:
 * Modified By:
 * -----
 * Copyright (c) 2019 QK Capital
 *
 * Description:
 *
 */
package gap

import (
	"fmt"
	"math"
	"time"

	"gonum.org/v1/gonum/floats"
	"gonum.org/v1/gonum/stat"

	"github.com/alpacahq/marketstore/v4/uda"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/functions"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

var (
	requiredColumns = []io.DataShape{}

	optionalColumns = []io.DataShape{}

	initArgs = []io.DataShape{}
)

type Gap struct {
	uda.AggInterface

	BigGapIdxs            []int
	Input                 *io.ColumnInterface
	avgGapIntervalSeconds int64
}

func (g *Gap) GetRequiredArgs() []io.DataShape {
	return requiredColumns
}

func (g *Gap) GetOptionalArgs() []io.DataShape {
	return optionalColumns
}

func (g *Gap) GetInitArgs() []io.DataShape {
	return initArgs
}

// Accum() sends new data to the aggregate
// Use Zscore to find out the big hole in data.
func (g *Gap) Accum(_ io.TimeBucketKey, _ *functions.ArgumentMap, cols io.ColumnInterface) (*io.ColumnSeries, error) {
	g.BigGapIdxs = []int{}
	g.Input = &cols

	if cols.Len() == 0 {
		return g.Output(), nil
	}

	epochs, err := uda.ColumnToFloat64(cols, "Epoch")

	if err != nil || epochs == nil || len(epochs) < 2 {
		return g.Output(), nil
	}

	size := len(epochs)
	// Time gap of two contiguous epochs
	gaps := make([]float64, size-1)
	floats.SubTo(gaps, epochs[1:], epochs[:size-1])

	// Big gap which exceed the avg time gap interval,
	// val of BigGapIdxs is the index of Epochs from data ColumnSeries
	if g.avgGapIntervalSeconds < 0 {
		// Z-Score
		m := stat.Mean(gaps, nil)
		s := stat.StdDev(gaps, nil)
		if s == 0 {
			s = 1
		}

		for i, x := range gaps {
			if math.Abs(stat.StdScore(x, m, s)) > 3 {
				g.BigGapIdxs = append(g.BigGapIdxs, i)
			}
		}
	} else {
		// Use specific threshold
		thresholdZ := float64(g.avgGapIntervalSeconds)

		for i, x := range gaps {
			if x > thresholdZ {
				g.BigGapIdxs = append(g.BigGapIdxs, i)
			}
		}
	}

	return g.Output(), nil
}

/*
	Creates a new count using the arguments of the specific implementation
	for inputColumns and optionalInputColumns
*/
func (g Gap) New(_ *functions.ArgumentMap, args ...interface{}) (out uda.AggInterface, err error) {
	gx := &Gap{
		BigGapIdxs:            []int{},
		Input:                 nil,
		avgGapIntervalSeconds: -1,
	}

	if len(args) > 0 {
		var tfstring string
		switch val := args[0].(type) {
		case string:
			tfstring = val
		case *string:
			tfstring = *val
		case *[]string:
			if len(*val) != 1 {
				return nil, fmt.Errorf("argument passed to Init() is not a string")
			}
			tfstring = (*val)[0]
		case []string:
			if len(val) != 1 {
				return nil, fmt.Errorf("Argument passed to Init() is not a string")
			}
			tfstring = val[0]
		}
		cd := utils.CandleDurationFromString(tfstring)
		if cd != nil {
			// fmt.Printf("Duration %v, args[0] %v, time.SEcond %v\n", cd.Duration(), args[0], time.Second)
			gx.avgGapIntervalSeconds = int64(cd.Duration() / time.Second)
		}
	}

	return gx, nil
}

/*
	Output() returns the currently valid output of this aggregate
*/
func (g *Gap) Output() *io.ColumnSeries {
	cs := io.NewColumnSeries()

	resultRows := len(g.BigGapIdxs)
	gapStartEpoch := make([]int64, resultRows)
	gapEndEpoch := make([]int64, resultRows)
	gapLength := make([]int64, resultRows)

	if len(g.BigGapIdxs) > 0 && g.Input != nil {
		cols := *g.Input
		epochs := cols.GetColumn("Epoch").([]int64)

		for i, idx := range g.BigGapIdxs {
			gapStartEpoch[i] = epochs[idx]
			gapEndEpoch[i] = epochs[idx+1]
			gapLength[i] = epochs[idx+1] - epochs[idx]
		}
	}

	cs.AddColumn("Epoch", gapStartEpoch)
	cs.AddColumn("End", gapEndEpoch)
	cs.AddColumn("Length", gapLength)

	return cs
}
