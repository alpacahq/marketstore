package tickcandler

import (
	"fmt"

	"github.com/alpacahq/marketstore/v4/catalog"
	"github.com/alpacahq/marketstore/v4/contrib/candler"
	"github.com/alpacahq/marketstore/v4/uda"
	"github.com/alpacahq/marketstore/v4/utils/functions"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

var (
	requiredColumns = []io.DataShape{
		{Name: "CandlePrice", Type: io.FLOAT32},
	}

	/*
	   For the optional inputs, we'll postpend the input names mapped to each optional
	   for output, for example: if we map user input "Volume" to "Sum", the output
	   will be "Sum_Volume"
	*/
	optionalColumns = []io.DataShape{
		{Name: "Sum", Type: io.FLOAT32},
		{Name: "Avg", Type: io.FLOAT32},
	}

	initArgs = []io.DataShape{
		{Name: "Timeframe", Type: io.STRING},
	}
)

type TickCandler struct {
	*candler.Candler
}

func (c TickCandler) New(_ bool) (ica uda.AggInterface, am *functions.ArgumentMap) {
	ca := &TickCandler{candler.NewCandler(requiredColumns, optionalColumns)}
	return ca, ca.ArgMap
}

func (ca *TickCandler) GetRequiredArgs() []io.DataShape {
	return requiredColumns
}
func (ca *TickCandler) GetOptionalArgs() []io.DataShape {
	return optionalColumns
}
func (ca *TickCandler) GetInitArgs() []io.DataShape {
	return initArgs
}

/*
	Accum() sends new data to the aggregate
*/
func (ca *TickCandler) Accum(cols io.ColumnInterface, _ *catalog.Directory) error {
	if cols.Len() == 0 {
		return fmt.Errorf("Empty input to Accum")
	}
	/*
		Get the input column for "Price"
	*/
	priceCols := ca.ArgMap.GetMappedColumns(requiredColumns[0].Name)
	price, err := candler.GetAverageColumnFloat32(cols, priceCols)
	if err != nil {
		return err
	}

	/*
		Get the time column
	*/
	ts, err := cols.GetTime()
	/*
		Update each candle
		Prepare a consolidated map of columns for use in updating sums
	*/
	var sumCols map[string][]float32
	if len(ca.AccumSumNames) != 0 {
		sumCols = make(map[string][]float32)
		for _, name := range ca.AccumSumNames {
			sumCols[name], err = uda.ColumnToFloat32(cols, name)
			if err != nil {
				return err
			}
		}
	}
	var candle *candler.Candle
	for i, t := range ts {
		candle = ca.GetCandle(t, candle)
		candle.AddCandle(t, price[i])
		/*
			Iterate over the candle's named columns that need sums
		*/
		for _, name := range ca.AccumSumNames {
			candle.SumMap[name] += float64(sumCols[name][i])
		}
		candle.Count++
	}
	return nil
}

/*
Utility Functions
*/

func (ca *TickCandler) SetTimeBucketKey(tbk io.TimeBucketKey) {
	// for compatibility reasons only
}
