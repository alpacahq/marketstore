package tickcandler

import (
	"fmt"

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

func (c TickCandler) New(argMap *functions.ArgumentMap, args ...interface{}) (ica uda.AggInterface, err error) {
	cl := candler.Candler{}
	ca, err := cl.New(argMap, args...)
	return &TickCandler{ca}, err
}

func (c *TickCandler) GetRequiredArgs() []io.DataShape {
	return requiredColumns
}

func (c *TickCandler) GetOptionalArgs() []io.DataShape {
	return optionalColumns
}

func (c *TickCandler) GetInitArgs() []io.DataShape {
	return initArgs
}

/*
	Accum() sends new data to the aggregate
*/
func (c *TickCandler) Accum(_ io.TimeBucketKey, argMap *functions.ArgumentMap, cols io.ColumnInterface,
) (*io.ColumnSeries, error) {
	if cols.Len() == 0 {
		return nil, fmt.Errorf("empty input to Accum")
	}
	/*
		Get the input column for "Price"
	*/
	priceCols := argMap.GetMappedColumns(requiredColumns[0].Name)
	price, err := candler.GetAverageColumnFloat32(cols, priceCols)
	if err != nil {
		return nil, err
	}

	/*
		Get the time column
	*/
	ts, err := cols.GetTime()
	if err != nil {
		return nil, fmt.Errorf("get time column for tick candler: %w", err)
	}
	/*
		Update each candle
		Prepare a consolidated map of columns for use in updating sums
	*/
	var sumCols map[string][]float32
	if len(c.AccumSumNames) != 0 {
		sumCols = make(map[string][]float32)
		for _, name := range c.AccumSumNames {
			sumCols[name], err = uda.ColumnToFloat32(cols, name)
			if err != nil {
				return nil, err
			}
		}
	}
	var candle *candler.Candle
	for i, t := range ts {
		candle = c.GetCandle(t, candle)
		candle.AddCandle(t, price[i])
		/*
			Iterate over the candle's named columns that need sums
		*/
		for _, name := range c.AccumSumNames {
			candle.SumMap[name] += float64(sumCols[name][i])
		}
		candle.Count++
	}
	return c.Output()
}
