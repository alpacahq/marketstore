package candler

import (
	"fmt"
	"sort"
	"time"

	"github.com/alpacahq/marketstore/v4/uda"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/functions"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

/*
	Superclass for the Candler subclasses
*/

var (
	/*
	   This is filled in for example purposes, should be overridden in implementation
	*/
	requiredColumns = []io.DataShape{
		{Name: "Open", Type: io.FLOAT32},
		{Name: "High", Type: io.FLOAT32},
		{Name: "Low", Type: io.FLOAT32},
		{Name: "Close", Type: io.FLOAT32},
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

type Candler struct {
	uda.AggInterface

	// Input arguments mapping
	ArgMap *functions.ArgumentMap
	/*
	   Manages one timeframe, creates candles in that timeframe from
	   input and outputs them on demand.
	*/
	MyCD *utils.CandleDuration
	CMap CandleMap
	/*
	   We need to keep an ordered list of sum and avg names to ensure ordered output
	*/
	SumNames, AvgNames []string // A cache of the column names that are summed and averaged in the candles
	AccumSumNames      []string // Consolidated list of names either summed or averaged
}

func (ca *Candler) GetRequiredArgs() []io.DataShape {
	return requiredColumns
}
func (ca *Candler) GetOptionalArgs() []io.DataShape {
	return optionalColumns
}
func (ca *Candler) GetInitArgs() []io.DataShape {
	return initArgs
}

/*
OVERRIDES - these methods should be overridden in a concrete implementation of this class
*/
/*
	OVERRIDE THIS METHOD
	Accum() sends new data to the aggregate
*/
func (ca *Candler) Accum(cols io.ColumnInterface) error {
	return fmt.Errorf("Accum called from base class, must override implementation")
}

/*
	OVERRIDE THIS METHOD
	Creates a new candler using the arguments of the specific implementation
	for inputColumns and optionalInputColumns
*/
func (c Candler) New() (ca *Candler, am *functions.ArgumentMap) {
	ca = NewCandler(requiredColumns, optionalColumns)
	return ca, ca.ArgMap
}

/*
CONCRETE - these may be suitable methods for general usage
*/
func NewCandler(inputColumns, optionalInputColumns []io.DataShape) (ca *Candler) {
	ca = new(Candler)
	ca.ArgMap = functions.NewArgumentMap(inputColumns, optionalInputColumns...)
	return ca
}
func (ca *Candler) Init(args ...interface{}) error {
	if len(args) != 1 {
		return fmt.Errorf("Init requires a *utils.CandleDuration as the argument")
	}

	var tfstring string
	switch val := args[0].(type) {
	case string:
		tfstring = val
	case *string:
		tfstring = *val
	case *[]string:
		if len(*val) != 1 {
			return fmt.Errorf("Argument passed to Init() is not a string")
		}
		tfstring = (*val)[0]
	case []string:
		if len(val) != 1 {
			return fmt.Errorf("Argument passed to Init() is not a string")
		}
		tfstring = val[0]
	}
	cd := utils.CandleDurationFromString(tfstring)

	if ca == nil {
		return fmt.Errorf("Init called without calling New()")
	}
	if cd == nil {
		return fmt.Errorf("No suitable timeframe provided")
	}
	if unmapped := ca.ArgMap.Validate(); unmapped != nil {
		return fmt.Errorf("Unmapped columns: %s", unmapped)
	}
	ca.MyCD = cd
	ca.CMap = make(CandleMap)
	/*
		Build the cache of summed input column names
	*/
	ca.SumNames = make([]string, 0)
	ca.AvgNames = make([]string, 0)
	ca.AccumSumNames = make([]string, 0)
	for _, ds := range optionalColumns {
		/*
			This currently includes Sum and Avg
		*/
		//		fmt.Println("Optionals: ", ds.Name, ca.argMap.GetMappedColumns(ds.Name))
		switch ds.Name {
		case "Sum":
			for _, dds := range ca.ArgMap.GetMappedColumns("Sum") {
				ca.SumNames = append(ca.SumNames, dds.Name)
			}
		case "Avg":
			for _, dds := range ca.ArgMap.GetMappedColumns("Avg") {
				ca.AvgNames = append(ca.AvgNames, dds.Name)
			}
		}
	}
	consMap := make(map[string]struct{})
	for _, name := range ca.SumNames {
		consMap[name] = struct{}{}
	}
	for _, name := range ca.AvgNames {
		consMap[name] = struct{}{}
	}
	for name := range consMap {
		ca.AccumSumNames = append(ca.AccumSumNames, name)
	}
	return nil
}

/*
	Output() returns the currently valid output of this aggregate
*/
func (ca *Candler) Output() *io.ColumnSeries {
	dataShapes := []io.DataShape{
		{Name: "Epoch", Type: io.INT64},
		{Name: "Open", Type: io.FLOAT32},
		{Name: "High", Type: io.FLOAT32},
		{Name: "Low", Type: io.FLOAT32},
		{Name: "Close", Type: io.FLOAT32},
	}
	for _, name := range ca.SumNames {
		dataShapes = append(dataShapes,
			io.DataShape{Name: name + "_SUM", Type: io.FLOAT64})
	}

	for _, name := range ca.AvgNames {
		dataShapes = append(dataShapes,
			io.DataShape{Name: name + "_AVG", Type: io.FLOAT64})
	}

	var tsa uda.OrderedTime
	for tkey := range ca.CMap {
		tsa = append(tsa, tkey)
	}
	sort.Sort(tsa)

	var dataBuf []byte
	for _, tkey := range tsa {
		cdl := ca.CMap[tkey]
		dataBuf = append(dataBuf, cdl.SerializeToRowData(ca.SumNames, ca.AvgNames)...)
	}

	rows := io.NewRows(dataShapes, dataBuf)
	catt := io.CandleAttributes(io.OHLC)
	rows.SetCandleAttributes(&catt)
	return rows.ToColumnSeries()
}

/*
	Reset() puts the aggregate state back to "new"
*/
func (ca *Candler) Reset() {
	ca.Init(ca.MyCD.String)
}

func (ca *Candler) GetCandle(t time.Time, cndl ...*Candle) *Candle {
	/*
		Returns a candle matching the start time "t", or creates
		a new one if one does not already exist
	*/
	candleTime := ca.MyCD.Truncate(t)
	/*
		If a candle is passed in, determine if it is suitable for this time
	*/
	if len(cndl) != 0 {
		if cndl[0] != nil {
			if cndl[0].StartTime == candleTime {
				return cndl[0]
			}
		}
	}
	/*
		We must provide a new candle if there isn't one with this time in the map
	*/
	if _, ok := ca.CMap[candleTime]; !ok {
		ca.CMap[candleTime] = NewCandle(candleTime, ca.MyCD, ca.SumNames, ca.AvgNames)
	}
	return ca.CMap[candleTime]
}

/*
*********************************** Candle *******************************************
- Represents quantities within an interval of time
- Always has Open, High, Low, Close prices defined within
- Optionally has averaged and summed quantities inside
- Has a starting time representing the candle, it begins the interval
*********************************** Candle *******************************************
*/
type EOHLCStruct struct {
	Epoch                  int64
	Open, High, Low, Close float32
}
type Candle struct {
	StartTime time.Time
	Duration  *utils.CandleDuration
	/*
		Every candle has OHLC
	*/
	EOHLC               EOHLCStruct
	OpenTime, CloseTime time.Time // The time at which the Open and Close prices happened
	/*
		Some candles optionally sum quantities like "Volume" from the
		input columns
	*/
	SumMap map[string]float64 // One sum per mapped column
	Count  int64              // Counts the elements incorporated in the Sums
	/*
		Does this candle have complete data? Sometimes we can tell...
	*/
	Complete bool
}

func NewCandle(startTime time.Time, cd *utils.CandleDuration, sumColumns, avgColumns []string) (ca *Candle) {
	st := cd.Truncate(startTime)
	ep := st.Unix()
	ca = &Candle{
		StartTime: st,
		OpenTime:  time.Time{},
		CloseTime: time.Time{},
		Duration:  cd,
		EOHLC:     EOHLCStruct{ep, 0, 0, 0, 0},
	}
	if len(sumColumns) != 0 || len(avgColumns) != 0 {
		ca.SumMap = make(map[string]float64)
		for _, name := range sumColumns {
			ca.SumMap[name] = 0
		}
		for _, name := range avgColumns {
			ca.SumMap[name] = 0
		}
	}
	return ca
}

func (ca *Candle) IsWithin(ts time.Time) bool {
	return ca.Duration.IsWithin(ts, ca.StartTime)
}

func (ca *Candle) AddCandle(ts time.Time, prices ...float32) bool {
	/*
		This routine works with both ticks and candles as input
		- A tick will have a single price
		- A candle will have open, high, low, close prices
	*/
	var open, high, low, close float32
	if len(prices) == 1 {
		open, high, low, close = prices[0], prices[0], prices[0], prices[0]
	} else {
		open, high, low, close = prices[0], prices[1], prices[2], prices[3]
	}
	/*
		The input price is used to update the candle if the time is within
		The return value indicates whether the time was within or not
	*/
	if !ca.IsWithin(ts) {
		return false
	}
	if ca.OpenTime.IsZero() {
		ca.EOHLC.Open, ca.EOHLC.High, ca.EOHLC.Low, ca.EOHLC.Close = open, high, low, close
		ca.OpenTime, ca.CloseTime = ts, ts
	}
	if ts.Before(ca.OpenTime) {
		ca.EOHLC.Open = open
		ca.OpenTime = ts
	}
	if ts.After(ca.CloseTime) {
		ca.EOHLC.Close = close
		ca.CloseTime = ts
	}
	if high > ca.EOHLC.High {
		ca.EOHLC.High = high
	}
	if low < ca.EOHLC.Low {
		ca.EOHLC.Low = low
	}
	return true
}

func (ca *Candle) SerializeToRowData(sumNames, avgNames []string) (rowBuf []byte) {
	rowBuf, _ = io.Serialize([]byte{}, ca.EOHLC)
	for _, name := range sumNames {
		rowBuf, _ = io.Serialize(rowBuf, ca.SumMap[name])
	}
	for _, name := range avgNames {
		rowBuf, _ = io.Serialize(rowBuf, ca.SumMap[name]/float64(ca.Count))
	}
	return rowBuf
}

/*
Map of start times to active candles
*/
type CandleMap map[time.Time]*Candle

/*
Utility Functions
*/
func GetAverageColumnFloat32(cols io.ColumnInterface, srcCols []io.DataShape) (avgCol []float32, err error) {
	numberCols := len(srcCols)
	if numberCols == 1 {
		name := srcCols[0].Name
		col, err := uda.ColumnToFloat32(cols, name)
		if err != nil {
			return nil, err
		}
		return col, nil
	} else {
		/*
			Average the input columns to produce the price column
		*/
		colLen := cols.Len()
		avgCol = make([]float32, colLen)
		for _, ds := range srcCols {
			col, err := uda.ColumnToFloat32(cols, ds.Name)
			if err != nil {
				return nil, err
			}
			for i := 0; i < colLen; i++ {
				avgCol[i] += col[i]
			}
		}
		for i := 0; i < colLen; i++ {
			avgCol[i] /= float32(numberCols)
		}
	}
	return avgCol, nil
}
