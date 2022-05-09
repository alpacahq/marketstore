package aggtrigger

import (
	"fmt"

	"github.com/alpacahq/marketstore/v4/contrib/ondiskagg/aggtrigger/functions"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

type accumParam struct {
	inputName, funcName, outputName string
}

type accumGroup struct {
	accumulators []*accumulator
	params       []accumParam
}

type accumulator struct {
	ivalues interface{} // input column(s)
	iout    interface{} // output slice
	ifunc   interface{} // function
}

func newAccumGroup(cs *io.ColumnSeries, params []accumParam) *accumGroup {
	accumulators := []*accumulator{}
	for _, param := range params {
		accumulator := newAccumulator(cs, param)
		accumulators = append(accumulators, accumulator)
	}
	return &accumGroup{
		accumulators: accumulators,
		params:       params,
	}
}

func (ag *accumGroup) apply(start, end int) error {
	for _, accumulator := range ag.accumulators {
		err := accumulator.apply(start, end)
		if err != nil {
			return fmt.Errorf("apply to accumulator. start=%d, end=%d:%w", start, end, err)
		}
	}
	return nil
}

func (ag *accumGroup) addColumns(cs *io.ColumnSeries) {
	for i, param := range ag.params {
		cs.AddColumn(param.outputName, ag.accumulators[i].iout)
	}
}

var float32AccumFunc = map[string]interface{}{
	"first": functions.FirstFloat32,
	"max":   functions.MaxFloat32,
	"min":   functions.MinFloat32,
	"last":  functions.LastFloat32,
	"sum":   functions.SumFloat32,
}

var float64AccumFunc = map[string]interface{}{
	"first": functions.FirstFloat64,
	"max":   functions.MaxFloat64,
	"min":   functions.MinFloat64,
	"last":  functions.LastFloat64,
	"sum":   functions.SumFloat64,
}

var int8AccumFunc = map[string]interface{}{
	"first": functions.FirstInt8,
	"max":   functions.MaxInt8,
	"min":   functions.MinInt8,
	"last":  functions.LastInt8,
	"sum":   functions.SumInt8,
}

var intAccumFunc = map[string]interface{}{
	"first": functions.FirstInt,
	"max":   functions.MaxInt,
	"min":   functions.MinInt,
	"last":  functions.LastInt,
	"sum":   functions.SumInt,
}

var int16AccumFunc = map[string]interface{}{
	"first": functions.FirstInt16,
	"max":   functions.MaxInt16,
	"min":   functions.MinInt16,
	"last":  functions.LastInt16,
	"sum":   functions.SumInt16,
}

var int32AccumFunc = map[string]interface{}{
	"first": functions.FirstInt32,
	"max":   functions.MaxInt32,
	"min":   functions.MinInt32,
	"last":  functions.LastInt32,
	"sum":   functions.SumInt32,
}

var int64AccumFunc = map[string]interface{}{
	"first": functions.FirstInt64,
	"max":   functions.MaxInt64,
	"min":   functions.MinInt64,
	"last":  functions.LastInt64,
	"sum":   functions.SumInt64,
}

var uint8AccumFunc = map[string]interface{}{
	"first": functions.FirstUint8,
	"max":   functions.MaxUint8,
	"min":   functions.MinUint8,
	"last":  functions.LastUint8,
	"sum":   functions.SumUint8,
}

var uint16AccumFunc = map[string]interface{}{
	"first": functions.FirstUint16,
	"max":   functions.MaxUint16,
	"min":   functions.MinUint16,
	"last":  functions.LastUint16,
	"sum":   functions.SumUint16,
}

var uintAccumFunc = map[string]interface{}{
	"first": functions.FirstUint,
	"max":   functions.MaxUint,
	"min":   functions.MinUint,
	"last":  functions.LastUint,
	"sum":   functions.SumUint,
}

var uint32AccumFunc = map[string]interface{}{
	"first": functions.FirstUint32,
	"max":   functions.MaxUint32,
	"min":   functions.MinUint32,
	"last":  functions.LastUint32,
	"sum":   functions.SumUint32,
}

var uint64AccumFunc = map[string]interface{}{
	"first": functions.FirstUint64,
	"max":   functions.MaxUint64,
	"min":   functions.MinUint64,
	"last":  functions.LastUint64,
	"sum":   functions.SumUint64,
}

func newAccumulator(cs *io.ColumnSeries, param accumParam) *accumulator {
	var ifunc, iout interface{}

	inColumn := cs.GetColumn(param.inputName)
	switch inColumn.(type) {
	case []float32:
		ifunc = float32AccumFunc[param.funcName]
		iout = make([]float32, 0)
	case []float64:
		ifunc = float64AccumFunc[param.funcName]
		iout = make([]float64, 0)
	case []int8:
		ifunc = int8AccumFunc[param.funcName]
		iout = make([]int8, 0)
	case []int:
		ifunc = intAccumFunc[param.funcName]
		iout = make([]int, 0)
	case []int16:
		ifunc = int16AccumFunc[param.funcName]
		iout = make([]int16, 0)
	case []int32:
		ifunc = int32AccumFunc[param.funcName]
		iout = make([]int32, 0)
	case []int64:
		ifunc = int64AccumFunc[param.funcName]
		iout = make([]int64, 0)
	case []uint8:
		ifunc = uint8AccumFunc[param.funcName]
		iout = make([]uint8, 0)
	case []uint16:
		ifunc = uint16AccumFunc[param.funcName]
		iout = make([]uint16, 0)
	case []uint:
		ifunc = uintAccumFunc[param.funcName]
		iout = make([]uint, 0)
	case []uint32:
		ifunc = uint32AccumFunc[param.funcName]
		iout = make([]uint32, 0)
	case []uint64:
		ifunc = uint64AccumFunc[param.funcName]
		iout = make([]uint64, 0)
	default:
		log.Error("no compatible function")
		return nil
	}
	return &accumulator{
		iout:    iout,
		ifunc:   ifunc,
		ivalues: cs.GetColumn(param.inputName),
	}
}

func (ac *accumulator) apply(start, end int) error {
	switch fn := ac.ifunc.(type) {
	case func([]float32) float32:
		ival, ok := ac.ivalues.([]float32)
		if !ok {
			return fmt.Errorf("convert to float32 slice. ivalues=%v", ac.ivalues)
		}
		out, ok := ac.iout.([]float32)
		if !ok {
			return fmt.Errorf("convert to float32 slice. iout=%v", ac.iout)
		}
		out = append(out, fn(ival[start:end]))
		ac.iout = out
	case func([]float64) float64:
		ival, ok := ac.ivalues.([]float64)
		if !ok {
			return fmt.Errorf("convert to float64 slice. iout=%v", ac.ivalues)
		}
		out, ok := ac.iout.([]float64)
		if !ok {
			return fmt.Errorf("convert to float64 slice. iout=%v", ac.iout)
		}
		out = append(out, fn(ival[start:end]))
		ac.iout = out
	case func([]int8) int8:
		ival, ok := ac.ivalues.([]int8)
		if !ok {
			return fmt.Errorf("convert to int8 slice. iout=%v", ac.ivalues)
		}
		out, ok := ac.iout.([]int8)
		if !ok {
			return fmt.Errorf("convert to int8 slice. iout=%v", ac.iout)
		}
		out = append(out, fn(ival[start:end]))
		ac.iout = out
	case func([]int16) int16:
		ival, ok := ac.ivalues.([]int16)
		if !ok {
			return fmt.Errorf("convert to uint16 slice. iout=%v", ac.ivalues)
		}
		out, ok := ac.iout.([]int16)
		if !ok {
			return fmt.Errorf("convert to int16 slice. iout=%v", ac.iout)
		}
		out = append(out, fn(ival[start:end]))
		ac.iout = out
	case func([]int) int:
		ival, ok := ac.ivalues.([]int)
		if !ok {
			return fmt.Errorf("convert to int32 slice. iout=%v", ac.ivalues)
		}
		out, ok := ac.iout.([]int)
		if !ok {
			return fmt.Errorf("convert to int slice. iout=%v", ac.iout)
		}
		out = append(out, fn(ival[start:end]))
		ac.iout = out
	case func([]int32) int32:
		ival, ok := ac.ivalues.([]int32)
		if !ok {
			return fmt.Errorf("convert to int32 slice. iout=%v", ac.ivalues)
		}
		out, ok := ac.iout.([]int32)
		if !ok {
			return fmt.Errorf("convert to int32 slice. iout=%v", ac.iout)
		}
		out = append(out, fn(ival[start:end]))
		ac.iout = out
	case func([]int64) int64:
		ival, ok := ac.ivalues.([]int64)
		if !ok {
			return fmt.Errorf("convert to int64 slice. iout=%v", ac.ivalues)
		}
		out, ok := ac.iout.([]int64)
		if !ok {
			return fmt.Errorf("convert to int64 slice. iout=%v", ac.iout)
		}
		out = append(out, fn(ival[start:end]))
		ac.iout = out
	case func([]uint8) uint8:
		ival, ok := ac.ivalues.([]uint8)
		if !ok {
			return fmt.Errorf("convert to uint8 slice. iout=%v", ac.ivalues)
		}
		out, ok := ac.iout.([]uint8)
		if !ok {
			return fmt.Errorf("convert to uint8 slice. iout=%v", ac.iout)
		}
		out = append(out, fn(ival[start:end]))
		ac.iout = out
	case func([]uint16) uint16:
		ival, ok := ac.ivalues.([]uint16)
		if !ok {
			return fmt.Errorf("convert to uint16 slice. iout=%v", ac.ivalues)
		}
		out, ok := ac.iout.([]uint16)
		if !ok {
			return fmt.Errorf("convert to uint16 slice. iout=%v", ac.iout)
		}
		out = append(out, fn(ival[start:end]))
		ac.iout = out
	case func([]uint) uint:
		ival, ok := ac.ivalues.([]uint)
		if !ok {
			return fmt.Errorf("convert to uint slice. iout=%v", ac.ivalues)
		}
		out, ok := ac.iout.([]uint)
		if !ok {
			return fmt.Errorf("convert to uint slice. iout=%v", ac.iout)
		}
		out = append(out, fn(ival[start:end]))
		ac.iout = out
	case func([]uint32) uint32:
		ival, ok := ac.ivalues.([]uint32)
		if !ok {
			return fmt.Errorf("convert to uint32 slice. iout=%v", ac.ivalues)
		}
		out, ok := ac.iout.([]uint32)
		if !ok {
			return fmt.Errorf("convert to uint32 slice. ivalues=%v", ac.iout)
		}
		out = append(out, fn(ival[start:end]))
		ac.iout = out
	case func([]uint64) uint64:
		ival, ok := ac.ivalues.([]uint64)
		if !ok {
			return fmt.Errorf("convert to uint64 slice. iout=%v", ac.ivalues)
		}
		out, ok := ac.iout.([]uint64)
		if !ok {
			return fmt.Errorf("convert to uint64 slice. iout=%v", ac.iout)
		}
		out = append(out, fn(ival[start:end]))
		ac.iout = out
	default:
		return fmt.Errorf("unexpected ifunc type in an accumulator. ifunc=%v, ivalues=%v", ac.ifunc, ac.ivalues)
	}
	return nil
}
