package aggtrigger

import (
	"fmt"

	"github.com/alpacahq/marketstore/quanatee/plugins/quanatee_ondiskagg/aggtrigger/functions"
	"github.com/alpacahq/marketstore/utils/io"
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

func (ag *accumGroup) apply(start, end int) {
	for _, accumulator := range ag.accumulators {
		accumulator.apply(start, end)
	}
}

func (ag *accumGroup) addColumns(cs *io.ColumnSeries) {
	for i, param := range ag.params {
		cs.AddColumn(param.outputName, ag.accumulators[i].iout)
	}
}

func newAccumulator(cs *io.ColumnSeries, param accumParam) *accumulator {
	var ifunc, iout interface{}
	switch param.funcName {
	case "first":
		inColumn := cs.GetColumn(param.inputName)
		switch inColumn.(type) {
		case []float32:
			ifunc = functions.FirstFloat32
			iout = make([]float32, 0)
		case []float64:
			ifunc = functions.FirstFloat64
			iout = make([]float64, 0)
		case []int8:
			ifunc = functions.FirstInt8
			iout = make([]int8, 0)
		case []int:
			ifunc = functions.FirstInt
			iout = make([]int, 0)
		case []int16:
			ifunc = functions.FirstInt16
			iout = make([]int16, 0)
		case []int32:
			ifunc = functions.FirstInt32
			iout = make([]int32, 0)
		case []int64:
			ifunc = functions.FirstInt64
			iout = make([]int64, 0)
		case []uint8:
			ifunc = functions.FirstUint8
			iout = make([]uint8, 0)
		case []uint16:
			ifunc = functions.FirstUint16
			iout = make([]uint16, 0)
		case []uint:
			ifunc = functions.FirstUint
			iout = make([]uint, 0)
		case []uint32:
			ifunc = functions.FirstUint32
			iout = make([]uint32, 0)
		case []uint64:
			ifunc = functions.FirstUint64
			iout = make([]uint64, 0)
		default:
			fmt.Printf("no compatible function\n")
			return nil
		}
	case "max":
		inColumn := cs.GetColumn(param.inputName)
		switch inColumn.(type) {
		case []float32:
			ifunc = functions.MaxFloat32
			iout = make([]float32, 0)
		case []float64:
			ifunc = functions.MaxFloat64
			iout = make([]float64, 0)
		case []int8:
			ifunc = functions.MaxInt8
			iout = make([]int8, 0)
		case []int:
			ifunc = functions.MaxInt
			iout = make([]int, 0)
		case []int16:
			ifunc = functions.MaxInt16
			iout = make([]int16, 0)
		case []int32:
			ifunc = functions.MaxInt32
			iout = make([]int32, 0)
		case []int64:
			ifunc = functions.MaxInt64
			iout = make([]int64, 0)
		case []uint8:
			ifunc = functions.MaxUint8
			iout = make([]uint8, 0)
		case []uint16:
			ifunc = functions.MaxUint16
			iout = make([]uint16, 0)
		case []uint:
			ifunc = functions.MaxUint
			iout = make([]uint, 0)
		case []uint32:
			ifunc = functions.MaxUint32
			iout = make([]uint32, 0)
		case []uint64:
			ifunc = functions.MaxUint64
			iout = make([]uint64, 0)
		default:
			fmt.Printf("no compatible function\n")
			return nil
		}
	case "min":
		inColumn := cs.GetColumn(param.inputName)
		switch inColumn.(type) {
		case []float32:
			ifunc = functions.MinFloat32
			iout = make([]float32, 0)
		case []float64:
			ifunc = functions.MinFloat64
			iout = make([]float64, 0)
		case []int8:
			ifunc = functions.MinInt8
			iout = make([]int8, 0)
		case []int:
			ifunc = functions.MinInt
			iout = make([]int, 0)
		case []int16:
			ifunc = functions.MinInt16
			iout = make([]int16, 0)
		case []int32:
			ifunc = functions.MinInt32
			iout = make([]int32, 0)
		case []int64:
			ifunc = functions.MinInt64
			iout = make([]int64, 0)
		case []uint8:
			ifunc = functions.MinUint8
			iout = make([]uint8, 0)
		case []uint16:
			ifunc = functions.MinUint16
			iout = make([]uint16, 0)
		case []uint:
			ifunc = functions.MinUint
			iout = make([]uint, 0)
		case []uint32:
			ifunc = functions.MinUint32
			iout = make([]uint32, 0)
		case []uint64:
			ifunc = functions.MinUint64
			iout = make([]uint64, 0)
		default:
			fmt.Printf("no compatible function\n")
			return nil
		}
	case "last":
		inColumn := cs.GetColumn(param.inputName)
		switch inColumn.(type) {
		case []float32:
			ifunc = functions.LastFloat32
			iout = make([]float32, 0)
		case []float64:
			ifunc = functions.LastFloat64
			iout = make([]float64, 0)
		case []int8:
			ifunc = functions.LastInt8
			iout = make([]int8, 0)
		case []int:
			ifunc = functions.LastInt
			iout = make([]int, 0)
		case []int16:
			ifunc = functions.LastInt16
			iout = make([]int16, 0)
		case []int32:
			ifunc = functions.LastInt32
			iout = make([]int32, 0)
		case []int64:
			ifunc = functions.LastInt64
			iout = make([]int64, 0)
		case []uint8:
			ifunc = functions.LastUint8
			iout = make([]uint8, 0)
		case []uint16:
			ifunc = functions.LastUint16
			iout = make([]uint16, 0)
		case []uint:
			ifunc = functions.LastUint
			iout = make([]uint, 0)
		case []uint32:
			ifunc = functions.LastUint32
			iout = make([]uint32, 0)
		case []uint64:
			ifunc = functions.LastUint64
			iout = make([]uint64, 0)
		default:
			fmt.Printf("no compatible function\n")
			return nil
		}
	case "sum":
		inColumn := cs.GetColumn(param.inputName)
		switch inColumn.(type) {
		case []float32:
			ifunc = functions.SumFloat32
			iout = make([]float32, 0)
		case []float64:
			ifunc = functions.SumFloat64
			iout = make([]float64, 0)
		case []int8:
			ifunc = functions.SumInt8
			iout = make([]int8, 0)
		case []int:
			ifunc = functions.SumInt
			iout = make([]int, 0)
		case []int16:
			ifunc = functions.SumInt16
			iout = make([]int16, 0)
		case []int32:
			ifunc = functions.SumInt32
			iout = make([]int32, 0)
		case []int64:
			ifunc = functions.SumInt64
			iout = make([]int64, 0)
		case []uint8:
			ifunc = functions.SumUint8
			iout = make([]uint8, 0)
		case []uint16:
			ifunc = functions.SumUint16
			iout = make([]uint16, 0)
		case []uint:
			ifunc = functions.SumUint
			iout = make([]uint, 0)
		case []uint32:
			ifunc = functions.SumUint32
			iout = make([]uint32, 0)
		case []uint64:
			ifunc = functions.SumUint64
			iout = make([]uint64, 0)
		default:
			fmt.Printf("no compatible function\n")
			return nil
		}
	}
	case "avg":
		inColumn := cs.GetColumn(param.inputName)
		switch inColumn.(type) {
		case []float32:
			ifunc = functions.AvgFloat32
			iout = make([]float32, 0)
		case []float64:
			ifunc = functions.AvgFloat64
			iout = make([]float64, 0)
		default:
			fmt.Printf("no compatible function\n")
			return nil
		}
	}
	return &accumulator{
		iout:    iout,
		ifunc:   ifunc,
		ivalues: cs.GetColumn(param.inputName),
	}
}

func (ac *accumulator) apply(start, end int) {
	switch fn := ac.ifunc.(type) {
	case func([]float32) float32:
		ivalues := ac.ivalues
		out := ac.iout.([]float32)
		ac.iout = append(out, fn(ivalues.([]float32)[start:end]))
	case func([]float64) float64:
		ivalues := ac.ivalues
		out := ac.iout.([]float64)
		ac.iout = append(out, fn(ivalues.([]float64)[start:end]))
	case func([]int8) int8:
		ivalues := ac.ivalues
		out := ac.iout.([]int8)
		ac.iout = append(out, fn(ivalues.([]int8)[start:end]))
	case func([]int16) int16:
		ivalues := ac.ivalues
		out := ac.iout.([]int16)
		ac.iout = append(out, fn(ivalues.([]int16)[start:end]))
	case func([]int) int:
		ivalues := ac.ivalues
		out := ac.iout.([]int)
		ac.iout = append(out, fn(ivalues.([]int)[start:end]))
	case func([]int32) int32:
		ivalues := ac.ivalues
		out := ac.iout.([]int32)
		ac.iout = append(out, fn(ivalues.([]int32)[start:end]))
	case func([]int64) int64:
		ivalues := ac.ivalues
		out := ac.iout.([]int64)
		ac.iout = append(out, fn(ivalues.([]int64)[start:end]))
	case func([]uint8) uint8:
		ivalues := ac.ivalues
		out := ac.iout.([]uint8)
		ac.iout = append(out, fn(ivalues.([]uint8)[start:end]))
	case func([]uint16) uint16:
		ivalues := ac.ivalues
		out := ac.iout.([]uint16)
		ac.iout = append(out, fn(ivalues.([]uint16)[start:end]))
	case func([]uint) uint:
		ivalues := ac.ivalues
		out := ac.iout.([]uint)
		ac.iout = append(out, fn(ivalues.([]uint)[start:end]))
	case func([]uint32) uint32:
		ivalues := ac.ivalues
		out := ac.iout.([]uint32)
		ac.iout = append(out, fn(ivalues.([]uint32)[start:end]))
	case func([]uint64) uint64:
		ivalues := ac.ivalues
		out := ac.iout.([]uint64)
		ac.iout = append(out, fn(ivalues.([]uint64)[start:end]))
	default:
		panic("cannot apply")
	}
}
