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
	var accumulators []*accumulator
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

var accumMap = map[io.EnumElementType]func(funcName string, column interface{}) *accumulator{
	io.FLOAT32: func(funcName string, column interface{}) *accumulator {
		ifunc := float32AccumFunc[funcName]
		iout := make([]float32, 0)
		return &accumulator{iout: iout, ifunc: ifunc, ivalues: column}
	},
	io.FLOAT64: func(funcName string, column interface{}) *accumulator {
		ifunc := float64AccumFunc[funcName]
		iout := make([]float64, 0)
		return &accumulator{iout: iout, ifunc: ifunc, ivalues: column}
	},
	io.INT16: func(funcName string, column interface{}) *accumulator {
		ifunc := int16AccumFunc[funcName]
		iout := make([]int16, 0)
		return &accumulator{iout: iout, ifunc: ifunc, ivalues: column}
	},
	io.INT32: func(funcName string, column interface{}) *accumulator {
		ifunc := int32AccumFunc[funcName]
		iout := make([]int32, 0)
		return &accumulator{iout: iout, ifunc: ifunc, ivalues: column}
	},
	io.INT64: func(funcName string, column interface{}) *accumulator {
		ifunc := int64AccumFunc[funcName]
		iout := make([]int64, 0)
		return &accumulator{iout: iout, ifunc: ifunc, ivalues: column}
	},
	io.BYTE: func(funcName string, column interface{}) *accumulator {
		ifunc := int8AccumFunc[funcName]
		iout := make([]byte, 0)
		return &accumulator{iout: iout, ifunc: ifunc, ivalues: column}
	},
	io.UINT8: func(funcName string, column interface{}) *accumulator {
		ifunc := uint8AccumFunc[funcName]
		iout := make([]uint8, 0)
		return &accumulator{iout: iout, ifunc: ifunc, ivalues: column}
	},
	io.UINT16: func(funcName string, column interface{}) *accumulator {
		ifunc := uint16AccumFunc[funcName]
		iout := make([]uint16, 0)
		return &accumulator{iout: iout, ifunc: ifunc, ivalues: column}
	},
	io.UINT32: func(funcName string, column interface{}) *accumulator {
		ifunc := uint32AccumFunc[funcName]
		iout := make([]uint32, 0)
		return &accumulator{iout: iout, ifunc: ifunc, ivalues: column}
	},
	io.UINT64: func(funcName string, column interface{}) *accumulator {
		ifunc := uint64AccumFunc[funcName]
		iout := make([]uint64, 0)
		return &accumulator{iout: iout, ifunc: ifunc, ivalues: column}
	},
	io.BOOL: func(funcName string, column interface{}) *accumulator {
		log.Error("no compatible accum func for BOOL type column")
		return nil
	},
	io.NONE: func(funcName string, column interface{}) *accumulator {
		log.Error("no compatible accum func for NONE type column")
		return nil
	},
	io.STRING: func(funcName string, column interface{}) *accumulator {
		log.Error("no compatible accum func for STRING type column")
		return nil
	},
	io.STRING16: func(funcName string, column interface{}) *accumulator {
		log.Error("no compatible accum func for STRING16 type column")
		return nil
	},
}

func newAccumulator(cs *io.ColumnSeries, param accumParam) *accumulator {
	inColumn := cs.GetColumn(param.inputName)
	return accumMap[io.GetElementType(inColumn)](param.funcName, inColumn)
}

func (ac *accumulator) float32out(fn func([]float32) float32, start, end int) error {
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
	return nil
}

func (ac *accumulator) float64out(fn func([]float64) float64, start, end int) error {
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
	return nil
}

func (ac *accumulator) int8out(fn func([]int8) int8, start, end int) error {
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
	return nil
}

func (ac *accumulator) int16out(fn func([]int16) int16, start, end int) error {
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
	return nil
}

func (ac *accumulator) intout(fn func([]int) int, start, end int) error {
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
	return nil
}

func (ac *accumulator) int32out(fn func([]int32) int32, start, end int) error {
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
	return nil
}

func (ac *accumulator) int64out(fn func([]int64) int64, start, end int) error {
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
	return nil
}

func (ac *accumulator) uint8out(fn func([]uint8) uint8, start, end int) error {
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
	return nil
}

func (ac *accumulator) uint16out(fn func([]uint16) uint16, start, end int) error {
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
	return nil
}

func (ac *accumulator) uintout(fn func([]uint) uint, start, end int) error {
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
	return nil
}

func (ac *accumulator) uint32out(fn func([]uint32) uint32, start, end int) error {
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
	return nil
}

func (ac *accumulator) uint64out(fn func([]uint64) uint64, start, end int) error {
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
	return nil
}

func (ac *accumulator) apply(start, end int) error {
	switch fn := ac.ifunc.(type) {
	case func([]float32) float32:
		return ac.float32out(fn, start, end)
	case func([]float64) float64:
		return ac.float64out(fn, start, end)
	case func([]int8) int8:
		return ac.int8out(fn, start, end)
	case func([]int16) int16:
		return ac.int16out(fn, start, end)
	case func([]int) int:
		return ac.intout(fn, start, end)
	case func([]int32) int32:
		return ac.int32out(fn, start, end)
	case func([]int64) int64:
		return ac.int64out(fn, start, end)
	case func([]uint8) uint8:
		return ac.uint8out(fn, start, end)
	case func([]uint16) uint16:
		return ac.uint16out(fn, start, end)
	case func([]uint) uint:
		return ac.uintout(fn, start, end)
	case func([]uint32) uint32:
		return ac.uint32out(fn, start, end)
	case func([]uint64) uint64:
		return ac.uint64out(fn, start, end)
	default:
		return fmt.Errorf("unexpected ifunc type in an accumulator. ifunc=%v, ivalues=%v", ac.ifunc, ac.ivalues)
	}
}
