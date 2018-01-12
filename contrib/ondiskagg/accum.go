package main

import (
	"github.com/golang/glog"

	"github.com/alpacahq/marketstore/utils/io"
)

func firstFloat32(values []float32) float32 {
	return values[0]
}

func firstFloat64(values []float64) float64 {
	return values[0]
}

func minFloat32(values []float32) float32 {
	min := values[0]
	for _, val := range values[1:] {
		if val < min {
			min = val
		}
	}
	return min
}

func minFloat64(values []float64) float64 {
	min := values[0]
	for _, val := range values[1:] {
		if val < min {
			min = val
		}
	}
	return min
}

func maxFloat32(values []float32) float32 {
	max := values[0]
	for _, val := range values[1:] {
		if val > max {
			max = val
		}
	}
	return max
}

func maxFloat64(values []float64) float64 {
	max := values[0]
	for _, val := range values[1:] {
		if val > max {
			max = val
		}
	}
	return max
}

func lastFloat32(values []float32) float32 {
	return values[len(values)-1]
}

func lastFloat64(values []float64) float64 {
	return values[len(values)-1]
}

func sumFloat32(values []float32) float32 {
	sum := float32(0)
	for _, val := range values {
		sum += val
	}
	return sum
}

func sumFloat64(values []float64) float64 {
	sum := float64(0)
	for _, val := range values {
		sum += val
	}
	return sum
}

func sumInt32(values []int32) int32 {
	// TODO: check overflow
	sum := int32(0)
	for _, val := range values {
		sum += val
	}
	return sum
}

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
			ifunc = firstFloat32
			iout = make([]float32, 0)
		case []float64:
			ifunc = firstFloat64
			iout = make([]float64, 0)
		default:
			glog.Errorf("no compatible function")
			return nil
		}
	case "max":
		inColumn := cs.GetColumn(param.inputName)
		switch inColumn.(type) {
		case []float32:
			ifunc = maxFloat32
			iout = make([]float32, 0)
		case []float64:
			ifunc = maxFloat64
			iout = make([]float64, 0)
		default:
			glog.Errorf("no compatible function")
			return nil
		}
	case "min":
		inColumn := cs.GetColumn(param.inputName)
		switch inColumn.(type) {
		case []float32:
			ifunc = minFloat32
			iout = make([]float32, 0)
		case []float64:
			ifunc = minFloat64
			iout = make([]float64, 0)
		default:
			glog.Errorf("no compatible function")
			return nil
		}
	case "last":
		inColumn := cs.GetColumn(param.inputName)
		switch inColumn.(type) {
		case []float32:
			ifunc = lastFloat32
			iout = make([]float32, 0)
		case []float64:
			ifunc = lastFloat64
			iout = make([]float64, 0)
		default:
			glog.Errorf("no compatible function")
			return nil
		}
	case "sum":
		inColumn := cs.GetColumn(param.inputName)
		switch inColumn.(type) {
		case []float32:
			ifunc = sumFloat32
			iout = make([]float32, 0)
		case []float64:
			ifunc = sumFloat64
			iout = make([]float64, 0)
		case []int32:
			ifunc = sumInt32
			iout = make([]int32, 0)
		default:
			glog.Errorf("no compatible function")
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
	case func([]int32) int32:
		ivalues := ac.ivalues
		out := ac.iout.([]int32)
		ac.iout = append(out, fn(ivalues.([]int32)[start:end]))
	default:
		panic("cannot apply")
	}
}
