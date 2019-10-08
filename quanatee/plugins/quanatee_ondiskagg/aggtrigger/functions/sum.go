package functions

import "math"
import "math/big"

func SumFloat32(values []float32) float32 {
	sum := float32(0)
	for _, val := range values {
		sum += val
	}
	return sum
}

func SumFloat64(values []float64) float64 {
	sum := big.NewFloat(float64(0.0))
	for _, val := range values {
        sum = sum.Add(sum, big.NewFloat(float64(val)))
	}
    
    result, _ := sum.Float64()
    
	return result
}

func SumInt8(values []int8) int8 {
	sum := int8(0)
	for _, val := range values {
		if sum > math.MaxInt8-val {
			panic("integer overflow")
		}
		sum += val
	}
	return sum
}

func SumInt16(values []int16) int16 {
	sum := int16(0)
	for _, val := range values {
		if sum > math.MaxInt16-val {
			panic("integer overflow")
		}
		sum += val
	}
	return sum
}

func SumInt(values []int) int {
	sum := int(0)
	for _, val := range values {
		if sum > math.MaxInt32-val {
			panic("integer overflow")
		}
		sum += val
	}
	return sum
}

func SumInt32(values []int32) int32 {
	sum := int32(0)
	for _, val := range values {
		if sum > math.MaxInt32-val {
			panic("integer overflow")
		}
		sum += val
	}
	return sum
}

func SumInt64(values []int64) int64 {
	sum := int64(0)
	for _, val := range values {
		if sum > math.MaxInt64-val {
			panic("integer overflow")
		}
		sum += val
	}
	return sum
}

func SumUint(values []uint) uint {
	sum := uint(0)
	for _, val := range values {
		sum += val
	}
	return sum
}

func SumUint8(values []uint8) uint8 {
	sum := uint8(0)
	for _, val := range values {
		sum += val
	}
	return sum
}

func SumUint16(values []uint16) uint16 {
	sum := uint16(0)
	for _, val := range values {
		sum += val
	}
	return sum
}

func SumUint32(values []uint32) uint32 {
	sum := uint32(0)
	for _, val := range values {
		sum += val
	}
	return sum
}

func SumUint64(values []uint64) uint64 {
	sum := uint64(0)
	for _, val := range values {
		sum += val
	}
	return sum
}
