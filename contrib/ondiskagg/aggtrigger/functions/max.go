// Package functions
// nolint: dupl // interface/generics can make the code slower
package functions

func MaxFloat32(values []float32) float32 {
	max := values[0]
	for _, val := range values[1:] {
		if val > max {
			max = val
		}
	}
	return max
}

func MaxFloat64(values []float64) float64 {
	max := values[0]
	for _, val := range values[1:] {
		if val > max {
			max = val
		}
	}
	return max
}

func MaxInt8(values []int8) int8 {
	max := values[0]
	for _, val := range values[1:] {
		if val > max {
			max = val
		}
	}
	return max
}

func MaxInt16(values []int16) int16 {
	max := values[0]
	for _, val := range values[1:] {
		if val > max {
			max = val
		}
	}
	return max
}

func MaxInt(values []int) int {
	max := values[0]
	for _, val := range values[1:] {
		if val > max {
			max = val
		}
	}
	return max
}

func MaxInt32(values []int32) int32 {
	max := values[0]
	for _, val := range values[1:] {
		if val > max {
			max = val
		}
	}
	return max
}

func MaxInt64(values []int64) int64 {
	max := values[0]
	for _, val := range values[1:] {
		if val > max {
			max = val
		}
	}
	return max
}

func MaxUint(values []uint) uint {
	max := values[0]
	for _, val := range values[1:] {
		if val > max {
			max = val
		}
	}
	return max
}

func MaxUint8(values []uint8) uint8 {
	max := values[0]
	for _, val := range values[1:] {
		if val > max {
			max = val
		}
	}
	return max
}

func MaxUint16(values []uint16) uint16 {
	max := values[0]
	for _, val := range values[1:] {
		if val > max {
			max = val
		}
	}
	return max
}

func MaxUint32(values []uint32) uint32 {
	max := values[0]
	for _, val := range values[1:] {
		if val > max {
			max = val
		}
	}
	return max
}

func MaxUint64(values []uint64) uint64 {
	max := values[0]
	for _, val := range values[1:] {
		if val > max {
			max = val
		}
	}
	return max
}
