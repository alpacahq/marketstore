package functions

func MinFloat32(values []float32) float32 {
	min := values[0]
	for _, val := range values[1:] {
		if val < min {
			min = val
		}
	}
	return min
}

func MinFloat64(values []float64) float64 {
	min := values[0]
	for _, val := range values[1:] {
		if val < min {
			min = val
		}
	}
	return min
}

func MinInt8(values []int8) int8 {
	min := values[0]
	for _, val := range values[1:] {
		if val < min {
			min = val
		}
	}
	return min
}

func MinInt16(values []int16) int16 {
	min := values[0]
	for _, val := range values[1:] {
		if val < min {
			min = val
		}
	}
	return min
}

func MinInt(values []int) int {
	min := values[0]
	for _, val := range values[1:] {
		if val < min {
			min = val
		}
	}
	return min
}

func MinInt32(values []int32) int32 {
	min := values[0]
	for _, val := range values[1:] {
		if val < min {
			min = val
		}
	}
	return min
}

func MinInt64(values []int64) int64 {
	min := values[0]
	for _, val := range values[1:] {
		if val < min {
			min = val
		}
	}
	return min
}

func MinUint(values []uint) uint {
	min := values[0]
	for _, val := range values[1:] {
		if val < min {
			min = val
		}
	}
	return min
}

func MinUint8(values []uint8) uint8 {
	min := values[0]
	for _, val := range values[1:] {
		if val < min {
			min = val
		}
	}
	return min
}

func MinUint16(values []uint16) uint16 {
	min := values[0]
	for _, val := range values[1:] {
		if val < min {
			min = val
		}
	}
	return min
}

func MinUint32(values []uint32) uint32 {
	min := values[0]
	for _, val := range values[1:] {
		if val < min {
			min = val
		}
	}
	return min
}

func MinUint64(values []uint64) uint64 {
	min := values[0]
	for _, val := range values[1:] {
		if val < min {
			min = val
		}
	}
	return min
}
