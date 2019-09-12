package functions

import "math"
import "math/big"

func AvgFloat32(values []float32) float32 {
	avg := float32(0)
	for _, val := range values {
		avg += val
	}
    
    avg = avg/len(float32(values))
    
	return avg
}

func AvgFloat64(values []float64) float64 {
	avg := big.NewFloat(float64(0.0))
	for _, val := range values {
        avg = avg.Add(avg, big.NewFloat(float64(val)))
	}

    avg = new(big.Float).Quo(avg, big.NewFloat(float64(len(values))))

    result, _ := avg.Float64()
    
	return result
}