package estimate

import (
	"fmt"
	"github.com/spf13/cobra"
	"math"
)

const (
	usage                 = "estimate"
	short                 = "Estimate required storage space"
	long                  = short
	example               = "marketstore estimate --symbols 5000 --timeframe 1Sec --years 5"

	headerBytes           = 37024
)
var intervalsPerDay = map[string]int64{
	"1D":   1,
	"1Min": 24 * 60,
	"1Sec": 24 * 60 * 60,
	"1ms":  24 * 60 * 60 * 1000,
	"1us":  24 * 60 * 60 * 1000 * 1000,
}

var (
	Cmd = &cobra.Command{
		Use:        usage,
		Short:      short,
		Long:       long,
		Example:    example,
		RunE:       executeStart,
	}
	Num4ByteCols    int64
	Num8ByteCols    int64
	Timeframe       string
	NumSymbols      int64
	NumYears        int64
	HoursPerDay     float64
	DaysPerYear     int64
)

func init() {
	Cmd.Flags().Int64VarP(&Num4ByteCols, "4byteCols", "", 0,
		"Number of 4byte columns")
	Cmd.Flags().Int64VarP(&Num8ByteCols, "8byteCols", "", 5,
		"Number of 8byte columns")
	Cmd.Flags().StringVarP(&Timeframe, "timeframe", "t", "1Min",
		"Timeframe to estimate for")
	Cmd.Flags().Int64VarP(&NumSymbols, "symbols", "s", 1000,
		"Number of symbols stored")
	Cmd.Flags().Int64VarP(&NumYears, "years", "y", 10,
		"Number of years worth of data to store")
	Cmd.Flags().Int64VarP(&DaysPerYear, "days","d", 261,
		"Number of trading days in a year")
	Cmd.Flags().Float64VarP(&HoursPerDay, "hours", "", 6.5,
		"Number of hours per day the market is open")
}

func executeStart(cmd *cobra.Command, args []string) error {
	var (
		recordBytes    int64
		padding        int64
		yearFraction   float64
		fileBytes      float64
		totalBytes     float64
	)

	recordBytes = 8 + (Num4ByteCols * 4) + (Num8ByteCols * 8)  // +8 for the index
	padding = int64(math.Mod(float64(recordBytes), 8))
	recordBytes = recordBytes + padding

	yearFraction = float64(DaysPerYear) * (HoursPerDay / 24.0)
	fileBytes = float64(headerBytes) + (
		float64(recordBytes) * float64(intervalsPerDay[Timeframe]) * yearFraction)

	totalBytes = fileBytes * float64(NumSymbols) * float64(NumYears)

	/*
		Print the results and exit
	 */
	symbolStr := "symbols"
	if NumSymbols == 1 {
		symbolStr = "symbol"
	}
	yearStr := "years"
	if NumYears == 1 {
		yearStr = "year"
	}
	var sizes = []string{"KB", "MB", "GB", "TB", "PB", "EB"}
	for i := range sizes {
		sizeBytes := math.Pow(10, float64((i + 1) * 3))

		if totalBytes < (sizeBytes * 10000) {
			fmt.Printf(
				"Estimated space required for %d %s with %d %s of %s data: %.0f%s\n",
				NumSymbols, symbolStr, NumYears, yearStr, Timeframe, totalBytes / sizeBytes, sizes[i],
			)
			return nil
		}
	}

	// fallback message for ridiculously huge amounts
	fmt.Println("Estimated space required is more than 10,000EB")
	return nil
}
