package writer

import (
	"fmt"
	"time"

	"github.com/pkg/errors"

	v1 "github.com/alpacahq/marketstore/v4/contrib/alpacabkfeeder/api/v1"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

// BarWriter is an interface to write chart data to the marketstore.
type BarWriter interface {
	Write(symbol string, bars []v1.Bar) error
}

// BarWriterImpl is an implementation of the BarWriter interface.
type BarWriterImpl struct {
	MarketStoreWriter MarketStoreWriter
	Timeframe         string
	// BarWriterImpl writes data with the timezone
	Timezone *time.Location
}

// Write converts the Response of the ListBars API to a ColumnSeriesMap and write it to the local marketstore server.
func (b BarWriterImpl) Write(symbol string, bars []v1.Bar) error {
	// convert Bar Data to CSM (ColumnSeriesMap)
	csm := b.convertToCSM(symbol, bars)

	// write CSM to marketstore
	if err := b.MarketStoreWriter.Write(csm); err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to write the bar data to marketstore. %v", csm))
	}

	log.Debug(fmt.Sprintf("Data has been saved to marketstore successfully. symbol=%s, len(bars)=%d",
		symbol, len(bars)))
	return nil
}

func (b *BarWriterImpl) convertToCSM(symbol string, bars []v1.Bar) io.ColumnSeriesMap {
	epochs := make([]int64, len(bars))
	opens := make([]float32, len(bars))
	closes := make([]float32, len(bars))
	highs := make([]float32, len(bars))
	lows := make([]float32, len(bars))
	volumes := make([]int32, len(bars))
	csm := io.NewColumnSeriesMap()

	for i := range bars {
		// // skip the symbol which timestamp is empty string and cannot be parsed,
		// // which means the symbols have never been executed
		// if time.Time(bar.Timestamp) == (time.Time{}) {
		//	continue
		// }

		// // When Volume is 0, alpaca getBarsAPI may return data with open:0, close:0, high:0, low:0.
		// // we don't write the zero data to marketstore.
		// // For Index Symbol data, Volume is always 0.
		// if !isIndexSymbol && bar.Volume == 0 {
		//	continue
		// }

		// // UTCOffset is used to adjust the time to UTC based on the config.
		// UTCOffset := time.Duration(-1*bar.UTCOffSet) * time.Hour

		// Start time of each bar is used for "epoch"
		// to align with the 1-day chart backfill. ("00:00:00"(starting time of a day) is used for epoch)
		epochs[i] = bars[i].Time
		opens[i] = bars[i].Open
		closes[i] = bars[i].Close
		highs[i] = bars[i].High
		lows[i] = bars[i].Low
		volumes[i] = bars[i].Volume
	}

	// to avoid that empty array is added to csm when all data are Volume=0 and there is no data to write
	if len(epochs) == 0 {
		// no data to write.
		return csm
	}

	cs := b.newColumnSeries(epochs, opens, closes, highs, lows, volumes)
	tbk := io.NewTimeBucketKey(symbol + "/" + b.Timeframe + "/OHLCV")
	csm.AddColumnSeries(*tbk, cs)
	return csm
}

func (b BarWriterImpl) newColumnSeries(epochs []int64, opens, closes, highs, lows []float32, volumes []int32,
) *io.ColumnSeries {
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", epochs)
	cs.AddColumn("Open", opens)
	cs.AddColumn("Close", closes)
	cs.AddColumn("High", highs)
	cs.AddColumn("Low", lows)
	cs.AddColumn("Volume", volumes)

	return cs
}
