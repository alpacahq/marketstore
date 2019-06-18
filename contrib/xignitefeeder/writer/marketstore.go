package writer

import (
	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/utils/io"
)

// MarketStoreWriter is an interface to write data to marketstore.
// this interface is necessary for writing unit tests of XigniteFeeder without actually saving data to the marketstore.
type MarketStoreWriter interface {
	Write(csm io.ColumnSeriesMap) error
}

// MarketStoreWriterImpl writes the column series map data to the local marketstore data.
type MarketStoreWriterImpl struct{}

func (m *MarketStoreWriterImpl) Write(csm io.ColumnSeriesMap) error {
	// no new data to write
	if len(csm) == 0 {
		return nil
	}
	return executor.WriteCSM(csm, false)
}
