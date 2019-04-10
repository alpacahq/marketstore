package feed

import (
	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/utils/io"
)

type CSMWriter interface {
	Write(csm io.ColumnSeriesMap) error
}

type MarketStoreWriter struct{}

func (msw MarketStoreWriter) Write(csm io.ColumnSeriesMap) error {
	return executor.WriteCSM(csm, true)
}
