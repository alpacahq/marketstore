package executor

import (
	"errors"

	"github.com/alpacahq/marketstore/v4/utils/io"
)

type ErrorWriter struct{}

func (w *ErrorWriter) WriteCSM(csm io.ColumnSeriesMap, isVariableLength bool) error {
	return errors.New("write is not allowed on replica")
}
