package wal

import (
	"fmt"

	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

type ShortReadError string

func (msg ShortReadError) Error() string {
	return errReport("%s: Unexpectedly short read", string(msg))
}

func errReport(base string, msg string) string {
	base = io.GetCallerFileContext(2) + ":" + base
	log.Error(base, msg)
	return fmt.Sprintf(base, msg)
}
