package wal

import (
	"fmt"
	"strconv"

	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

type ShortReadError string

func (msg ShortReadError) Error() string {
	return errReport("Unexpectedly short read:%s", string(msg))
}

// ReplayError is used when the WALfile Replay process fails.
// If Cont:true, it will give up the Replay process,
// move the walfile to a temporary file, and continue with other marketstore processing.
type ReplayError struct {
	Msg  string
	Cont bool
}

func (e ReplayError) Error() string {
	return errReport("Error Replaying WAL. Cont="+strconv.FormatBool(e.Cont)+":%s", e.Msg)
}

func errReport(base string, msg string) string {
	base = io.GetCallerFileContext(2) + ":" + base
	log.Warn(base, msg)
	return fmt.Sprintf(base, msg)
}
