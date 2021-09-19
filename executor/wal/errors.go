package wal

import (
	"fmt"
	"strconv"

	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

type ShortReadError string

func (msg ShortReadError) Error() string {
	return errReport("%s: Unexpectedly short read", string(msg))
}

// ReplayError is used when the WALfile Replay process fails.
// If Cont:true, it will give up the Replay process,
// move the walfile to a temporary file, and continue with other marketstore processing.
type ReplayError struct {
	Msg  string
	Cont bool
}

func (e ReplayError) Error() string {
	return errReport("%s: Error Replaying WAL. Cont="+strconv.FormatBool(e.Cont), e.Msg)
}

func errReport(base string, msg string) string {
	base = io.GetCallerFileContext(2) + ":" + base
	log.Error(base, msg)
	return fmt.Sprintf(base, msg)
}
