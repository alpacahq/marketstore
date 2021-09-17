package executor

import (
	"fmt"
	"strconv"

	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

type RecordLengthNotConsistent string

func (msg RecordLengthNotConsistent) Error() string {
	return errReport("%s: Record Length not the same across target data", string(msg))
}

type SingleTargetRequiredForWriter string

func (msg SingleTargetRequiredForWriter) Error() string {
	return errReport("%s: There can be only one target directory for a writer, change your query", string(msg))
}

// WAL Messages
type CacheEntryAlreadyOpenError string

func (msg CacheEntryAlreadyOpenError) Error() string {
	return errReport("%s: Cache entry already open", string(msg))
}

type WrongSizeError string

func (msg WrongSizeError) Error() string {
	return errReport("%s: Wrong record length", string(msg))
}

type NotOpenError string

func (msg NotOpenError) Error() string {
	return errReport("%s: Path Not Open", string(msg))
}

type CacheImmutableError string

func (msg CacheImmutableError) Error() string {
	return errReport("%s: Cache is already written, can not append new data", string(msg))
}

type WALCreateError string

func (msg WALCreateError) Error() string {
	return errReport("%s: Error Creating WAL File", string(msg))
}

type WALTakeOverError string

func (msg WALTakeOverError) Error() string {
	return errReport("%s: Error Taking Over WAL File", string(msg))
}

type WALWriteError string

func (msg WALWriteError) Error() string {
	return errReport("%s: Error Writing to WAL", string(msg))
}

// WALReplayError is used when the WALfile Replay process fails.
// If skipReplay:true, it will attempt to give up the Replay process,
// move the walfile to a temporary file, and continue with other marketstore processing.
type WALReplayError struct {
	msg        string
	skipReplay bool
}

func (e WALReplayError) Error() string {
	return errReport("%s: Error Replaying WAL. skipReplay="+strconv.FormatBool(e.skipReplay), e.msg)
}

func errReport(base string, msg string) string {
	base = io.GetCallerFileContext(2) + ":" + base
	log.Error(base, msg)
	return fmt.Sprintf(base, msg)
}
