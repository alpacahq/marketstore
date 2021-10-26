package wal

import (
	"fmt"
	io2 "io"
	"os"

	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

type FileStatusEnum int8

const (
	Invalid FileStatusEnum = iota
	OPEN
	CLOSED
)

type ReplayStateEnum int8

const (
	Invalid2 ReplayStateEnum = iota
	NOTREPLAYED
	REPLAYED
	REPLAYINPROCESS
)

func ReadStatus(filePtr *os.File) (fileStatus FileStatusEnum, replayStatus ReplayStateEnum, OwningInstanceID int64, err error) {
	var buffer [10]byte
	buf, _, err := Read(filePtr, buffer[:])
	return FileStatusEnum(buf[0]), ReplayStateEnum(buf[1]), io.ToInt64(buf[2:]), err
}

// Read reads the WAL file from current position.
// At end of file, Read returns io.EOF error.
func Read(fp *os.File, buffer []byte) (result []byte, newOffset int64, err error) {
	offset, err := fp.Seek(0, io2.SeekCurrent)
	if err != nil {
		log.Error(io.GetCallerFileContext(0) + ": Unable to seek in WALFile")
		return nil, 0, fmt.Errorf("unable to seek in WALFile from curpos:%w", err)
	}

	numToRead := len(buffer)
	n, err := fp.Read(buffer)
	if err == io2.EOF {
		return result, newOffset, io2.EOF
	}
	if n != numToRead {
		msg := fmt.Sprintf("Read: Expected: %d Got: %d", numToRead, n)
		err = ShortReadError(msg)
	} else if err != nil {
		log.Error(io.GetCallerFileContext(0) + ": Unable to read WALFile")
		err = fmt.Errorf("unable to read WALFile:%w", err)
	}

	return buffer, offset + int64(n), err
}
