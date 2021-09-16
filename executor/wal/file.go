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
	buf, _, err := Read(filePtr, -1, buffer[:])
	return FileStatusEnum(buf[0]), ReplayStateEnum(buf[1]), io.ToInt64(buf[2:]), err
}

func Read(fp *os.File, targetOffset int64, buffer []byte) (result []byte, newOffset int64, err error) {
	/*
		Read from the WAL file
			targetOffset: -1 will read from current position
	*/
	offset, err := fp.Seek(0, io2.SeekCurrent)
	if err != nil {
		log.Fatal(io.GetCallerFileContext(0) + ": Unable to seek in WALFile")
	}
	if targetOffset != -1 {
		if offset != targetOffset {
			fp.Seek(targetOffset, io2.SeekStart)
		}
	}
	numToRead := len(buffer)
	n, err := fp.Read(buffer)
	if n != numToRead {
		msg := fmt.Sprintf("Read: Expected: %d Got: %d", numToRead, n)
		err = ShortReadError(msg)
	} else if err != nil {
		log.Fatal(io.GetCallerFileContext(0) + ": Unable to read WALFile")
	}
	return buffer, offset + int64(n), err
}
