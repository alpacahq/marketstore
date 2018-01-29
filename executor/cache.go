package executor

import (
	"sync/atomic"
	"time"

	"github.com/alpacahq/marketstore/utils/io"
)

/*
	NOTE: Access to the TransactionPipe structures is single threaded with the exception of the CommandChannel.
	Modification of the cache contents is performed by de-queueing commands from the CommandChannel
	in the single Cache thread.
*/
const WriteChannelCommandDepth = 1000000

type WriteCommand struct {
	RecordType    io.EnumRecordType
	WALKeyPath    string
	Offset, Index int64
	Data          []byte
}

/*
	TransactionPipe stores the contents of the current pending Transaction Group and writes it to WAL when flush() is called
*/
type TransactionPipe struct {
	tgID         int64              // Current transaction group ID
	writeChannel chan *WriteCommand // Channel for write commands
	flushChannel chan interface{}   // Channel for flush request
}

func NewTransactionPipe() *TransactionPipe {
	tgc := new(TransactionPipe)
	// Allocate the write channel with enough depth to allow all conceivable writers concurrent access
	tgc.writeChannel = make(chan *WriteCommand, WriteChannelCommandDepth)
	tgc.flushChannel = make(chan interface{}, WriteChannelCommandDepth)
	tgc.NewTGID()
	return tgc
}

func (tgc *TransactionPipe) NewTGID() int64 {
	// TODO: use atomic.Add() to guarantee monotonically-increasing behavior
	return atomic.SwapInt64(&tgc.tgID, time.Now().UTC().UnixNano())
}

func (tgc *TransactionPipe) TGID() int64 {
	return atomic.LoadInt64(&tgc.tgID)
}
