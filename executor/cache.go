package executor

import (
	"sync/atomic"
	"time"

	"github.com/alpacahq/marketstore/v4/executor/wal"
)

/*
	NOTE: Access to the TransactionPipe structures is single threaded with the exception of the CommandChannel.
	Modification of the cache contents is performed by de-queueing commands from the CommandChannel
	in the single Cache thread.
*/
const WriteChannelCommandDepth = 1000000

// TransactionPipe stores the contents of the current pending Transaction Group
// and writes it to WAL when flush() is called
type TransactionPipe struct {
	tgID         int64                  // Current transaction group ID
	writeChannel chan *wal.WriteCommand // Channel for write commands
	flushChannel chan chan struct{}     // Channel for flush request
}

// NewTransactionPipe creates a new transaction pipe that channels all
// of the write transactions to the WAL and primary writers
func NewTransactionPipe() *TransactionPipe {
	tgc := new(TransactionPipe)
	// Allocate the write channel with enough depth to allow all conceivable writers concurrent access
	tgc.writeChannel = make(chan *wal.WriteCommand, WriteChannelCommandDepth)
	tgc.flushChannel = make(chan chan struct{}, WriteChannelCommandDepth)
	tgc.newTGID()
	return tgc
}

// NewTGID monotonically increases the transaction group ID using
// the current unix epoch nanosecond timestamp
func (tgc *TransactionPipe) newTGID() int64 {
	return atomic.AddInt64(&tgc.tgID, time.Now().UTC().UnixNano()-tgc.tgID)
}

// IncrementTGID increments the transaction group ID and returns the new value
func (tgc *TransactionPipe) IncrementTGID() int64 {
	return atomic.AddInt64(&tgc.tgID, 1)
}

// TGID returns the latest transaction group ID
func (tgc *TransactionPipe) TGID() int64 {
	return atomic.LoadInt64(&tgc.tgID)
}
