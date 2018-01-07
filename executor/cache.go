package executor

import (
	"github.com/alpacahq/marketstore/utils/io"
	"time"
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
	TGID         int64              // Current transaction group ID
	writeChannel chan *WriteCommand // Channel for write commands
}

func NewTransactionPipe() *TransactionPipe {
	tgc := new(TransactionPipe)
	// Allocate the write channel with enough depth to allow all conceivable writers concurrent access
	tgc.writeChannel = make(chan *WriteCommand, WriteChannelCommandDepth)
	tgc.NewTGID()
	return tgc
}
func (tgc *TransactionPipe) NewTGID() {
	tgc.TGID = time.Now().UTC().UnixNano()
}
