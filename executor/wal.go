package executor

import (
	"bytes"
	"crypto/md5"
	"errors"
	"fmt"
	goio "io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/alpacahq/marketstore/v4/executor/buffile"

	"github.com/alpacahq/marketstore/v4/executor/wal"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

/*
	NOTE: Access to the WAL structures for a single WAL File is single threaded
	of the CommandChannel.
*/

type WALFileType struct {
	// These three fields plus the MID form the WAL Header, written at the beginning of the WAL File
	FileStatus       wal.FileStatusEnum
	ReplayState      wal.ReplayStateEnum
	OwningInstanceID int64
	// End of WAL Header
	rootDir           string
	FilePtr           *os.File          // Active file pointer to FileName
	lastCommittedTGID int64             // TGID to be checkpointed
	ReplicationSender ReplicationSender // send messages to replica servers
	walBypass         bool
	shutdownPending   *bool
	walWaitGroup      *sync.WaitGroup
	tpd               *TriggerPluginDispatcher
	txnPipe           *TransactionPipe
}

type ReplicationSender interface {
	Send(transactionGroup []byte)
}

type TransactionGroup struct {
	// A "locally unique" transaction group identifier, can be a clock value
	ID int64
	// The contents of the WTSets
	WTGroup []wal.WTSet
	// MD5 checksum of the TG contents prior to the checksum
	Checksum [16]byte
}

func NewWALFile(rootDir string, owningInstanceID int64, rs ReplicationSender,
	walBypass bool, walWaitGroup *sync.WaitGroup, tpd *TriggerPluginDispatcher,
	txnPipe *TransactionPipe,
) (wf *WALFileType, err error) {
	shutdownPending := false
	wf = &WALFileType{
		lastCommittedTGID: 0,
		OwningInstanceID:  owningInstanceID,
		rootDir:           rootDir,
		ReplicationSender: rs,
		walBypass:         walBypass,
		shutdownPending:   &shutdownPending,
		walWaitGroup:      walWaitGroup,
		tpd:               tpd,
		txnPipe:           txnPipe,
	}

	if err = wf.createFile(rootDir); err != nil {
		log.Error("%v: Can not create new WALFile - Error: %v", io.GetCallerFileContext(0), err)
		return nil, fmt.Errorf("can not create new WALFile: %w", err)
	}
	if err := wf.WriteStatus(wal.OPEN, wal.NOTREPLAYED); err != nil {
		return nil, fmt.Errorf("failed to write NOT_REPLAYED status to wal: %w", err)
	}

	return wf, nil
}

// TakeOverWALFile opens an existing wal file and returns WALFileType for it.
func TakeOverWALFile(filePath string) (wf *WALFileType, err error) {
	wf = new(WALFileType)
	wf.lastCommittedTGID = 0
	// filePath := filepath.Join(rootDir, fileName)

	err = wf.open(filePath)
	if err != nil {
		return nil, WALTakeOverError("TakeOverFile" + err.Error())
	}

	fileStatus, replayState, owningInstanceID, err := readStatus(wf.FilePtr)
	if err != nil {
		return nil, fmt.Errorf("failed to read walfile(%s) status: %w", wf.FilePtr.Name(), err)
	}
	if wf.callerOwnsFile(owningInstanceID) {
		return nil, WALTakeOverError("TakeOver: File file is owned by calling process")
	}

	wf.OwningInstanceID = owningInstanceID

	// We call this to take over the file by writing our PID to it
	if err := wf.WriteStatus(fileStatus, replayState); err != nil {
		return nil, fmt.Errorf("failed to write replayState:%v to walfile: %w", replayState, err)
	}

	return wf, nil
}

// createFile creates a WAL file in "{rootDir}/WALFile.{currentEpochNanoSecondInUTC}.walfile" format.
// it doesn't return an error even if another WAL file has already been created.
func (wf *WALFileType) createFile(rootDir string) error {
	now := time.Now().UTC()
	nowNano := now.UnixNano()
	filePath := filepath.Join(rootDir, "WALFile")
	filePath = fmt.Sprintf("%s.%d.walfile", filePath, nowNano)
	// Try to open the file for writing, creating it in the process if it doesn't exist

	if err := wf.open(filePath); err != nil {
		return WALCreateError("CreateFile" + err.Error())
	}
	return nil
}

func (wf *WALFileType) open(filePath string) error {
	var err error
	wf.FilePtr, err = os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return errors.New(io.GetCallerFileContext(0) + err.Error())
	}
	return nil
}

func (wf *WALFileType) close(replayStatus wal.ReplayStateEnum) error {
	if err := wf.WriteStatus(wal.CLOSED, replayStatus); err != nil {
		return fmt.Errorf("failed to write CLOSED status to wal: %w", err)
	}
	return wf.FilePtr.Close()
}

func (wf *WALFileType) Delete(callersInstanceID int64) (err error) {
	err = wf.syncStatusRead()
	if err != nil {
		return fmt.Errorf("cannot delete wal because failed to read wal sync status. callersInstanceID=%d:%w",
			callersInstanceID, err)
	}

	if !wf.IsOpen() {
		log.Warn(io.GetCallerFileContext(0) + ": Can not delete open WALFile")
		return errors.New("WAL File is open")
	}
	if wf.isActive(callersInstanceID) {
		log.Warn(io.GetCallerFileContext(0) + ": Can not delete active WALFile")
		return errors.New("WAL File is active")
	}

	needsReplay, err := wf.NeedsReplay()
	if err != nil {
		return fmt.Errorf("read if wal needs replay:%w", err)
	}
	if needsReplay {
		log.Warn(io.GetCallerFileContext(0) + ": WALFile needs replay, can not delete")
		return errors.New("WAL File needs replay, can not delete")
	}

	if err2 := wf.close(wal.REPLAYED); err2 != nil {
		return fmt.Errorf("failed to close walfile: %w", err2)
	}
	if err = os.Remove(wf.FilePtr.Name()); err != nil {
		log.Error(io.GetCallerFileContext(0) + ": Can not remove WALFile")
		return fmt.Errorf("cannot remove WALFile %s: %w", wf.FilePtr.Name(), err)
	}

	return nil
}

func (wf *WALFileType) WriteCommand(rt io.EnumRecordType, tbiAbsPath string, varRecLen int, offset, index int64,
	data []byte, ds []io.DataShape,
) *wal.WriteCommand {
	return &wal.WriteCommand{
		RecordType: rt,
		WALKeyPath: FullPathToWALKey(wf.rootDir, tbiAbsPath),
		VarRecLen:  varRecLen,
		Offset:     offset,
		Index:      index,
		Data:       data,
		DataShapes: ds,
	}
}

func FullPathToWALKey(rootPath, fullPath string) (keyPath string) {
	/*
		NOTE: This key includes the year filename at the end of the metadata key
	*/
	// Chops rootPath from fullPath to produce a WAL Key
	keyPath, _ = filepath.Rel(rootPath, fullPath)
	return keyPath
}

func walKeyToFullPath(rootPath, keyPath string) (fullPath string) {
	// Adds rootPath to keyPath to produce a fullPath
	return filepath.Join(rootPath, keyPath)
}

func (wf *WALFileType) QueueWriteCommand(wc *wal.WriteCommand) {
	wf.txnPipe.writeChannel <- wc
}

// FlushToWAL A.k.a. Commit transaction.
//	Here we flush the contents of the write cache to:
//	- Primary storage via the OS write cache - data is visible to readers
//	- WAL file with synchronization to physical storage - in case we need to recover from a crash
func (wf *WALFileType) FlushToWAL() (err error) {
	// walBypass = true // Bypass all writing to the WAL File, leaving the writes to the primary

	// Count of WT Sets in this TG as of now
	if wf.txnPipe == nil {
		return nil
	}

	WTCount := len(wf.txnPipe.writeChannel)
	if WTCount == 0 {
		// refresh TGID so requester can confirm it went through even if nothing is written
		wf.txnPipe.IncrementTGID()
		return nil
	}

	if !wf.walBypass {
		canWrite, err := wf.CanWrite(wf.OwningInstanceID)
		// TODO: error handling to move walFile to a temporary file and create a new one when walFile is corrupted.
		if err != nil || !canWrite {
			panic("Failed attempt to write to WAL")
		}

		// WAL Transaction Preparing Message
		if err := wf.WriteTransactionInfo(wf.txnPipe.TGID(), WAL, PREPARING); err != nil {
			return fmt.Errorf("failed to write TRANSACTION PREPARING message to wal: %w", err)
		}
	}

	// Serialize all data to be written except for the size of this buffer
	writeCommands := make([]*wal.WriteCommand, WTCount)
	for i := 0; i < WTCount; i++ {
		writeCommands[i] = <-wf.txnPipe.writeChannel
	}

	return wf.FlushCommandsToWAL(writeCommands)
}

func (wf *WALFileType) FlushCommandsToWAL(writeCommands []*wal.WriteCommand) (err error) {
	defer wf.tpd.DispatchRecords()

	fileRecordTypes := map[string]io.EnumRecordType{}
	varRecLens := map[string]int{}
	for i := 0; i < len(writeCommands); i++ {
		keyPath := writeCommands[i].WALKeyPath
		if _, ok := fileRecordTypes[keyPath]; !ok {
			fileRecordTypes[keyPath] = writeCommands[i].RecordType
		}
		if _, ok := varRecLens[keyPath]; !ok {
			varRecLens[keyPath] = writeCommands[i].VarRecLen
		}
	}

	tgSerialized, writesPerFile := serializeTG(wf.txnPipe.tgID, writeCommands)

	if !wf.walBypass {
		// Serialize the size of the buffer into another buffer
		tgLenSerialized, _ := io.Serialize(nil, int64(len(tgSerialized)))

		// Calculate the MD5 checksum, including the value of TGLen
		hash := md5.New()
		hash.Write(tgLenSerialized)
		hash.Write(tgSerialized)

		// Write the Message ID to identify TG Data
		if _, err := wf.FilePtr.Write(wf.initMessage(TGDATA)); err != nil {
			return fmt.Errorf("failed to write Message ID to walfile: %w", err)
		}
		// Write the TG Data and the checksum and Sync()
		if _, err := wf.FilePtr.Write(tgLenSerialized); err != nil {
			return fmt.Errorf("failed to write TransactionGroup length to walfile: %w", err)
		}
		if _, err := wf.FilePtr.Write(tgSerialized); err != nil {
			return fmt.Errorf("failed to write TransactionGroup data to walfile: %w", err)
		}
		cksum := hash.Sum(nil)
		if _, err := wf.FilePtr.Write(cksum); err != nil { // Checksum
			return fmt.Errorf("failed to write TransactionGroup checksum to walfile: %w", err)
		}
		// WAL Transaction Commit Complete Message
		TGID := wf.txnPipe.TGID()
		if err := wf.WriteTransactionInfo(TGID, WAL, COMMITCOMPLETE); err != nil {
			return fmt.Errorf("failed to write COMMITCOMPLETE status to walfile: %w", err)
		}
		wf.lastCommittedTGID = TGID
		wf.txnPipe.IncrementTGID()

		if err := wf.FilePtr.Sync(); err != nil { // Flush the OS buffer
			return fmt.Errorf("failed to flush wal data: %w", err)
		}

		// send transaction to replicas
		if wf.ReplicationSender != nil {
			wf.ReplicationSender.Send(tgSerialized)
		}
	}

	/*
		Write the buffers to primary files (should happen after WAL writes)
	*/
	for keyPath, writes := range writesPerFile {
		recordType := fileRecordTypes[keyPath]
		varRecLen := varRecLens[keyPath]
		if err := wf.writePrimary(keyPath, writes, recordType, varRecLen); err != nil {
			// TODO: what should we do if the write commit partially failed?
			log.Error(fmt.Sprintf("failed to write data to file %s: %s", keyPath, err.Error()))
		}
		for i, buffer := range writes {
			wf.tpd.AppendRecord(keyPath, buffer.IndexAndPayload())
			writes[i] = nil // for GC
		}
		writesPerFile[keyPath] = nil // for GC
	}
	return nil
}

func serializeTG(tgID int64, commands []*wal.WriteCommand,
) (tgSerialized2 []byte, writesPerFile map[string][]wal.OffsetIndexBuffer) {
	WTCount := len(commands)

	// Serialize all data to be written except for the size of this buffer
	var tgSerialized []byte
	tgSerialized, _ = io.Serialize(tgSerialized, tgID)
	tgSerialized, _ = io.Serialize(tgSerialized, int64(WTCount))
	writesPerFile = map[string][]wal.OffsetIndexBuffer{}
	/*
		This loop serializes write transactions from the channel for writing to disk
	*/
	for i := 0; i < WTCount; i++ {
		tgSerialized, _ = io.Serialize(tgSerialized, int8(commands[i].RecordType))
		tgSerialized, _ = io.Serialize(tgSerialized, int16(len(commands[i].WALKeyPath)))
		tgSerialized, _ = io.Serialize(tgSerialized, commands[i].WALKeyPath)
		tgSerialized, _ = io.Serialize(tgSerialized, int32(len(commands[i].Data)))
		tgSerialized, _ = io.Serialize(tgSerialized, int32(commands[i].VarRecLen))
		oStart := len(tgSerialized)
		bufferSize := 8 + 8 + len(commands[i].Data)
		tgSerialized, _ = io.Serialize(tgSerialized, commands[i].Offset)
		tgSerialized, _ = io.Serialize(tgSerialized, commands[i].Index)
		tgSerialized = append(tgSerialized, commands[i].Data...)
		// include DataShape information in TG because it's necessary for creating a new bucket from WAL
		dsvBytes, err := io.DSVToBytes(commands[i].DataShapes)
		if err == nil {
			tgSerialized = append(tgSerialized, dsvBytes...)
		}

		keyPath := commands[i].WALKeyPath
		// Store the data in a buffer for primary storage writes after WAL writes are done
		writesPerFile[keyPath] = append(writesPerFile[keyPath],
			tgSerialized[oStart:oStart+bufferSize])
	}

	return tgSerialized, writesPerFile
}

func writeFixedBuffer(writes []wal.OffsetIndexBuffer, fullPath string) error {
	const batchThreshold = 100

	type WriteAtCloser interface {
		goio.WriterAt
		goio.Closer
	}
	var (
		fp  WriteAtCloser
		err error
	)
	if len(writes) >= batchThreshold {
		fp, err = buffile.New(fullPath)
	} else {
		fp, err = os.OpenFile(fullPath, os.O_RDWR, 0o700)
	}
	if err != nil {
		// this is critical, in fact, since tx has been committed
		log.Error("cannot open file %s for write transaction commit: %v", fullPath, err)
		return err
	}
	defer func() {
		if err2 := fp.Close(); err2 != nil {
			log.Error("failed to close walfile", zap.Error(err2))
		}
	}()

	for _, buffer := range writes {
		if err = WriteBufferToFile(fp, buffer); err != nil {
			log.Error("failed to write committed data: %v", err)
			return err
		}
	}
	return nil
}

func writeVariableLengthBuffer(writes []wal.OffsetIndexBuffer, fullPath string, varRecLen int) error {
	fp, err := os.OpenFile(fullPath, os.O_RDWR, 0o700)
	if err != nil {
		// this is critical, in fact, since tx has been committed
		log.Error("cannot open file %s for write transaction commit: %v", fullPath, err)
		return err
	}
	defer func() {
		if err2 := fp.Close(); err2 != nil {
			log.Error("failed to close walfile", zap.Error(err2))
		}
	}()

	for _, buffer := range writes {
		if err = WriteBufferToFileIndirect(fp, buffer, varRecLen); err != nil {
			log.Error("failed to write committed data: %v", err)
			return err
		}
	}
	return nil
}

func (wf *WALFileType) writePrimary(keyPath string, writes []wal.OffsetIndexBuffer, recordType io.EnumRecordType,
	varRecLen int,
) (err error) {
	rootDir := filepath.Dir(wf.FilePtr.Name())
	fullPath := walKeyToFullPath(rootDir, keyPath)
	switch recordType {
	case io.FIXED:
		if err = writeFixedBuffer(writes, fullPath); err != nil {
			return err
		}
	case io.VARIABLE:
		if err = writeVariableLengthBuffer(writes, fullPath, varRecLen); err != nil {
			return err
		}
	default:
		return errors.New("unknown record type(io.NOTYPE) found")
	}

	return nil
}

// CreateCheckpoint flushes all primary dirty pages to disk, and
// so closes out the previous WAL state to end.  Note, this is
// not goroutine-safe with FlushToWAL and caller should make sure
// it is streamlined.
func (wf *WALFileType) CreateCheckpoint() error {
	if wf.lastCommittedTGID == 0 {
		return nil
	}
	if wf.walBypass {
		io.Syncfs()
		wf.lastCommittedTGID = 0
		return nil
	}

	// WAL Transaction Preparing Message
	// Get the latest TGID and write a prepare message
	TGID := wf.lastCommittedTGID
	if err := wf.WriteTransactionInfo(TGID, CHECKPOINT, PREPARING); err != nil {
		return fmt.Errorf("write PREPARING transaction info: %w", err)
	}
	// Sync the filesystem, after this point the filesystem cache data is committed to disk
	io.Syncfs()
	if err := wf.WriteTransactionInfo(TGID, CHECKPOINT, COMMITCOMPLETE); err != nil {
		return fmt.Errorf("write COMMITCOMPLETE transaction info: %w", err)
	}

	wf.lastCommittedTGID = 0
	return nil
}

type TGIDlist []int64

func (tgl TGIDlist) Len() int           { return len(tgl) }
func (tgl TGIDlist) Less(i, j int) bool { return tgl[i] < tgl[j] }
func (tgl TGIDlist) Swap(i, j int)      { tgl[i], tgl[j] = tgl[j], tgl[i] }

func (wf *WALFileType) WriteStatus(fileStatus wal.FileStatusEnum, replayState wal.ReplayStateEnum) error {
	wf.FileStatus = fileStatus
	wf.ReplayState = replayState
	// This process now owns this file
	buffer := wf.initMessage(STATUS)
	buffer, _ = io.Serialize(buffer, int8(wf.FileStatus))
	buffer, _ = io.Serialize(buffer, int8(wf.ReplayState))
	buffer, _ = io.Serialize(buffer, wf.OwningInstanceID)
	if _, err := wf.FilePtr.Seek(0, goio.SeekStart); err != nil {
		return err
	}
	if _, err := wf.FilePtr.Write(buffer); err != nil {
		return err
	}
	if err := wf.FilePtr.Sync(); err != nil {
		return err
	}
	if _, err := wf.FilePtr.Seek(0, goio.SeekEnd); err != nil {
		return err
	}
	return nil
}

func (wf *WALFileType) WriteTransactionInfo(tid int64, did DestEnum, txnStatus TxnStatusEnum) error {
	buffer := wf.initMessage(TXNINFO)
	buffer, _ = io.Serialize(buffer, tid)
	buffer, _ = io.Serialize(buffer, did)
	buffer, _ = io.Serialize(buffer, txnStatus)
	_, err := wf.FilePtr.Write(buffer)
	if err != nil {
	}
	return nil
}

func (wf *WALFileType) readTransactionInfo() (tgid int64, destination DestEnum, txnStatus TxnStatusEnum, err error) {
	var buffer [10]byte
	buf, _, err := wal.Read(wf.FilePtr, buffer[:])
	if err != nil {
		return 0, 0, 0, wal.ShortReadError("WALFileType.readTransactionInfo")
	}
	tgid, destination, txnStatus = io.ToInt64(buf), DestEnum(buf[8]), TxnStatusEnum(buf[9])
	switch destination {
	case CHECKPOINT, WAL:
		break
	default:
		return 0, 0, 0, fmt.Errorf("WALFileType.readTransactionInfo Invalid destination ID: %d", destination)
	}
	switch txnStatus {
	case PREPARING, COMMITINTENDED, COMMITCOMPLETE:
		break
	default:
		return 0, 0, 0, fmt.Errorf("WALFileType.readTransactionInfo Invalid Txn Status: %d", txnStatus)
	}

	return tgid, destination, txnStatus, nil
}

func (wf *WALFileType) initMessage(mid MIDEnum) []byte {
	buffer, _ := io.Serialize([]byte{}, mid)
	return buffer
}

func validateCheckSum(tgLenSerialized, tgSerialized, checkBuf []byte) error {
	// compute the checksum
	hash := md5.New()
	hash.Write(tgLenSerialized)
	hash.Write(tgSerialized)
	cksum := hash.Sum(nil)

	if !bytes.Equal(cksum, checkBuf) {
		return errors.New(io.GetCallerFileContext(0) + fmt.Sprintf(":Checksum was: %v should be: %v", cksum, checkBuf))
	}

	return nil
}

func ParseTGData(tgSerialized []byte, rootPath string) (tgID int64, wtSets []wal.WTSet) {
	// see /docs/design/durable_writes_design.txt for the details of the transaction group format
	const (
		tgIDLenBytes      = 8
		wtCountLenBytes   = 8
		recordLenLenBytes = 1
		fpLenLenBytes     = 2
		dataLenLenBytes   = 4
		varRecLenLenBytes = 4
		// (offset (8bytes), index(8bytes), buffer(dataLen-bytes))
		offsetLenBytes = 8
		indexLenBytes  = 8
	)
	tgID = io.ToInt64(tgSerialized[0:tgIDLenBytes])
	WTCount := io.ToInt64(tgSerialized[tgIDLenBytes : tgIDLenBytes+wtCountLenBytes])

	cursor := tgIDLenBytes + wtCountLenBytes
	wtSets = make([]wal.WTSet, WTCount)

	for i := 0; i < int(WTCount); i++ {
		RecordType := io.ToInt8(tgSerialized[cursor : cursor+recordLenLenBytes])
		cursor += recordLenLenBytes
		FPLen := int(io.ToInt16(tgSerialized[cursor : cursor+fpLenLenBytes]))
		cursor += fpLenLenBytes
		WALKeyPath := bytes.NewBuffer(tgSerialized[cursor : cursor+FPLen]).String()
		cursor += FPLen
		dataLen := int(io.ToInt32(tgSerialized[cursor : cursor+dataLenLenBytes]))
		cursor += dataLenLenBytes
		varRecLen := int(io.ToInt32(tgSerialized[cursor : cursor+varRecLenLenBytes]))
		cursor += varRecLenLenBytes
		fullPath := walKeyToFullPath(rootPath, WALKeyPath)
		data := tgSerialized[cursor : cursor+offsetLenBytes+indexLenBytes+dataLen]
		cursor += offsetLenBytes + indexLenBytes + dataLen
		dataShapes, l := io.DSVFromBytes(tgSerialized[cursor:])
		cursor += l

		wtSets[i] = wal.NewWTSet(
			io.EnumRecordType(RecordType),
			fullPath,
			dataLen,
			varRecLen,
			data,
			dataShapes,
		)
	}

	return tgID, wtSets
}

func (wf *WALFileType) IsOpen() bool {
	if _, err := wf.FilePtr.Stat(); err != nil {
		log.Info(io.GetCallerFileContext(0) + ": File stat failed, file probably deleted: " + err.Error())
		return false
	}
	if wf.FileStatus != wal.OPEN {
		log.Info(io.GetCallerFileContext(0) + ": File not opened")
		return false
	}
	return true
}

func (wf *WALFileType) syncStatusRead() error {
	_, err := wf.FilePtr.Stat()
	if err != nil {
		log.Error(io.GetCallerFileContext(0) + ": File stat failed")
		return fmt.Errorf("failed to read walFile status. trace=%s: %w", io.GetCallerFileContext(0), err)
	}
	wf.FileStatus, wf.ReplayState, wf.OwningInstanceID, err = readStatus(wf.FilePtr)
	if err != nil {
		return fmt.Errorf("failed to read Status of %s: %w", wf.FilePtr.Name(), err)
	}

	return nil
}

func readStatus(filePtr *os.File,
) (fileStatus wal.FileStatusEnum, replayStatus wal.ReplayStateEnum, owningInstanceID int64, err error) {
	// Read from beginning of file +1 to cont over the MID
	if _, err2 := filePtr.Seek(1, goio.SeekStart); err2 != nil {
		log.Error("failed to seek file pointer", zap.Int("offset", 1),
			zap.String("whence", "start"), zap.Error(err2),
		)
	}
	fileStatus, replayStatus, owningInstanceID, err = wal.ReadStatus(filePtr)
	if err != nil {
		log.Error(io.GetCallerFileContext(0) + ": Unable to ReadStatus()")
		return 0, 0, 0, fmt.Errorf("unable to read status: %w", err)
	}
	//	wf.FileStatus, wf.ReplayState, wf.OwningInstanceID = fileStatus, replayStatus, owningInstanceID
	// Reset the file pointer to the end of the file
	if _, err := filePtr.Seek(0, goio.SeekEnd); err != nil {
		log.Error("failed to seek file pointer", zap.Int("offset", 0),
			zap.String("whence", "end"),
		)
	}
	return fileStatus, replayStatus, owningInstanceID, nil
}

func (wf *WALFileType) callerOwnsFile(callersInstanceID int64) bool {
	// syncStatus() should be called prior to this
	return callersInstanceID == wf.OwningInstanceID
}

func (wf *WALFileType) isActive(callersInstanceID int64) bool {
	// syncStatus() should be called prior to this
	rState := wf.ReplayState
	return wf.IsOpen() && wf.callerOwnsFile(callersInstanceID) && rState == wal.NOTREPLAYED
}

func (wf *WALFileType) NeedsReplay() (bool, error) {
	if err := wf.syncStatusRead(); err != nil {
		return false, fmt.Errorf("wal syncStatuRead: %w", err)
	}

	if wf.ReplayState == wal.NOTREPLAYED || wf.ReplayState == wal.REPLAYINPROCESS {
		return true, nil
	}
	return false, nil
}

func (wf *WALFileType) CanWrite(callersInstanceID int64) (bool, error) {
	if err := wf.syncStatusRead(); err != nil {
		return false, fmt.Errorf("read syncStatus:%w", err)
	}
	if !wf.isActive(callersInstanceID) {
		log.Warn(io.GetCallerFileContext(0) + ": Inactive WALFile")
		return false, nil
	}
	return true, nil
}

func sanityCheckValue(fp *os.File, value int64) (isSane bool) {
	const safetyFactor = 1000
	// As a sanity check, get the file size to ensure that TGLen is reasonable prior to buffer allocations
	fstat, _ := fp.Stat()
	sanityLen := safetyFactor * fstat.Size()
	return value < sanityLen
}

var haveWALWriter = false

func (wf *WALFileType) SyncWAL(walRefresh, primaryRefresh time.Duration, walRotateInterval int) {
	/*
	   Example: syncWAL(500 * time.Millisecond, 15 * time.Minute)
	*/
	const (
		numTickerCheckPerWALRefresh = 100
		writeChannelCapThreshold    = 0.8
	)
	haveWALWriter = true
	tickerWAL := time.NewTicker(walRefresh)
	tickerPrimary := time.NewTicker(primaryRefresh)
	tickerCheck := time.NewTicker(walRefresh / numTickerCheckPerWALRefresh)
	primaryFlushCounter := 0

	chanCap := cap(wf.txnPipe.writeChannel)
	for {
		if !*wf.shutdownPending {
			select {
			case <-tickerWAL.C:
				if err := wf.FlushToWAL(); err != nil {
					log.Error("[tickerWAL] failed to FlushToWAL: " + err.Error())
				}
			case f := <-wf.txnPipe.flushChannel:
				if err := wf.FlushToWAL(); err != nil {
					log.Error("[txnPipe.flushChannel] failed to FlushToWAL: " + err.Error())
				}
				f <- struct{}{}
			case <-tickerCheck.C:
				queued := len(wf.txnPipe.writeChannel)
				if float64(queued)/float64(chanCap) >= writeChannelCapThreshold {
					if err := wf.FlushToWAL(); err != nil {
						log.Error("[tickerCheck] failed to FlushToWAL: " + err.Error())
					}
				}
			case <-tickerPrimary.C:
				if err := wf.CreateCheckpoint(); err != nil {
					log.Error("failed to create WAL checkpoint", zap.Error(err))
				}
				primaryFlushCounter++
				if primaryFlushCounter%walRotateInterval == 0 {
					log.Info("Truncating WAL file...")
					if err := wf.FilePtr.Truncate(0); err != nil {
						log.Error("failed to truncate wal file", zap.Error(err))
					}
					if err := wf.WriteStatus(wal.OPEN, wal.NOTREPLAYED); err != nil {
						log.Error("failed to write NOT_REPLAYED status to wal", zap.Error(err))
					}
					primaryFlushCounter = 0
				}
			}
		} else {
			haveWALWriter = false
			log.Info("Flushing to WAL...")
			err := wf.FlushToWAL()
			if err != nil {
				log.Error("[shutdown] failed to flush to WAL: " + err.Error())
			}
			log.Info("Flushing to disk...")
			err = wf.CreateCheckpoint()
			if err != nil {
				log.Error("[shutdown] failed to createCheckpoint in WAL: " + err.Error())
			}
			wf.walWaitGroup.Done()
			return
		}
	}
}

// RequestFlush requests WAL Flush to the WAL writer goroutine
// if it exists, or just does the work in the same goroutine otherwise.
// The function blocks if there are no current queued flushes, and
// returns if there is already one queued which will handle the data
// present in the write channel, as it will flush as soon as possible.
func (wf *WALFileType) RequestFlush() {
	if !haveWALWriter {
		if err := wf.FlushToWAL(); err != nil {
			log.Error("failed to flush WAL", zap.Error(err))
		}
		return
	}
	// if there's already a queued flush, no need to queue another
	if len(wf.txnPipe.flushChannel) > 0 {
		return
	}
	f := make(chan struct{})
	wf.txnPipe.flushChannel <- f
	<-f
}

func (wf *WALFileType) TriggerShutdown() {
	*wf.shutdownPending = true
}

// FinishAndWait closes the writtenIndexes channel, and waits
// for the remaining triggers to fire, returning.
func (wf *WALFileType) FinishAndWait() {
	const tryCloseInterval = 500 * time.Millisecond
	wf.tpd.triggerWg.Wait()
	for {
		if len(wf.txnPipe.writeChannel) == 0 && len(wf.tpd.c) == 0 {
			close(wf.tpd.c)
			<-wf.tpd.done
			return
		}
		time.Sleep(tryCloseInterval)
	}
}
