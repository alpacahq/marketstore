package executor

import (
	"crypto/md5"
	"fmt"
	goio "io"
	"os"
	"time"

	"github.com/alpacahq/marketstore/plugins/trigger"

	"bytes"
	"io/ioutil"
	"path/filepath"
	"sort"

	"github.com/alpacahq/marketstore/executor/buffile"
	"github.com/alpacahq/marketstore/utils/io"
	"github.com/alpacahq/marketstore/utils/log"
)

/*
	NOTE: Access to the WAL structures for a single WAL File is single threaded
	of the CommandChannel.
*/

type WALFileType struct {
	// These three fields plus the MID form the WAL Header, written at the beginning of the WAL File
	FileStatus       FileStatusEnum
	ReplayState      ReplayStateEnum
	OwningInstanceID int64
	// End of WAL Header
	RootPath          string   // Path to the root directory, base of FileName
	FilePath          string   // WAL file full path
	lastCommittedTGID int64    // TGID to be checkpointed
	FilePtr           *os.File // Active file pointer to FileName
}

func NewWALFile(rootDir string, existingFilePath string) (wf *WALFileType, err error) {
	wf = new(WALFileType)
	wf.lastCommittedTGID = 0

	if len(existingFilePath) == 0 {
		if err = wf.createFile(rootDir); err != nil {
			log.Fatal("%v: Can not create new WALFile - Error: %v", io.GetCallerFileContext(0), err)
		}
		wf.WriteStatus(OPEN, NOTREPLAYED)
	} else {
		if err = wf.takeOverFile(rootDir, existingFilePath); err != nil {
			log.Fatal("%v: Can not take over existing WALFile - Error: %v", io.GetCallerFileContext(0), err)
		}
		// We call this to take over the file by writing our PID to it
		fileStatus, replayState, _ := wf.readStatus()
		wf.WriteStatus(fileStatus, replayState)
	}
	return wf, nil
}
func (wf *WALFileType) createFile(rootDir string) error {
	wf.RootPath = rootDir
	now := time.Now().UTC()
	nowNano := now.UnixNano()
	wf.FilePath = filepath.Join(rootDir, "WALFile")
	wf.FilePath = fmt.Sprintf("%s.%d", wf.FilePath, nowNano)
	wf.FilePath = wf.FilePath + ".walfile"
	// Try to open the file for writing, creating it in the process
	err := wf.Open()
	if err != nil {
		return WALCreateError("CreateFile" + err.Error())
	}
	return nil
}
func (wf *WALFileType) takeOverFile(rootDir string, existingPath string) error {
	wf.RootPath = rootDir
	wf.FilePath = existingPath
	err := wf.Open()
	if err != nil {
		return WALTakeOverError("TakeOverFile" + err.Error())
	}
	if wf.callerOwnsFile() {
		return WALTakeOverError("TakeOver: File file is owned by calling process")
	}
	return nil
}
func (wf *WALFileType) Open() error {
	var err error
	wf.FilePtr, err = os.OpenFile(wf.FilePath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return fmt.Errorf(io.GetCallerFileContext(0) + err.Error())
	}
	return nil
}
func (wf *WALFileType) Close(ReplayStatus ReplayStateEnum) {
	wf.WriteStatus(CLOSED, ReplayStatus)
	wf.FilePtr.Close()
}
func (wf *WALFileType) Delete() (err error) {
	if !wf.IsOpen() {
		log.Warn(io.GetCallerFileContext(0) + ": Can not delete open WALFile")
		return fmt.Errorf("WAL File is open")
	}
	if wf.isActive() {
		log.Warn(io.GetCallerFileContext(0) + ": Can not delete active WALFile")
		return fmt.Errorf("WAL File is active")
	}
	if wf.NeedsReplay() {
		log.Warn(io.GetCallerFileContext(0) + ": WALFile needs replay, can not delete")
		return fmt.Errorf("WAL File needs replay")
	}

	wf.Close(REPLAYED)
	if err = os.Remove(wf.FilePath); err != nil {
		log.Fatal(io.GetCallerFileContext(0) + ": Can not remove WALFile")
	}

	return nil
}
func (wf *WALFileType) read(targetOffset int64, buffer []byte) (result []byte, newOffset int64, err error) {
	/*
		Read from the WAL file
			targetOffset: -1 will read from current position
	*/
	offset, err := wf.FilePtr.Seek(0, os.SEEK_CUR)
	if err != nil {
		log.Fatal(io.GetCallerFileContext(0) + ": Unable to seek in WALFile")
	}
	if targetOffset != -1 {
		if offset != targetOffset {
			wf.FilePtr.Seek(targetOffset, os.SEEK_SET)
		}
	}
	numToRead := len(buffer)
	n, err := wf.FilePtr.Read(buffer)
	if n != numToRead {
		msg := fmt.Sprintf("Read: Expected: %d Got: %d", numToRead, n)
		err = ShortReadError(msg)
	} else if err != nil {
		log.Fatal(io.GetCallerFileContext(0) + ": Unable to read WALFile")
	}
	return buffer, offset + int64(n), err
}
func (wf *WALFileType) FullPathToWALKey(fullPath string) (keyPath string) {
	/*
		NOTE: This key includes the year filename at the end of the metadata key
	*/
	// Chops rootPath from fullPath to produce a WAL Key
	keyPath, _ = filepath.Rel(wf.RootPath, fullPath)
	return keyPath
}

func (wf *WALFileType) WALKeyToFullPath(keyPath string) (fullPath string) {
	// Adds rootPath to keyPath to produce a fullPath
	return filepath.Join(wf.RootPath, keyPath)
}

type offsetIndexBuffer []byte

func (b offsetIndexBuffer) Offset() int64 {
	return io.ToInt64(b[:8])
}

func (b offsetIndexBuffer) Index() int64 {
	return io.ToInt64(b[8:16])
}

func (b offsetIndexBuffer) IndexAndPayload() []byte {
	return b[8:]
}

func (b offsetIndexBuffer) Payload() []byte {
	return b[16:]
}

type CachedFP struct {
	fileName string
	fp       *os.File
}

func NewCachedFP() *CachedFP {
	return new(CachedFP)
}

func (cfp *CachedFP) GetFP(fileName string) (fp *os.File, err error) {
	if fileName == cfp.fileName {
		return cfp.fp, nil
	} else if len(cfp.fileName) != 0 {
		cfp.fp.Close()
	}
	cfp.fp, err = os.OpenFile(fileName, os.O_RDWR, 0700)
	if err != nil {
		return nil, err
	}
	cfp.fileName = fileName
	return cfp.fp, nil
}

func (cfp *CachedFP) Close() error {
	if cfp.fp != nil {
		return cfp.fp.Close()
	}
	return nil
}

// A.k.a. Commit transaction
func (wf *WALFileType) flushToWAL(tgc *TransactionPipe) (err error) {
	/*
		Here we flush the contents of the write cache to:
		- Primary storage via the OS write cache - data is visible to readers
		- WAL file with synchronization to physical storage - in case we need to recover from a crash
	*/

	defer dispatchRecords()

	WALBypass := ThisInstance.WALBypass
	//WALBypass = true // Bypass all writing to the WAL File, leaving the writes to the primary

	// Count of WT Sets in this TG as of now
	if tgc == nil {
		return nil
	}

	WTCount := len(tgc.writeChannel)
	if WTCount == 0 {
		// refresh TGID so requester can confirm it went through even if nothing is written
		tgc.NewTGID()
		return nil
	}

	if !WALBypass {
		if !wf.CanWrite("WriteTG") {
			panic("Failed attempt to write to WAL")
		}

		// WAL Transaction Preparing Message
		wf.WriteTransactionInfo(tgc.TGID(), WAL, PREPARING)
	}

	// Serialize all data to be written except for the size of this buffer
	var TG_Serialized, TGLen_Serialized []byte
	TG_Serialized, _ = io.Serialize(TG_Serialized, tgc.TGID())
	TG_Serialized, _ = io.Serialize(TG_Serialized, int64(WTCount))
	writesPerFile := map[string][]offsetIndexBuffer{}
	fileRecordTypes := map[string]io.EnumRecordType{}
	varRecLens := map[string]int32{}
	/*
		This loop serializes write transactions from the channel for writing to disk
	*/
	for i := 0; i < WTCount; i++ {
		command := <-tgc.writeChannel
		TG_Serialized, _ = io.Serialize(TG_Serialized, int8(command.RecordType))
		TG_Serialized, _ = io.Serialize(TG_Serialized, int16(len(command.WALKeyPath)))
		TG_Serialized, _ = io.Serialize(TG_Serialized, command.WALKeyPath)
		TG_Serialized, _ = io.Serialize(TG_Serialized, int32(len(command.Data)))
		TG_Serialized, _ = io.Serialize(TG_Serialized, int32(command.VarRecLen))
		oStart := len(TG_Serialized)
		bufferSize := 8 + 8 + len(command.Data)
		TG_Serialized, _ = io.Serialize(TG_Serialized, command.Offset)
		TG_Serialized, _ = io.Serialize(TG_Serialized, command.Index)
		TG_Serialized = append(TG_Serialized, command.Data...)
		keyPath := command.WALKeyPath
		// Store the data in a buffer for primary storage writes after WAL writes are done
		writesPerFile[keyPath] = append(writesPerFile[keyPath],
			offsetIndexBuffer(TG_Serialized[oStart:oStart+bufferSize]))
		if _, ok := fileRecordTypes[keyPath]; !ok {
			fileRecordTypes[keyPath] = command.RecordType
		}
		if _, ok := varRecLens[keyPath]; !ok {
			varRecLens[keyPath] = command.VarRecLen
		}
	}
	if !WALBypass {
		// Serialize the size of the buffer into another buffer
		TGLen_Serialized, _ = io.Serialize(TGLen_Serialized, int64(len(TG_Serialized)))

		// Calculate the MD5 checksum, including the value of TGLen
		hash := md5.New()
		hash.Write(TGLen_Serialized)
		hash.Write(TG_Serialized)

		wf.FilePtr.Write(wf.initMessage(TGDATA)) // Write the Message ID to identify TG Data
		// Write the TG Data and the checksum and Sync()
		wf.FilePtr.Write(TGLen_Serialized)
		wf.FilePtr.Write(TG_Serialized)
		cksum := hash.Sum(nil)
		wf.FilePtr.Write(cksum) // Checksum
		wf.FilePtr.Sync()       // Flush the OS buffer

		// WAL Transaction Commit Complete Message
		TGID := tgc.TGID()
		wf.WriteTransactionInfo(TGID, WAL, COMMITCOMPLETE)
		wf.lastCommittedTGID = TGID
		tgc.NewTGID()
	}

	/*
		Write the buffers to primary files (should happen after WAL writes)
	*/
	for keyPath, writes := range writesPerFile {
		recordType := fileRecordTypes[keyPath]
		varRecLen := varRecLens[keyPath]
		if err := wf.writePrimary(keyPath, writes, recordType, varRecLen); err != nil {
			return err
		}
		for i, buffer := range writes {
			appendRecord(keyPath, trigger.Record(buffer.IndexAndPayload()))
			writes[i] = nil // for GC
		}
		writesPerFile[keyPath] = nil // for GC
	}
	return nil
}

func (wf *WALFileType) writePrimary(keyPath string, writes []offsetIndexBuffer, recordType io.EnumRecordType, varRecLen int32) (err error) {
	type WriteAtCloser interface {
		goio.WriterAt
		goio.Closer
	}
	const batchThreshold = 100
	var fp WriteAtCloser
	fullPath := wf.WALKeyToFullPath(keyPath)
	if recordType == io.FIXED && len(writes) >= batchThreshold {
		fp, err = buffile.New(fullPath)
	} else {
		fp, err = os.OpenFile(fullPath, os.O_RDWR, 0700)
	}
	if err != nil {
		// this is critical, in fact, since tx has been committed
		log.Error("cannot open file %s for write: %v", fullPath, err)
		return err
	}
	defer fp.Close()

	for _, buffer := range writes {
		switch recordType {
		case io.FIXED:
			err = WriteBufferToFile(fp, buffer)
		case io.VARIABLE:
			err = WriteBufferToFileIndirect(
				fp.(*os.File),
				buffer,
				varRecLen,
			)
		}
		if err != nil {
			log.Error("failed to write committed data: %v", err)
			return err
		}
	}
	return nil
}

// createCheckpoint flushes all primary dirty pages to disk, and
// so closes out the previous WAL state to end.  Note, this is
// not goroutine-safe with flushToWAL and caller should make sure
// it is streamlined.
func (wf *WALFileType) createCheckpoint() error {
	if wf.lastCommittedTGID == 0 {
		return nil
	}
	if ThisInstance.WALBypass {
		io.Syncfs()
	} else {
		// WAL Transaction Preparing Message
		// Get the latest TGID and write a prepare message
		TGID := wf.lastCommittedTGID
		wf.WriteTransactionInfo(TGID, CHECKPOINT, PREPARING)
		// Sync the filesystem, after this point the filesystem cache data is committed to disk
		io.Syncfs()
		wf.WriteTransactionInfo(TGID, CHECKPOINT, COMMITCOMPLETE)
	}
	wf.lastCommittedTGID = 0
	return nil
}

type TGIDlist []int64

func (tgl TGIDlist) Len() int           { return len(tgl) }
func (tgl TGIDlist) Less(i, j int) bool { return tgl[i] < tgl[j] }
func (tgl TGIDlist) Swap(i, j int)      { tgl[i], tgl[j] = tgl[j], tgl[i] }

func (wf *WALFileType) Replay(writeData bool) error {
	/*
		Replay this WAL File's unwritten transactions.
		We will do this in two passes, in the first pass we will collect the Transaction Group IDs that are
		not yet durably written to the primary store. In the second pass, we write the data into the
		Primary Store directly and then flush the results.
		Finally we close the WAL File and mark it completely written.

		1) First WAL Pass: Locate unwritten TGIDs
		2) Second WAL Pass: Load the open TG data into the Primary Data files
		3) Flush the TG Cache to primary and mark this WAL File completely processed

		Note that the TG Data for any given TGID should appear in the WAL only once. We verify it in the first
		pass.
	*/

	// Make sure this file needs replay
	if !wf.NeedsReplay() {
		err := fmt.Errorf("WALFileType.NeedsReplay No Replay Needed")
		log.Info(err.Error())
		return err
	}

	// Take control of this file and set the status
	if writeData {
		wf.WriteStatus(OPEN, REPLAYINPROCESS)
	}

	// First pass of WAL Replay: determine transaction states and record locations of TG data
	txnStateWAL := make(map[int64]TxnStatusEnum, 0)
	txnStatePrimary := make(map[int64]TxnStatusEnum, 0)
	offsetTGDataInWAL := make(map[int64]int64, 0)

	fullRead := func(err error) bool {
		// Check to see if we have read only partial data
		if err != nil {
			if _, ok := err.(ShortReadError); ok {
				log.Info("Partial Read")
				return false
			} else {
				log.Fatal(io.GetCallerFileContext(0) + ": Uncorrectable IO error in WAL Replay")
			}
		}
		return true
	}
	log.Info("Beginning WAL Replay")
	if !writeData {
		log.Info("Debugging mode enabled - no writes will be performed...")
	}
	// Create a map to store the TG Data prior to replay
	TGData := make(map[int64][]byte)

	wf.FilePtr.Seek(0, goio.SeekStart)
	continueRead := true
	for continueRead {
		MID, err := wf.readMessageID()
		if continueRead = fullRead(err); !continueRead {
			break // Break out of read loop
		}
		switch MID {
		case TGDATA:
			// Read a TGData
			offset, _ := wf.FilePtr.Seek(0, goio.SeekCurrent)
			TGID, TG_Serialized, err := wf.readTGData()
			TGData[TGID] = TG_Serialized
			if continueRead = fullRead(err); !continueRead {
				break // Break out of switch
			}
			// Throw FATAL if there is already a TG data location in this WAL
			if _, ok := offsetTGDataInWAL[TGID]; ok {
				log.Fatal(io.GetCallerFileContext(0) + ": Duplicate TG Data in WAL")
			}
			//			log.Info("Successfully read past TG data for TGID: %v", TGID)
			// Save the offset of this TG Data for the second pass
			offsetTGDataInWAL[TGID] = offset
		case TXNINFO:
			// Read a TXNInfo
			TGID, destination, txnStatus, err := wf.readTransactionInfo()
			if continueRead = fullRead(err); !continueRead {
				break // Break out of switch
			}
			switch destination {
			case WAL:
				txnStateWAL[TGID] = txnStatus
			case CHECKPOINT:
				if _, ok := TGData[TGID]; ok && txnStatus == COMMITCOMPLETE {
					// Remove all TGData for TGID less than this complete one
					for tgid, _ := range TGData {
						if tgid <= TGID {
							TGData[tgid] = nil
							delete(TGData, tgid)
						}
					}
				} else {
					// Record this txnStatus for later analysis
					txnStatePrimary[TGID] = txnStatus
				}
			}
		case STATUS:
			// Read the status - note that this message should only be at the file beginning
			_, _, _, err := wf.ReadStatus()
			if continueRead = fullRead(err); !continueRead {
				break // Break out of switch
			}
		default:
			log.Warn("Unknown meessage id %d", MID)
		}
	}

	// Second Pass of WAL Replay: Find any pending transactions based on the state and load the TG data into cache
	log.Info("Entering replay of TGData")
	// We need to replay TGs in descending TGID order

	// StringSlice attaches the methods of Interface to []string, sorting in increasing order.

	var sortedTGIDs TGIDlist
	for tgid := range TGData {
		sortedTGIDs = append(sortedTGIDs, tgid)
	}
	sort.Sort(sortedTGIDs)

	//for tgid, TG_Serialized := range TGData {
	for _, tgid := range sortedTGIDs {
		TG_Serialized := TGData[tgid]
		if TG_Serialized != nil {
			// Note that only TG data that did not have a COMMITCOMPLETE record are replayed
			if writeData {
				log.Info("Replaying TGID: %d, data length is: %d bytes", tgid, len(TG_Serialized))
				if err := wf.replayTGData(TG_Serialized); err != nil {
					return err
				}
			} else {
				log.Info("Replay for TGID: %d, data length is: %d bytes", tgid, len(TG_Serialized))
			}
		}
	}
	log.Info("Replay of WAL file %s finished", wf.FilePath)
	if writeData {
		wf.WriteStatus(OPEN, REPLAYED)
	}

	log.Info("Finished replay of TGData")
	return nil
}
func (wf *WALFileType) WriteStatus(FileStatus FileStatusEnum, ReplayState ReplayStateEnum) {
	wf.FileStatus = FileStatus
	wf.ReplayState = ReplayState
	// This process now owns this file
	wf.OwningInstanceID = ThisInstance.InstanceID
	buffer := wf.initMessage(STATUS)
	buffer, _ = io.Serialize(buffer, int8(wf.FileStatus))
	buffer, _ = io.Serialize(buffer, int8(wf.ReplayState))
	buffer, _ = io.Serialize(buffer, int64(wf.OwningInstanceID))
	wf.FilePtr.Seek(0, os.SEEK_SET)
	wf.FilePtr.Write(buffer)
	wf.FilePtr.Sync()
	wf.FilePtr.Seek(0, os.SEEK_END)
}
func (wf *WALFileType) write(buffer []byte) {
	wf.FilePtr.Write(buffer)
	wf.FilePtr.Sync()
}
func (wf *WALFileType) WriteTransactionInfo(tid int64, did DestEnum, txnStatus TxnStatusEnum) {
	buffer := wf.initMessage(TXNINFO)
	buffer, _ = io.Serialize(buffer, tid)
	buffer, _ = io.Serialize(buffer, did)
	buffer, _ = io.Serialize(buffer, txnStatus)
	wf.write(buffer)
}
func (wf *WALFileType) readTransactionInfo() (tgid int64, destination DestEnum, txnStatus TxnStatusEnum, err error) {
	var buffer [10]byte
	buf, _, err := wf.read(-1, buffer[:])
	if err != nil {
		return 0, 0, 0, ShortReadError("WALFileType.readTransactionInfo")
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
func (wf *WALFileType) writeMessageID(mid MIDEnum) {
	wf.write(wf.initMessage(mid))
}
func (wf *WALFileType) readMessageID() (mid MIDEnum, err error) {
	var buffer [1]byte
	buf, _, err := wf.read(-1, buffer[:])
	if err != nil {
		return 0, ShortReadError("WALFileType.ReadMessageID")
	}
	MID := MIDEnum(buf[0])
	switch MID {
	case TGDATA, TXNINFO, STATUS:
		return MID, nil
	}
	return 99, fmt.Errorf("WALFileType.ReadMessageID Incorrect MID read, value: %d", MID)
}
func (wf *WALFileType) readTGData() (TGID int64, TG_Serialized []byte, err error) {
	TGLen_Serialized := make([]byte, 8)
	TGLen_Serialized, _, err = wf.read(-1, TGLen_Serialized)
	if err != nil {
		return 0, nil, ShortReadError(io.GetCallerFileContext(0))
	}
	TGLen := io.ToInt64(TGLen_Serialized)

	if !wf.sanityCheckValue(TGLen) {
		return 0, nil, fmt.Errorf(io.GetCallerFileContext(0) + fmt.Sprintf(": Insane TG Length: %d", TGLen))
	}

	// Read the data
	TG_Serialized = make([]byte, TGLen)
	n, err := wf.FilePtr.Read(TG_Serialized)
	if int64(n) != TGLen || err != nil {
		return 0, nil, ShortReadError(io.GetCallerFileContext(0) + ":Reading Data")
	}
	TGID = io.ToInt64(TG_Serialized[:7])

	// Compute the checksum
	hash := md5.New()
	hash.Write(TGLen_Serialized)
	hash.Write(TG_Serialized)
	cksum := hash.Sum(nil)

	// Read the checksum
	checkBuf := make([]byte, 16)
	n, err = wf.FilePtr.Read(checkBuf)
	if n != 16 || err != nil {
		return 0, nil, ShortReadError(io.GetCallerFileContext(0) + ":Reading Checksum")
	}
	if !bytes.Equal(cksum, checkBuf) {
		return 0, nil, fmt.Errorf(io.GetCallerFileContext(0) + fmt.Sprintf(":Checksum was: %v should be: %v", cksum, checkBuf))
	}

	return TGID, TG_Serialized, nil
}
func (wf *WALFileType) replayTGData(TG_Serialized []byte) (err error) {
	TGID := io.ToInt64(TG_Serialized[0:8])
	WTCount := io.ToInt64(TG_Serialized[8:16])
	cursor := 16
	if int(WTCount) != 0 {
		cfp := NewCachedFP() // Cached open file pointer
		defer cfp.Close()
		for i := 0; i < int(WTCount); i++ {
			RecordType := int(io.ToInt8(TG_Serialized[cursor : cursor+1]))
			cursor += 1
			FPLen := int(io.ToInt16(TG_Serialized[cursor : cursor+2]))
			cursor += 2
			WALKeyPath := bytes.NewBuffer(TG_Serialized[cursor : cursor+FPLen]).String()
			cursor += FPLen
			dataLen := int(io.ToInt32(TG_Serialized[cursor : cursor+4]))
			cursor += 4
			varRecLen := io.ToInt32(TG_Serialized[cursor : cursor+4])
			cursor += 4
			fullPath := wf.WALKeyToFullPath(WALKeyPath)
			fp, err := cfp.GetFP(fullPath)
			if err != nil {
				return err
			}
			switch io.EnumRecordType(RecordType) {
			case io.FIXED:
				if err = WriteBufferToFile(fp, TG_Serialized[cursor:cursor+8+8+dataLen]); err != nil {
					return err
				}
			case io.VARIABLE:
				// Find the record length - we need it to use the time column as a sort key later
				if err = WriteBufferToFileIndirect(fp,
					TG_Serialized[cursor:cursor+8+8+dataLen],
					varRecLen,
				); err != nil {
					return err
				}
			default:
				return fmt.Errorf("Error: Record Type is incorrect from WALFile, invalid/outdated WAL file?")
			}
			cursor += 8 + 8 + dataLen
		}
		wf.lastCommittedTGID = TGID
		wf.createCheckpoint()
	}
	return nil
}
func (wf *WALFileType) ReadStatus() (fileStatus FileStatusEnum, replayStatus ReplayStateEnum, OwningInstanceID int64, err error) {
	var buffer [10]byte
	buf, _, err := wf.read(-1, buffer[:])
	return FileStatusEnum(buf[0]), ReplayStateEnum(buf[1]), io.ToInt64(buf[2:]), err
}
func (wf *WALFileType) IsOpen() bool {
	_, err := wf.FilePtr.Stat()
	if err != nil {
		log.Info(io.GetCallerFileContext(0) + ": File stat failed, file probably deleted: " + err.Error())
		return false
	}
	if wf.FileStatus != OPEN {
		log.Info(io.GetCallerFileContext(0) + ": File not opened")
		return false
	}
	return true
}
func (wf *WALFileType) syncStatusRead() {
	_, err := wf.FilePtr.Stat()
	if err != nil {
		log.Fatal(io.GetCallerFileContext(0) + ": File stat failed")
	}
	wf.FileStatus, wf.ReplayState, wf.OwningInstanceID = wf.readStatus()
}
func (wf *WALFileType) readStatus() (fileStatus FileStatusEnum, replayStatus ReplayStateEnum, owningInstanceID int64) {
	// Read from beginning of file +1 to skip over the MID
	wf.FilePtr.Seek(1, os.SEEK_SET)
	var err error
	fileStatus, replayStatus, owningInstanceID, err = wf.ReadStatus()
	if err != nil {
		log.Fatal(io.GetCallerFileContext(0) + ": Unable to ReadStatus()")
	}
	//	wf.FileStatus, wf.ReplayState, wf.OwningInstanceID = fileStatus, replayStatus, owningInstanceID
	// Reset the file pointer to the end of the file
	wf.FilePtr.Seek(0, os.SEEK_END)
	return fileStatus, replayStatus, owningInstanceID
}
func (wf *WALFileType) callerOwnsFile() bool {
	// syncStatus() should be called prior to this
	return ThisInstance.InstanceID == wf.OwningInstanceID
}
func (wf *WALFileType) isActive() bool {
	// syncStatus() should be called prior to this
	rState := wf.ReplayState
	return wf.IsOpen() && wf.callerOwnsFile() && rState == NOTREPLAYED
}
func (wf *WALFileType) NeedsReplay() bool {
	wf.syncStatusRead()
	if wf.ReplayState == NOTREPLAYED || wf.ReplayState == REPLAYINPROCESS {
		return true
	}
	return false
}
func (wf *WALFileType) CanWrite(msg string) bool {
	wf.syncStatusRead()
	if !wf.isActive() {
		log.Warn(io.GetCallerFileContext(0) + ": Inactive WALFile")
		return false
	}
	return true
}
func (wf *WALFileType) CanDeleteSafely() bool {
	wf.syncStatusRead()
	if wf.isActive() {
		log.Warn(io.GetCallerFileContext(0) + ": WALFile is active, can not delete")
		return false
	}
	if wf.NeedsReplay() {
		log.Warn(io.GetCallerFileContext(0) + ": WALFile needs replay, can not delete")
		return false
	}
	return true
}
func (wf *WALFileType) sanityCheckValue(value int64) (isSane bool) {
	// As a sanity check, get the file size to ensure that TGLen is reasonable prior to buffer allocations
	fstat, _ := wf.FilePtr.Stat()
	sanityLen := 1000 * fstat.Size()
	return value < sanityLen
}
func (wf *WALFileType) cleanupOldWALFiles(rootDir string) {
	rootDir = filepath.Clean(rootDir)
	files, err := ioutil.ReadDir(rootDir)
	if err != nil {
		log.Fatal("Unable to read root directory %s\n%s", rootDir, err)
	}
	myFileBase := filepath.Base(wf.FilePath)
	log.Info("My WALFILE: %s", myFileBase)
	for _, file := range files {
		if !file.IsDir() {
			filename := file.Name()
			if filepath.Ext(filename) == ".walfile" {
				if filename != myFileBase {
					log.Info("Found a WALFILE: %s, entering replay...", filename)
					filePath := filepath.Join(rootDir, filename)
					fi, _ := os.Stat(filePath)
					if fi.Size() < 11 {
						log.Info("WALFILE: %s is empty, removing it...", filename)
						os.Remove(filePath)
					} else {
						w, err := NewWALFile(rootDir, filePath)
						if err != nil {
							log.Fatal("Opening %s\n%s", filename, err)
						}
						if err = w.Replay(true); err != nil {
							log.Fatal("Unable to replay %s\n%s", filename, err)
						}
						if !w.CanDeleteSafely() {
							log.Fatal("Unable to delete %s after replay", filename)
						}
						w.Delete()
					}
				}
			}
		}
	}
}

func StartupCacheAndWAL(rootDir string) (tgc *TransactionPipe, wf *WALFileType, err error) {
	wf, err = NewWALFile(rootDir, "")
	if err != nil {
		log.Error("%s", err.Error())
		return nil, nil, err
	}
	wf.cleanupOldWALFiles(rootDir)
	return NewTransactionPipe(), wf, nil
}

var haveWALWriter = false

func (wf *WALFileType) SyncWAL(WALRefresh, PrimaryRefresh time.Duration, walRotateInterval int) {
	/*
	   Example: syncWAL(500 * time.Millisecond, 15 * time.Minute)
	*/
	haveWALWriter = true
	tickerWAL := time.NewTicker(WALRefresh)
	tickerPrimary := time.NewTicker(PrimaryRefresh)
	tickerCheck := time.NewTicker(WALRefresh / 100)
	primaryFlushCounter := 0

	chanCap := cap(ThisInstance.TXNPipe.writeChannel)
	for {
		if !ThisInstance.ShutdownPending {
			select {
			case <-tickerWAL.C:
				if err := wf.flushToWAL(ThisInstance.TXNPipe); err != nil {
					log.Fatal(err.Error())
				}
			case f := <-ThisInstance.TXNPipe.flushChannel:
				if err := wf.flushToWAL(ThisInstance.TXNPipe); err != nil {
					log.Fatal(err.Error())
				}
				f <- struct{}{}
			case <-tickerCheck.C:
				queued := len(ThisInstance.TXNPipe.writeChannel)
				if float64(queued)/float64(chanCap) >= 0.8 {
					if err := wf.flushToWAL(ThisInstance.TXNPipe); err != nil {
						log.Fatal(err.Error())
					}
				}
			case <-tickerPrimary.C:
				wf.createCheckpoint()
				primaryFlushCounter++
				if primaryFlushCounter%walRotateInterval == 0 {
					log.Info("Truncating WAL file...")
					wf.FilePtr.Truncate(0)
					wf.WriteStatus(OPEN, NOTREPLAYED)
					primaryFlushCounter = 0
				}
			}
		} else {
			haveWALWriter = false
			log.Info("Flushing to WAL...")
			wf.flushToWAL(ThisInstance.TXNPipe)
			log.Info("Flushing to disk...")
			wf.createCheckpoint()
			ThisInstance.WALWg.Done()
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
		wf.flushToWAL(ThisInstance.TXNPipe)
		return
	}
	// if there's already a queued flush, no need to queue another
	if len(ThisInstance.TXNPipe.flushChannel) > 0 {
		return
	}
	f := make(chan struct{})
	ThisInstance.TXNPipe.flushChannel <- f
	<-f
}
