package executor

import (
	"errors"
	"fmt"
	goio "io"
	"path/filepath"
	"sort"

	"github.com/alpacahq/marketstore/v4/executor/wal"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

// Replay loads this WAL File's unwritten transactions to primary store and mark it completely processed.
// We will do this in two passes, in the first pass we will collect the Transaction Group IDs that are
// not yet durably written to the primary store. In the second pass, we write the data into the
// Primary Store directly and then flush the results.
// Finally we close the WAL File and mark it completely written.
//
// 1) First WAL Pass: Locate unwritten TGIDs
// 2) Second WAL Pass: Load the open TG data into the Primary Data files
// 3) Flush the TG Cache to primary and mark this WAL File completely processed
//
// Note that the TG Data for any given TGID should appear in the WAL only once. We verify it in the first pass.
func (wf *WALFileType) Replay(dryRun bool) error {
	// Make sure this file needs replay
	needsReplay, err := wf.NeedsReplay()
	if err != nil {
		return fmt.Errorf("check if walfile needs to be replayed: %w", err)
	}
	if !needsReplay {
		log.Info("No WAL Replay needed.")
		return wal.ReplayError{
			Msg:  "WALFileType.NeedsReplay No Replay Needed",
			Cont: true,
		}
	}

	// Take control of this file and set the status
	if !dryRun {
		wf.WriteStatus(wal.OPEN, wal.REPLAYINPROCESS)
	}

	// First pass of WAL Replay: determine transaction states and record locations of TG data
	txnStateWAL := make(map[int64]TxnStatusEnum)
	txnStatePrimary := make(map[int64]TxnStatusEnum)
	offsetTGDataInWAL := make(map[int64]int64)

	log.Info("Beginning WAL Replay")
	if dryRun {
		log.Info("Debugging mode enabled - no writes will be performed...")
	}
	// Create a map to store the TG Data prior to replay
	tgData := make(map[int64][]byte)

	_, err = wf.FilePtr.Seek(0, goio.SeekStart)
	if err != nil {
		return fmt.Errorf("seek wal from start for replay:%w", err)
	}
	continueRead := true
	for continueRead {
		msgID, err := wf.readMessageID()
		if continueRead = fullRead(err); !continueRead {
			break // Break out of read loop
		}
		switch msgID {
		case TGDATA:
			// Read a TGData
			offset, err := wf.FilePtr.Seek(0, goio.SeekCurrent)
			if err != nil {
				return fmt.Errorf("seek error: %w", err)
			}
			tgID, tgSerialized, err := wf.readTGData()
			tgData[tgID] = tgSerialized
			if continueRead = fullRead(err); !continueRead {
				break // Break out of switch
			}
			// give up Replay if there is already a TG data location in this WAL
			if _, ok := offsetTGDataInWAL[tgID]; ok {
				log.Error(io.GetCallerFileContext(0) + ": Duplicate TG Data in WAL")
				return wal.ReplayError{
					Msg:  fmt.Sprintf("Duplicate TG Data in WAL. tgID=%d", tgID),
					Cont: true,
				}
			}
			// log.Info("Successfully read past TG data for tgID: %v", tgID)
			// Save the offset of this TG Data for the second pass
			offsetTGDataInWAL[tgID] = offset
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
				if _, ok := tgData[TGID]; ok && txnStatus == COMMITCOMPLETE {
					// Remove all TGData for tgID less than this complete one
					for tgid := range tgData {
						if tgid <= TGID {
							tgData[tgid] = nil
							delete(tgData, tgid)
						}
					}
				} else {
					// Record this txnStatus for later analysis
					txnStatePrimary[TGID] = txnStatus
				}
			}
		case STATUS:
			// Read the status - note that this message should only be at the file beginning
			_, _, _, err := wal.ReadStatus(wf.FilePtr)
			if continueRead = fullRead(err); !continueRead {
				break // Break out of switch
			}
		default:
			log.Warn("Unknown meessage id %d", msgID)
		}
	}

	// Second Pass of WAL Replay: Find any pending transactions based on the state and load the TG data into cache
	log.Info("Entering replay of TGData")
	// We need to replay TGs in descending TGID order

	// StringSlice attaches the methods of Interface to []string, sorting in increasing order.

	var sortedTGIDs TGIDlist
	for tgid := range tgData {
		sortedTGIDs = append(sortedTGIDs, tgid)
	}
	sort.Sort(sortedTGIDs)

	// for tgid, TG_Serialized := range tgData {
	for _, tgid := range sortedTGIDs {
		tgSerialized := tgData[tgid]
		if tgSerialized == nil {
			continue
		}

		if dryRun {
			continue
		}

		// Note that only TG data that did not have a COMMITCOMPLETE record are replayed
		rootDir := filepath.Dir(wf.FilePtr.Name())
		tgID, wtSets := ParseTGData(tgSerialized, rootDir)
		if err := wf.replayTGData(tgID, wtSets); err != nil {
			return fmt.Errorf("replay transaction group data. tgID=%d, "+
				"write transaction size=%d:%w", tgID, len(wtSets), err)
		}
	}

	log.Info("Replay of WAL file %s finished", wf.FilePtr.Name())
	if !dryRun {
		wf.WriteStatus(wal.OPEN, wal.REPLAYED)
	}

	log.Info("Finished replay of TGData")
	return nil
}

func (wf *WALFileType) replayTGData(tgID int64, wtSets []wal.WTSet) (err error) {
	if len(wtSets) == 0 {
		return nil
	}

	cfp := NewCachedFP() // Cached open file pointer
	defer func() {
		err2 := cfp.Close()
		if err2 != nil {
			log.Error(fmt.Sprintf("failed to close cached file pointer %s: %v", cfp, err2))
		}
	}()

	for _, wtSet := range wtSets {
		fp, err2 := cfp.GetFP(wtSet.FilePath)
		if err2 != nil {
			return wal.ReplayError{
				Msg: fmt.Sprintf("failed to open a filepath %s in write transaction set:%v",
					wtSet.FilePath, err2.Error(),
				),
				Cont: true,
			}
		}
		switch wtSet.RecordType {
		case io.FIXED:
			if err = WriteBufferToFile(fp, wtSet.Buffer); err != nil {
				return err
			}
		case io.VARIABLE:
			// Find the record length - we need it to use the time column as a sort key later
			if err = WriteBufferToFileIndirect(fp,
				wtSet.Buffer,
				wtSet.VarRecLen,
			); err != nil {
				return err
			}
		default:
			return fmt.Errorf("record Type is incorrect from WALFile, may be invalid/outdated WAL file")
		}
	}
	wf.lastCommittedTGID = tgID
	err = wf.CreateCheckpoint()
	if err != nil {
		return fmt.Errorf("create checkpoint of wal:%w", err)
	}

	return nil
}

// fullRead checks an error to see if we have read only partial data.
func fullRead(err error) bool {
	if err == nil {
		return true
	}

	if errors.Is(err, goio.EOF) {
		log.Debug("fullRead: read until the end of WAL file")
		return false
	}

	var targetErr wal.ShortReadError
	if ok := errors.As(err, &targetErr); ok {
		log.Info(fmt.Sprintf("Partial Read. err=%v", err))
		return false
	} else {
		log.Error(io.GetCallerFileContext(0) + ": Uncorrectable IO error in WAL Replay")
	}

	return true
}

// readMessageID reads 1 byte from the current offset of WALfile and return it as a MessageID.
// If it's at the end of wal file, readMessageID returns 0, io.EOF error.
// If it's not at the end of wal file but couldn't read 1 byte, readMessageID returns 0, wal.ShortReadError.
func (wf *WALFileType) readMessageID() (mid MIDEnum, err error) {
	const unknownMessageID = 99
	var buffer [1]byte
	buf, _, err := wal.Read(wf.FilePtr, buffer[:])
	if err != nil {
		if errors.Is(err, goio.EOF) {
			return 0, goio.EOF
		}
		return 0, wal.ShortReadError("WALFileType.ReadMessageID. err:" + err.Error())
	}
	MID := MIDEnum(buf[0])
	switch MID {
	case TGDATA, TXNINFO, STATUS:
		return MID, nil
	}
	return unknownMessageID, fmt.Errorf("WALFileType.ReadMessageID Incorrect MID read, value: %d:%w", MID, err)
}

const (
	// see /docs/durable_writes_design.txt for definition.
	tgLenBytes    = 8
	tgIDBytes     = 8
	checkSumBytes = 16
)

func (wf *WALFileType) readTGData() (TGID int64, tgSerialized []byte, err error) {
	tgLenSerialized := make([]byte, tgLenBytes)
	tgLenSerialized, _, err = wal.Read(wf.FilePtr, tgLenSerialized)
	if err != nil {
		return 0, nil, wal.ShortReadError(io.GetCallerFileContext(0))
	}
	tgLen := io.ToInt64(tgLenSerialized)

	if !sanityCheckValue(wf.FilePtr, tgLen) {
		return 0, nil, fmt.Errorf(io.GetCallerFileContext(0) + fmt.Sprintf(": Insane TG Length: %d", tgLen))
	}

	// Read the data
	tgSerialized = make([]byte, tgLen)
	n, err := wf.FilePtr.Read(tgSerialized)
	if int64(n) != tgLen || err != nil {
		return 0, nil, wal.ShortReadError(io.GetCallerFileContext(0) + ":Reading Data")
	}
	TGID = io.ToInt64(tgSerialized[:tgIDBytes-1])

	// Read the checksum
	checkBuf := make([]byte, checkSumBytes)
	n, err = wf.FilePtr.Read(checkBuf)
	if n != checkSumBytes || err != nil {
		return 0, nil, wal.ShortReadError(io.GetCallerFileContext(0) + ":Reading Checksum")
	}

	if err := validateCheckSum(tgLenSerialized, tgSerialized, checkBuf); err != nil {
		return 0, nil, err
	}

	return TGID, tgSerialized, nil
}
