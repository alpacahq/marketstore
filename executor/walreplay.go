package executor

import (
	"fmt"
	"github.com/alpacahq/marketstore/v4/executor/wal"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
	goio "io"
	"path/filepath"
	"sort"
)

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
	needsReplay, err := wf.NeedsReplay()
	if err != nil {
		return fmt.Errorf("check if walfile needs to be replayed: %w", err)
	}
	if !needsReplay {
		err := fmt.Errorf("WALFileType.NeedsReplay No Replay Needed")
		log.Info(err.Error())
		return err
	}

	// Take control of this file and set the status
	if writeData {
		wf.WriteStatus(wal.OPEN, wal.REPLAYINPROCESS)
	}

	// First pass of WAL Replay: determine transaction states and record locations of TG data
	txnStateWAL := make(map[int64]TxnStatusEnum)
	txnStatePrimary := make(map[int64]TxnStatusEnum)
	offsetTGDataInWAL := make(map[int64]int64)

	fullRead := func(err error) bool {
		// Check to see if we have read only partial data
		if err != nil {
			if _, ok := err.(wal.ShortReadError); ok {
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
					for tgid := range TGData {
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
			_, _, _, err := wal.ReadStatus(wf.FilePtr)
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
				rootDir := filepath.Dir(wf.FilePtr.Name())
				tgID, wtSets := ParseTGData(TG_Serialized, rootDir)
				if err := wf.replayTGData(tgID, wtSets); err != nil {
					return err
				}
			}
		}
	}
	log.Info("Replay of WAL file %s finished", wf.FilePtr.Name())
	if writeData {
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
	defer cfp.Close()

	for _, wtSet := range wtSets {
		fp, err := cfp.GetFP(wtSet.FilePath)
		if err != nil {
			return err
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
			return fmt.Errorf("Error: Record Type is incorrect from WALFile, invalid/outdated WAL file?")
		}
	}
	wf.lastCommittedTGID = tgID
	wf.CreateCheckpoint()

	return nil
}
