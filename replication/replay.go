package replication

import (
	"fmt"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/executor/wal"
	"github.com/pkg/errors"
	"strings"
)

//// Writer is the intereface to decouple WAL Replayer from executor package
//type Writer interface {
//	WriteBufferToFile(fp stdio.WriterAt, offsetIndexBuffer []byte) error
//	// for variable length record
//	WriteBufferToFileIndirect(fp *os.File, offsetIndexBuffer []byte, varRecLen int32) (err error)
//}
//
//type WALReplayer struct {
//	Writer Writer
//}

func replay(writeCommands []*wal.WriteCommand) error {
	fmt.Println("received!")
	println(writeCommands)

	// レコードの中に新しい年が入っていた場合はその年のフォルダを追加する
	if err := w.AddNewYearFile(year); err != nil {
		panic(err)
	}

	// まだバケット用のファイルができていない場合は作る
	if err := cDir.AddTimeBucket(&tbk, tbi); err != nil {
		// If File Exists error, ignore it, otherwise return the error
		if !strings.Contains(err.Error(), "Can not overwrite file") && !strings.Contains(err.Error(), "file exists") {
			return err
		}
	}

	err := executor.ThisInstance.WALFile.FlushCommandsToWAL(executor.ThisInstance.TXNPipe, writeCommands, executor.ThisInstance.WALBypass)
	if err != nil {
		return errors.Wrap(err, "[replica] failed to flush WriteCommands to WAL and primary store.")
	}

	return nil
}

//
//func (wr *WALReplayer) ReplayMessage(druRun bool) {
//	/*
//		Replay this WAL File's unwritten transactions.
//		We will do this in two passes, in the first pass we will collect the Transaction Group IDs that are
//		not yet durably written to the primary store. In the second pass, we write the data into the
//		Primary Store directly and then flush the results.
//		Finally we close the WAL File and mark it completely written.
//
//		1) First WAL Pass: Locate unwritten TGIDs
//		2) Second WAL Pass: Load the open TG data into the Primary Data files
//		3) Flush the TG Cache to primary and mark this WAL File completely processed
//
//		Note that the TG Data for any given TGID should appear in the WAL only once. We verify it in the first
//		pass.
//	*/
//
//	// Take control of this file and set the status
//	if writeData {
//		wf.WriteStatus(OPEN, REPLAYINPROCESS)
//	}
//
//	// First pass of WAL Replay: determine transaction states and record locations of TG data
//	txnStateWAL := make(map[int64]TxnStatusEnum, 0)
//	txnStatePrimary := make(map[int64]TxnStatusEnum, 0)
//	offsetTGDataInWAL := make(map[int64]int64, 0)
//
//	fullRead := func(err error) bool {
//		// Check to see if we have read only partial data
//		if err != nil {
//			if _, ok := err.(ShortReadError); ok {
//				log.Info("Partial Read")
//				return false
//			} else {
//				log.Fatal(io.GetCallerFileContext(0) + ": Uncorrectable IO error in WAL Replay")
//			}
//		}
//		return true
//	}
//	log.Info("Beginning WAL Replay")
//	if !writeData {
//		log.Info("Debugging mode enabled - no writes will be performed...")
//	}
//	// Create a map to store the TG Data prior to replay
//	TGData := make(map[int64][]byte)
//
//	wf.FilePtr.Seek(0, goio.SeekStart)
//	continueRead := true
//	for continueRead {
//		MID, err := wf.readMessageID()
//		if continueRead = fullRead(err); !continueRead {
//			break // Break out of read loop
//		}
//		switch MID {
//		case TGDATA:
//			// Read a TGData
//			offset, _ := wf.FilePtr.Seek(0, goio.SeekCurrent)
//			TGID, TG_Serialized, err := wf.readTGData()
//			TGData[TGID] = TG_Serialized
//			if continueRead = fullRead(err); !continueRead {
//				break // Break out of switch
//			}
//			// Throw FATAL if there is already a TG data location in this WAL
//			if _, ok := offsetTGDataInWAL[TGID]; ok {
//				log.Fatal(io.GetCallerFileContext(0) + ": Duplicate TG Data in WAL")
//			}
//			//			log.Info("Successfully read past TG data for TGID: %v", TGID)
//			// Save the offset of this TG Data for the second pass
//			offsetTGDataInWAL[TGID] = offset
//		case TXNINFO:
//			// Read a TXNInfo
//			TGID, destination, txnStatus, err := wf.readTransactionInfo()
//			if continueRead = fullRead(err); !continueRead {
//				break // Break out of switch
//			}
//			switch destination {
//			case WAL:
//				txnStateWAL[TGID] = txnStatus
//			case CHECKPOINT:
//				if _, ok := TGData[TGID]; ok && txnStatus == COMMITCOMPLETE {
//					// Remove all TGData for TGID less than this complete one
//					for tgid, _ := range TGData {
//						if tgid <= TGID {
//							TGData[tgid] = nil
//							delete(TGData, tgid)
//						}
//					}
//				} else {
//					// Record this txnStatus for later analysis
//					txnStatePrimary[TGID] = txnStatus
//				}
//			}
//		case STATUS:
//			// Read the status - note that this message should only be at the file beginning
//			_, _, _, err := wf.ReadStatus()
//			if continueRead = fullRead(err); !continueRead {
//				break // Break out of switch
//			}
//		default:
//			log.Warn("Unknown meessage id %d", MID)
//		}
//	}
//
//	// Second Pass of WAL Replay: Find any pending transactions based on the state and load the TG data into cache
//	log.Info("Entering replay of TGData")
//	// We need to replay TGs in descending TGID order
//
//	// StringSlice attaches the methods of Interface to []string, sorting in increasing order.
//
//	var sortedTGIDs TGIDlist
//	for tgid := range TGData {
//		sortedTGIDs = append(sortedTGIDs, tgid)
//	}
//	sort.Sort(sortedTGIDs)
//
//	//for tgid, TG_Serialized := range TGData {
//	for _, tgid := range sortedTGIDs {
//		TG_Serialized := TGData[tgid]
//		if TG_Serialized != nil {
//			// Note that only TG data that did not have a COMMITCOMPLETE record are replayed
//			if writeData {
//				log.Info("Replaying TGID: %d, data length is: %d bytes", tgid, len(TG_Serialized))
//				if err := wf.replayTGData(TG_Serialized); err != nil {
//					return err
//				}
//			} else {
//				log.Info("Replay for TGID: %d, data length is: %d bytes", tgid, len(TG_Serialized))
//			}
//		}
//	}
//	log.Info("Replay of WAL file %s finished", wf.FilePath)
//	if writeData {
//		wf.WriteStatus(OPEN, REPLAYED)
//	}
//
//	log.Info("Finished replay of TGData")
//	return nil
//}
//
//func (wf *WALFileType) NeedsReplay() bool {
//	wf.syncStatusRead()
//	if wf.ReplayState == NOTREPLAYED || wf.ReplayState == REPLAYINPROCESS {
//		return true
//	}
//	return false
//}
