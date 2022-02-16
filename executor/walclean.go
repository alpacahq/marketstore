package executor

import (
	"errors"
	"fmt"
	"os"

	"github.com/alpacahq/marketstore/v4/executor/wal"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

const (
	// For the details of wal file format, see docs/design/durable_writes_design.txt.
	fileStatusLenBytes  = 1
	replayStateLenBytes = 1
	owningPIDLenBytes   = 8
	walStatusLenBytes   = fileStatusLenBytes + replayStateLenBytes + owningPIDLenBytes
)

type WALCleaner struct {
	ignoreFile   string
	myInstanceID int64
}

func NewWALCleaner(ignoreFile string, myInstanceID int64) *WALCleaner {
	return &WALCleaner{
		ignoreFile:   ignoreFile,
		myInstanceID: myInstanceID,
	}
}

func (c *WALCleaner) CleanupOldWALFiles(walfileAbsPaths []string) error {
	for _, fp := range walfileAbsPaths {
		if fp == c.ignoreFile {
			continue
		}

		log.Info("Found a WALFILE: %s, entering replay...", fp)
		fi, err := os.Stat(fp)
		if err != nil {
			log.Error("failed to get fileStat of " + fp)
			continue
		}
		if fi.Size() <= walStatusLenBytes { // The first message in a WAL file is always the WAL Status Message
			log.Info("WALFILE: %s is empty, removing it...", fp)
			err = os.Remove(fp)
			if err != nil {
				log.Error("failed to remove an empty WALfile", fp)
			}
			continue
		}

		w, err := TakeOverWALFile(fp)
		if err != nil {
			return fmt.Errorf("opening %s: %w", fp, err)
		}
		if err = w.Replay(false); err != nil {
			// ---  move walfile to a temporary file and skip replay to continue other marketstore process
			var walReplayErr wal.ReplayError
			if !errors.As(err, &walReplayErr) {
				return fmt.Errorf("unable to replay %s: %w", fp, err)
			}
			if walReplayErr.Cont {
				tmpFP := fp + ".tmp"
				if err2 := wal.Move(fp, tmpFP); err2 != nil {
					return fmt.Errorf("failed to move old wal file %s to a tmp file:%w", fp, err2)
				}
				log.Info(fmt.Sprintf("Unable to replay. moved an old WAL file %s to a temporary file %s",
					fp, tmpFP))
			}

			continue
		}

		// delete if replay succeeds
		// if err = w.Delete(wf.OwningInstanceID); err != nil {
		if err = w.Delete(c.myInstanceID); err != nil {
			return fmt.Errorf("failed to delete wal file after replay:%w", err)
		}
	}
	return nil
}
