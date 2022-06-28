package di

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/executor/wal"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

func (c *Container) GetInitWALFile() *executor.WALFileType {
	if !c.mktsConfig.InitWALCache {
		return nil
	}
	if c.wal != nil {
		return c.wal
	}

	// initialize TransactionPipe
	txnPipe := executor.NewTransactionPipe()

	walWG := &sync.WaitGroup{}

	// initialize WAL File
	walfile, err := executor.NewWALFile(c.GetAbsRootDir(), c.GetInitInstanceID(), c.GetReplicationSender(),
		c.mktsConfig.WALBypass, walWG, c.GetStartTriggerPluginDispatcher(), txnPipe,
	)
	if err != nil {
		log.Error("Unable to create WAL. err=" + err.Error())
		panic(fmt.Sprintf("unable to create WAL: %v", err))
	}

	if !walfile.WALBypass {
		// ignoreFile := filepath.Base(ThisInstance.WALFile.FilePtr.Name())
		ignoreFile := walfile.FilePtr.Name()
		myInstanceID := walfile.OwningInstanceID

		finder := wal.NewFinder(os.ReadDir)
		walFileAbsPaths, err := finder.Find(filepath.Clean(c.GetAbsRootDir()))
		if err != nil {
			walFileAbsPaths = []string{}
			log.Error("failed to find wal files under %s: %w", filepath.Clean(c.GetAbsRootDir()), err)
		}

		c := executor.NewWALCleaner(ignoreFile, myInstanceID)
		err = c.CleanupOldWALFiles(walFileAbsPaths)
		if err != nil {
			log.Error("Unable to startup Cache and WAL:" + err.Error())
			panic(fmt.Sprintf("unable to startup Cache and WAL:%v", err))
		}
	}
	if c.mktsConfig.BackgroundSync {
		// Startup the WAL and Primary cache flushers
		const (
			defaultWalSyncInterval            = 500 * time.Millisecond
			defaultPrimaryDiskRefreshInterval = 5 * time.Minute
		)
		go walfile.SyncWAL(defaultWalSyncInterval, defaultPrimaryDiskRefreshInterval, c.mktsConfig.WALRotateInterval)
		walfile.IncrementWaitGroup()
	}

	c.wal = walfile
	return c.wal
}
