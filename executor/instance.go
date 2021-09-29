package executor

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/alpacahq/marketstore/v4/executor/wal"

	"github.com/pkg/errors"

	"github.com/alpacahq/marketstore/v4/catalog"
	"github.com/alpacahq/marketstore/v4/plugins/trigger"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

var ThisInstance *InstanceMetadata

type InstanceMetadata struct {
	CatalogDir *catalog.Directory
	WALFile    *WALFileType
}

func NewInstanceSetup(relRootDir string, rs ReplicationSender, tm []*trigger.TriggerMatcher,
	walRotateInterval int, options ...bool,
) (metadata *InstanceMetadata, shutdownPending *bool, walWG *sync.WaitGroup, err error) {
	/*
		Defaults
	*/
	initCatalog, initWALCache, backgroundSync, WALBypass := true, true, true, false
	switch {
	case len(options) >= 4:
		WALBypass = options[3]
		fallthrough
	case len(options) == 3:
		backgroundSync = options[2]
		fallthrough
	case len(options) == 2:
		initWALCache = options[1]
		fallthrough
	case len(options) == 1:
		initCatalog = options[0]
	}
	log.Info("WAL Setup: initCatalog %v, initWALCache %v, backgroundSync %v, WALBypass %v: \n",
		initCatalog, initWALCache, backgroundSync, WALBypass)

	if ThisInstance == nil {
		ThisInstance = new(InstanceMetadata)
	}

	// rootDir is the absolute path to the data directory.
	// e.g. rootDir = "/project/marketstore/data"
	rootDir, err := filepath.Abs(filepath.Clean(relRootDir))
	if err != nil {
		log.Error("Cannot take absolute path of root directory %s", err.Error())
	} else {
		log.Info("Root Directory: %s", rootDir)
		err = os.Mkdir(rootDir, 0770)
		if err != nil && !os.IsExist(err) {
			log.Error("Could not create root directory: %s", err.Error())
			return nil, nil, nil, err
		}
	}
	instanceID := time.Now().UTC().UnixNano()

	// Initialize a global catalog
	if initCatalog {
		ThisInstance.CatalogDir, err = catalog.NewDirectory(rootDir)
		if err != nil {
			var e catalog.ErrCategoryFileNotFound
			if errors.As(err, &e) {
				log.Debug("new root directory found:" + rootDir)
			} else {
				log.Error("Could not create a catalog directory: %s.", err.Error())
				return nil, nil, nil, err
			}
		}
	}

	// read Trigger plugin matchers
	tpd := NewTriggerPluginDispatcher(tm)

	shutdownPend := false
	walWG = &sync.WaitGroup{}
	if initWALCache {
		// initialize TransactionPipe
		txnPipe := NewTransactionPipe()

		// initialize WAL File
		ThisInstance.WALFile, err = NewWALFile(rootDir, instanceID, rs,
			WALBypass, &shutdownPend, walWG, tpd, txnPipe,
		)
		if err != nil {
			log.Error("Unable to create WAL. err=" + err.Error())
			return nil, nil, nil, fmt.Errorf("unable to create WAL: %w", err)
		}

		// Allocate a new WALFile and cache
		if !WALBypass {
			//ignoreFile := filepath.Base(ThisInstance.WALFile.FilePtr.Name())
			ignoreFile := ThisInstance.WALFile.FilePtr.Name()
			myInstanceID := ThisInstance.WALFile.OwningInstanceID

			finder := wal.NewFinder(ioutil.ReadDir)
			walFileAbsPaths, err := finder.Find(filepath.Clean(rootDir))
			if err != nil {
				walFileAbsPaths = []string{}
				log.Error("failed to find wal files under %s: %w", filepath.Clean(rootDir), err)
			}

			c := NewWALCleaner(ignoreFile, myInstanceID)
			err = c.CleanupOldWALFiles(walFileAbsPaths)
			if err != nil {
				log.Error("Unable to startup Cache and WAL:" + err.Error())
				return nil, nil, nil,
					fmt.Errorf("unable to startup Cache and WAL:%w", err)
			}
		}
		if backgroundSync {
			// Startup the WAL and Primary cache flushers
			go ThisInstance.WALFile.SyncWAL(500*time.Millisecond, 5*time.Minute, walRotateInterval)
			walWG.Add(1)
		}
	}
	return ThisInstance, &shutdownPend, walWG, nil
}
