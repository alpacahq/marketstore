package executor

import (
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/alpacahq/marketstore/v4/catalog"
	"github.com/alpacahq/marketstore/v4/plugins/trigger"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

var ThisInstance *InstanceMetadata

type InstanceMetadata struct {
	// RootDir is the absolute path to the data directory
	RootDir         string
	CatalogDir      *catalog.Directory
	TXNPipe         *TransactionPipe
	WALFile         *WALFileType
	TriggerMatchers []*trigger.TriggerMatcher
}

func NewInstanceSetup(relRootDir string, rs ReplicationSender, walRotateInterval int, options ...bool,
) (metadata *InstanceMetadata, shutdownPending *bool, walWG *sync.WaitGroup) {
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

	var err error
	rootDir, err := filepath.Abs(filepath.Clean(relRootDir))
	if err != nil {
		log.Error("Cannot take absolute path of root directory %s", err.Error())
	} else {
		log.Info("Root Directory: %s", rootDir)
		err = os.Mkdir(rootDir, 0770)
		if err != nil && !os.IsExist(err) {
			log.Fatal("Could not create root directory: %s", err.Error())
		}
	}
	instanceID := time.Now().UTC().UnixNano()
	ThisInstance.RootDir = rootDir

	// Initialize a global catalog
	if initCatalog {
		ThisInstance.CatalogDir = catalog.NewDirectory(rootDir)
	}

	shutdownPend := false
	walWG = &sync.WaitGroup{}
	if initWALCache {
		// Allocate a new WALFile and cache
		if WALBypass {
			ThisInstance.TXNPipe = NewTransactionPipe()
			ThisInstance.WALFile, err = NewWALFile(rootDir, instanceID, nil, WALBypass,
				&shutdownPend, walWG,
			)
			if err != nil {
				log.Fatal("Unable to create WAL")
			}
		} else {
			ThisInstance.TXNPipe, ThisInstance.WALFile, err = StartupCacheAndWAL(rootDir, instanceID, rs,
				WALBypass, &shutdownPend, walWG,
			)

			if err != nil {
				log.Fatal("Unable to startup Cache and WAL")
			}
		}
		if backgroundSync {
			// Startup the WAL and Primary cache flushers
			go ThisInstance.WALFile.SyncWAL(500*time.Millisecond, 5*time.Minute, walRotateInterval)
			walWG.Add(1)
		}
	}
	return ThisInstance, &shutdownPend, walWG
}
