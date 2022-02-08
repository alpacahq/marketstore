package executor

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/alpacahq/marketstore/v4/catalog"
	"github.com/alpacahq/marketstore/v4/executor/wal"
	"github.com/alpacahq/marketstore/v4/plugins/trigger"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

var ThisInstance *InstanceMetadata

type InstanceMetadata struct {
	CatalogDir *catalog.Directory
	WALFile    *WALFileType
}

type InstanceMetadataOptions struct {
	initCatalog    bool
	initWALCache   bool
	backgroundSync bool
	walBypass      bool
}
type Option func(option *InstanceMetadataOptions)

func InitCatalog(f bool) Option {
	return func(s *InstanceMetadataOptions) {
		s.initCatalog = f
	}
}

func InitWALCache(f bool) Option {
	return func(s *InstanceMetadataOptions) {
		s.initWALCache = f
	}
}

func BackgroundSync(f bool) Option {
	return func(s *InstanceMetadataOptions) {
		s.backgroundSync = f
	}
}

func WALBypass(f bool) Option {
	return func(s *InstanceMetadataOptions) {
		s.walBypass = f
	}
}

func NewInstanceSetup(relRootDir string, rs ReplicationSender, tm []*trigger.TriggerMatcher,
	walRotateInterval int, options ...Option,
) (metadata *InstanceMetadata, shutdownPending *bool, walWG *sync.WaitGroup, err error) {
	// default
	opts := &InstanceMetadataOptions{
		initCatalog:    true,
		initWALCache:   true,
		backgroundSync: true,
		walBypass:      false,
	}
	// apply options
	for _, opt := range options {
		opt(opts)
	}

	log.Info("WAL Setup: initCatalog %v, initWALCache %v, backgroundSync %v, WALBypass %v",
		opts.initCatalog, opts.initWALCache, opts.backgroundSync, WALBypass)

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
		const ownerGroupAll = 0o770
		err = os.Mkdir(rootDir, ownerGroupAll)
		if err != nil && !os.IsExist(err) {
			log.Error("Could not create root directory: %s", err.Error())
			return nil, nil, nil, err
		}
	}
	instanceID := time.Now().UTC().UnixNano()

	// Initialize a global catalog
	if opts.initCatalog {
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
	if opts.initWALCache {
		// initialize TransactionPipe
		txnPipe := NewTransactionPipe()

		// initialize WAL File
		ThisInstance.WALFile, err = NewWALFile(rootDir, instanceID, rs,
			opts.walBypass, &shutdownPend, walWG, tpd, txnPipe,
		)
		if err != nil {
			log.Error("Unable to create WAL. err=" + err.Error())
			return nil, nil, nil, fmt.Errorf("unable to create WAL: %w", err)
		}

		// Allocate a new WALFile and cache
		if !opts.walBypass {
			// ignoreFile := filepath.Base(ThisInstance.WALFile.FilePtr.Name())
			ignoreFile := ThisInstance.WALFile.FilePtr.Name()
			myInstanceID := ThisInstance.WALFile.OwningInstanceID

			finder := wal.NewFinder(os.ReadDir)
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
		if opts.backgroundSync {
			// Startup the WAL and Primary cache flushers
			const (
				defaultWalSyncInterval            = 500 * time.Millisecond
				defaultPrimaryDiskRefreshInterval = 5 * time.Minute
			)
			go ThisInstance.WALFile.SyncWAL(defaultWalSyncInterval, defaultPrimaryDiskRefreshInterval,
				walRotateInterval,
			)
			walWG.Add(1)
		}
	}
	return ThisInstance, &shutdownPend, walWG, nil
}
