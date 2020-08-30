package executor

import (
	"context"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"path/filepath"
	"sync"
	"time"

	"github.com/alpacahq/marketstore/v4/catalog"
	"github.com/alpacahq/marketstore/v4/plugins/trigger"
	"github.com/alpacahq/marketstore/v4/replication"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

var ThisInstance *InstanceMetadata

type InstanceMetadata struct {
	RootDir         string
	CatalogDir      *catalog.Directory
	TXNPipe         *TransactionPipe
	WALFile         *WALFileType
	WALWg           sync.WaitGroup
	ShutdownPending bool
	WALBypass       bool
	TriggerMatchers []*trigger.TriggerMatcher
	Replicator      replication.Sender
}

func NewInstanceSetup(ctx context.Context, relRootDir string, grpcServer *grpc.Server, options ...bool) {
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
	log.Info("Root Directory: %s", relRootDir)
	rootDir, err := filepath.Abs(filepath.Clean(relRootDir))
	if err != nil {
		log.Error("Cannot take absolute path of root directory %s", err.Error())
	}
	instanceID := time.Now().UTC().UnixNano()
	ThisInstance.RootDir = rootDir
	// Initialize a global catalog
	if initCatalog {
		ThisInstance.CatalogDir = catalog.NewDirectory(rootDir)
	}
	ThisInstance.WALBypass = WALBypass
	if initWALCache {
		// Allocate a new WALFile and cache
		if WALBypass {
			ThisInstance.TXNPipe = NewTransactionPipe()
			ThisInstance.WALFile, err = NewWALFile(ThisInstance.RootDir, instanceID, nil)
			if err != nil {
				log.Fatal("Unable to create WAL")
			}
		} else {
			// initialize replication master or client
			var rs *replication.Sender
			if utils.InstanceConfig.Replication.Enabled {
				rs = initReplicationMaster(ctx, grpcServer)
				log.Info("initialized replication master")
			} else if utils.InstanceConfig.Replication.MasterHost != "" {
				err = initReplicationClient(ctx)
				if err != nil {
					log.Fatal("Unable to startup Replication", err)
				}
				log.Info("initialized replication client")
			}

			//walReceiver := replication.NewReceiver()

			ThisInstance.TXNPipe, ThisInstance.WALFile, err = StartupCacheAndWAL(ThisInstance.RootDir, instanceID, rs)

			if err != nil {
				log.Fatal("Unable to startup Cache and WAL")
			}
		}
		if backgroundSync {
			// Startup the WAL and Primary cache flushers
			go ThisInstance.WALFile.SyncWAL(500*time.Millisecond, 1*time.Minute, 1)
			ThisInstance.WALWg.Add(1)
		}
	}
}

func initReplicationMaster(ctx context.Context, grpcServer *grpc.Server) *replication.Sender {
	grpcReplicationServer, err := replication.NewGRPCReplicationService(
		grpcServer,
		utils.InstanceConfig.Replication.ListenPort,
	)
	if err != nil {
		log.Fatal("Unable to startup gRPC server for replication")
	}
	replicationSender := replication.NewSender(grpcReplicationServer)
	replicationSender.Run(ctx)

	return replicationSender
}

func initReplicationClient(ctx context.Context) error {
	c, err := replication.NewGRPCReplicationClient(utils.InstanceConfig.Replication.MasterHost, false)
	if err != nil {
		return errors.Wrap(err, "failed to initialize gRPC client for replication")
	}

	// TODO: implement TLS between master and replica
	replicationReceiver := replication.NewReceiver(c)
	err = replicationReceiver.Run(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to connect Master instance from Replica")
	}

	return nil
}
