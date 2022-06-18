package di

import (
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/plugins/trigger"
	"github.com/alpacahq/marketstore/v4/sqlparser"
	"os"
	"path/filepath"
	"time"

	"github.com/alpacahq/marketstore/v4/catalog"
	"github.com/alpacahq/marketstore/v4/frontend"
	"github.com/alpacahq/marketstore/v4/replication"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/log"
	"google.golang.org/grpc"
)

type Container struct {
	mktsConfig            *utils.MktsConfig
	absRootDir            string
	instanceID            int64
	gRPCServerOptions     []grpc.ServerOption
	replicationSender     *replication.Sender
	replicationClient     *replication.Retryer
	writer                frontend.Writer
	catalogDir            *catalog.Directory
	wal                   *executor.WALFileType
	tpd                   *executor.TriggerPluginDispatcher
	triggerMatchers       []*trigger.Matcher
	aggRunner             *sqlparser.AggRunner
	grpcService           *frontend.GRPCService
	grpcServer            *grpc.Server
	httpService           *frontend.QueryService
	httpServer            *frontend.RPCServer
	replicationServer     *replication.GRPCReplicationServer
	grpcReplicationServer *grpc.Server
}

func NewContainer(cfg *utils.MktsConfig) *Container {
	return &Container{mktsConfig: cfg}
}

func (c *Container) GetAbsRootDir() string {
	if c.absRootDir != "" {
		return c.absRootDir
	}
	relRootDir := c.mktsConfig.RootDirectory

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
			panic(err)
		}
	}
	c.absRootDir = rootDir
	return c.absRootDir
}

func (c *Container) GetInitInstanceID() int64 {
	if c.instanceID != 0 {
		return c.instanceID
	}
	c.instanceID = time.Now().UTC().UnixNano()
	return c.instanceID
}
