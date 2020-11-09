package start

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/alpacahq/marketstore/v4/replication"
	"github.com/pkg/errors"
	"google.golang.org/grpc/credentials"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime/pprof"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/frontend"
	"github.com/alpacahq/marketstore/v4/frontend/stream"
	pb "github.com/alpacahq/marketstore/v4/proto"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/log"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

const (
	usage                 = "start"
	short                 = "Start a marketstore database server"
	long                  = "This command starts a marketstore database server"
	example               = "marketstore start --config <path>"
	defaultConfigFilePath = "./mkts.yml"
	configDesc            = "set the path for the marketstore YAML configuration file"
)

var (
	// Cmd is the start command.
	Cmd = &cobra.Command{
		Use:        usage,
		Short:      short,
		Long:       long,
		Aliases:    []string{"s"},
		SuggestFor: []string{"boot", "up"},
		Example:    example,
		RunE:       executeStart,
	}
	// configFilePath set flag for a path to the config file.
	configFilePath string
)

func init() {
	utils.InstanceConfig.StartTime = time.Now()
	Cmd.Flags().StringVarP(&configFilePath, "config", "c", defaultConfigFilePath, configDesc)
}

// executeStart implements the start command.
func executeStart(cmd *cobra.Command, args []string) error {
	ctx := context.Background()
	globalCtx, globalCancel := context.WithCancel(ctx)

	// Attempt to read config file.
	data, err := ioutil.ReadFile(configFilePath)
	if err != nil {
		return fmt.Errorf("failed to read configuration file error: %s", err.Error())
	}

	// Log config location.
	log.Info("using %v for configuration", configFilePath)

	// Attempt to set configuration.
	err = utils.InstanceConfig.Parse(data)
	if err != nil {
		return fmt.Errorf("failed to parse configuration file error: %v", err.Error())
	}

	// New grpc server for marketstore API.
	grpcServer := grpc.NewServer(
		grpc.MaxSendMsgSize(utils.InstanceConfig.GRPCMaxSendMsgSize),
		grpc.MaxRecvMsgSize(utils.InstanceConfig.GRPCMaxRecvMsgSize),
	)
	pb.RegisterMarketstoreServer(grpcServer, frontend.GRPCService{})

	// New gRPC stream server for replication.
	opts := []grpc.ServerOption{
		grpc.MaxSendMsgSize(utils.InstanceConfig.GRPCMaxSendMsgSize),
		grpc.MaxRecvMsgSize(utils.InstanceConfig.GRPCMaxRecvMsgSize),
	}

	// Initialize marketstore services.
	// --------------------------------
	log.Info("initializing marketstore...")

	// initialize replication master or client
	var rs executor.ReplicationSender
	var grpcReplicationServer *grpc.Server
	if utils.InstanceConfig.Replication.Enabled {
		// Enable TLS for all incoming connections if configured
		if utils.InstanceConfig.Replication.TLSEnabled {
			cert, err := tls.LoadX509KeyPair(
				utils.InstanceConfig.Replication.CertFile,
				utils.InstanceConfig.Replication.KeyFile,
			)
			if err != nil {
				return fmt.Errorf("failed to load server certificates for replication:"+
					" certFile:%v, keyFile:%v, err:%v",
					utils.InstanceConfig.Replication.CertFile,
					utils.InstanceConfig.Replication.KeyFile,
					err.Error(),
				)
			}
			opts = append(opts, grpc.Creds(credentials.NewServerTLSFromCert(&cert)))
			log.Debug("transport security is enabled on gRPC server for replication")
		}

		grpcReplicationServer = grpc.NewServer(opts...)
		rs = initReplicationMaster(globalCtx, grpcReplicationServer)
		log.Info("initialized replication master")
	} else if utils.InstanceConfig.Replication.MasterHost != "" {
		err = initReplicationClient(
			globalCtx,
			utils.InstanceConfig.Replication.TLSEnabled,
			utils.InstanceConfig.Replication.CertFile)
		if err != nil {
			log.Fatal("Unable to startup Replication", err)
		}
		log.Info("initialized replication client")
	}

	executor.NewInstanceSetup(
		utils.InstanceConfig.RootDirectory,
		rs,
		utils.InstanceConfig.InitCatalog,
		utils.InstanceConfig.InitWALCache,
		utils.InstanceConfig.BackgroundSync,
		utils.InstanceConfig.WALBypass,
	)

	// New server.
	server, _ := frontend.NewServer()

	// Set rpc handler.
	log.Info("launching rpc data server...")
	http.Handle("/rpc", server)

	// Set websocket handler.
	log.Info("initializing websocket...")
	stream.Initialize()
	http.HandleFunc("/ws", stream.Handler)

	// Set monitoring handler.
	log.Info("launching prometheus metrics server...")
	http.Handle("/metrics", promhttp.Handler())

	// Initialize any provided plugins.
	InitializeTriggers()
	RunBgWorkers()

	if utils.InstanceConfig.UtilitiesURL != "" {
		// Start utility endpoints.
		log.Info("launching utility service...")
		go frontend.Utilities(utils.InstanceConfig.UtilitiesURL)
	}

	log.Info("enabling query access...")
	atomic.StoreUint32(&frontend.Queryable, 1)

	// Serve.
	log.Info("launching tcp listener for all services...")
	if utils.InstanceConfig.GRPCListenURL != "" {
		grpcLn, err := net.Listen("tcp", utils.InstanceConfig.GRPCListenURL)
		if err != nil {
			return fmt.Errorf("failed to start GRPC server - error: %s", err.Error())
		}
		go func() {
			err := grpcServer.Serve(grpcLn)
			if err != nil {
				grpcServer.GracefulStop()
			}
		}()
	}

	// Spawn a goroutine and listen for a signal.
	signalChan := make(chan os.Signal)
	go func() {
		for s := range signalChan {
			switch s {
			case syscall.SIGUSR1:
				log.Info("dumping stack traces due to SIGUSR1 request")
				pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
			case syscall.SIGINT:
				fallthrough
			case syscall.SIGTERM:
				log.Info("initiating graceful shutdown due to '%v' request", s)
				grpcServer.GracefulStop()
				log.Info("shutdown grpc API server...")
				globalCancel()
				if grpcReplicationServer != nil {
					grpcReplicationServer.Stop() // gRPC stream connection doesn't close by GracefulStop()
				}
				log.Info("shutdown grpc Replication server...")

				atomic.StoreUint32(&frontend.Queryable, uint32(0))
				log.Info("waiting a grace period of %v to shutdown...", utils.InstanceConfig.StopGracePeriod)
				time.Sleep(utils.InstanceConfig.StopGracePeriod)
				shutdown()
			}
		}
	}()
	signal.Notify(signalChan, syscall.SIGUSR1, syscall.SIGINT, syscall.SIGTERM)

	if err := http.ListenAndServe(utils.InstanceConfig.ListenURL, nil); err != nil {
		return fmt.Errorf("failed to start server - error: %s", err.Error())
	}

	return nil
}

func shutdown() {
	executor.ThisInstance.ShutdownPending = true
	executor.ThisInstance.WALWg.Wait()
	log.Info("exiting...")
	os.Exit(0)
}

func initReplicationMaster(ctx context.Context, grpcServer *grpc.Server) *replication.Sender {
	grpcReplicationServer := replication.NewGRPCReplicationService()
	pb.RegisterReplicationServer(grpcServer, grpcReplicationServer)

	// start gRPC server for Replication
	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", utils.InstanceConfig.Replication.ListenPort))
	if err != nil {
		log.Fatal("failed to listen a port for replication:" + err.Error())
	}
	go func() {
		log.Info("starting GRPC server for replication...")
		if err := grpcServer.Serve(lis); err != nil {
			log.Error(fmt.Sprintf("failed to serve replication service:%v", err))
		}
	}()

	replicationSender := replication.NewSender(grpcReplicationServer)
	replicationSender.Run(ctx)

	return replicationSender
}

func initReplicationClient(ctx context.Context, tlsEnabled bool, certFile string) error {
	opts := []grpc.DialOption{
		// grpc.WithBlock(),
	}

	if tlsEnabled {
		creds, err := credentials.NewClientTLSFromFile(certFile, "")
		if err != nil {
			return errors.Wrap(err, "failed to load certFile for replication")
		}

		opts = append(opts, grpc.WithTransportCredentials(creds))
		log.Debug("transport security is enabled on gRPC client for replication")
	} else {
		// transport security is disabled
		opts = append(opts, grpc.WithInsecure())
	}

	conn, err := grpc.Dial(utils.InstanceConfig.Replication.MasterHost, opts...)
	if err != nil {
		return errors.Wrap(err, "failed to initialize gRPC client connection for replication")
	}

	c := replication.NewGRPCReplicationClient(pb.NewReplicationClient(conn))

	replayer := replication.NewReplayer(executor.ParseTGData, executor.WriteCSMInner, utils.InstanceConfig.RootDirectory)
	replicationReceiver := replication.NewReceiver(c, replayer)
	err = replicationReceiver.Run(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to connect Master instance from Replica")
	}

	return nil
}
