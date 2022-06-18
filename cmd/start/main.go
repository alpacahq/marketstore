package start

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime/pprof"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/alpacahq/marketstore/v4/internal/di"

	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/frontend"
	"github.com/alpacahq/marketstore/v4/frontend/stream"
	"github.com/alpacahq/marketstore/v4/metrics"
	pb "github.com/alpacahq/marketstore/v4/proto"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/log"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
)

const (
	usage                 = "start"
	short                 = "Start a marketstore database server"
	long                  = "This command starts a marketstore database server"
	example               = "marketstore start --config <path>"
	defaultConfigFilePath = "./mkts.yml"
	configDesc            = "set the path for the marketstore YAML configuration file"

	diskUsageMonitorInterval = 10 * time.Minute
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

// nolint:gochecknoinits // cobra's standard way to initialize flags
func init() {
	utils.InstanceConfig.StartTime = time.Now()
	Cmd.Flags().StringVarP(&configFilePath, "config", "c", defaultConfigFilePath, configDesc)
}

// executeStart implements the start command.
func executeStart(cmd *cobra.Command, _ []string) error {
	ctx := context.Background()
	globalCtx, globalCancel := context.WithCancel(ctx)
	defer globalCancel()

	// Attempt to read config file.
	data, err := os.ReadFile(configFilePath)
	if err != nil {
		return fmt.Errorf("failed to read configuration file error: %w", err)
	}

	// Don't output command usage if args(=only the filepath to mkts.yml at the moment) are correct
	cmd.SilenceUsage = true

	// Log config location.
	log.Info("using %v for configuration", configFilePath)

	// Attempt to set configuration.
	config, err := utils.ParseConfig(data)
	if err != nil {
		return fmt.Errorf("failed to parse configuration file error: %w", err)
	}
	utils.InstanceConfig = *config // TODO: remove the singleton instance

	// New gRPC stream server for replication.
	c := di.NewContainer(config)
	// initialize replication master or client
	c.GetReplicationSender().Run(ctx)
	// start TriggerPluginDispatcher
	c.GetStartTriggerPluginDispatcher()

	// Initialize marketstore services.
	// --------------------------------
	log.Info("initializing marketstore...")

	start := time.Now()

	executor.NewInstanceSetup(c.GetCatalogDir(), c.GetInitWALFile())
	if err != nil {
		return fmt.Errorf("craete new instance setup: %w", err)
	}

	go metrics.StartDiskUsageMonitor(metrics.TotalDiskUsageBytes, config.RootDirectory, diskUsageMonitorInterval)

	startupTime := time.Since(start)
	metrics.StartupTime.Set(startupTime.Seconds())
	log.Info("startup time: %s", startupTime)

	if replicationCli := c.GetReplicationClientWithRetry(); replicationCli != nil {
		err = replicationCli.Run(globalCtx)
		if err != nil {
			log.Error("Unable to startup Replication", err)
			return err
		}
		log.Info("initialized replication client")
	}
	// register grpc server
	pb.RegisterMarketstoreServer(c.GetGRPCServer(), c.GetGRPCService())

	// Set rpc handler.
	log.Info("launching rpc data server...")
	http.Handle("/rpc", c.GetHttpServer())

	// Set websocket handler.
	log.Info("initializing websocket...")
	stream.Initialize()
	http.HandleFunc("/ws", stream.Handler)

	// Set monitoring handler.
	log.Info("launching prometheus metrics server...")
	http.Handle("/metrics", promhttp.Handler())

	// Initialize any provided bgWorker plugins.
	RunBgWorkers(config.BgWorkers)

	if config.UtilitiesURL != "" {
		// Start utility endpoints.
		log.Info("launching utility service...")
		uah := frontend.NewUtilityAPIHandlers(config.StartTime)
		go func() {
			err = uah.Handle(config.UtilitiesURL)
			if err != nil {
				log.Error("utility API handle error: %v", err.Error())
			}
		}()
	}

	log.Info("enabling query access...")
	atomic.StoreUint32(&frontend.Queryable, 1)

	// Serve.
	log.Info("launching tcp listener for all services...")
	if config.GRPCListenURL != "" {
		grpcLn, err2 := net.Listen("tcp", config.GRPCListenURL)
		if err2 != nil {
			return fmt.Errorf("failed to start GRPC server - error: %w", err2)
		}
		go func() {
			err3 := c.GetGRPCServer().Serve(grpcLn)
			if err3 != nil {
				log.Error("gRPC server error: %v", err.Error())
				c.GetGRPCServer().GracefulStop()
			}
		}()
	}

	// Spawn a goroutine and listen for a signal.
	const defaultSignalChanLen = 10
	signalChan := make(chan os.Signal, defaultSignalChanLen)
	go func() {
		for s := range signalChan {
			switch s {
			case syscall.SIGUSR1:
				log.Info("dumping stack traces due to SIGUSR1 request")
				err2 := pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
				if err2 != nil {
					log.Error("failed to write goroutine pprof: %w", err)
					return
				}
			case syscall.SIGINT, syscall.SIGTERM:
				log.Info("initiating graceful shutdown due to '%v' request", s)
				c.GetGRPCServer().GracefulStop()
				log.Info("shutdown grpc API server...")
				globalCancel()
				if c.GetGRPCReplicationServer() != nil {
					c.GetGRPCReplicationServer().Stop() // gRPC stream connection doesn't close by GracefulStop()
				}
				log.Info("shutdown grpc Replication server...")

				atomic.StoreUint32(&frontend.Queryable, uint32(0))
				log.Info("waiting a grace period of %v to shutdown...", config.StopGracePeriod)
				time.Sleep(config.StopGracePeriod)
				c.GetInitWALFile().Shutdown()
				shutdown()
			}
		}
	}()
	signal.Notify(signalChan, syscall.SIGUSR1, syscall.SIGINT, syscall.SIGTERM)

	if err := http.ListenAndServe(config.ListenURL, nil); err != nil {
		return fmt.Errorf("failed to start server - error: %w", err)
	}

	return nil
}

func shutdown() {
	log.Info("exiting...")
	os.Exit(0)
}
