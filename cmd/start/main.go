package start

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"runtime/pprof"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/frontend"
	"github.com/alpacahq/marketstore/frontend/stream"
	"github.com/alpacahq/marketstore/utils"
	"github.com/alpacahq/marketstore/utils/log"
	"github.com/spf13/cobra"
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

	// Spawn a goroutine and listen for a signal.
	signalChan := make(chan os.Signal)
	go func() {
		for s := range signalChan {
			switch s {
			case syscall.SIGUSR1:
				log.Info("dumping stack traces due to SIGUSR1 request")
				pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
			case syscall.SIGINT:
				log.Info("initiating graceful shutdown due to SIGINT request")
				atomic.StoreUint32(&frontend.Queryable, uint32(0))
				log.Info("waiting a grace period of %v to shutdown...", utils.InstanceConfig.StopGracePeriod)
				time.Sleep(utils.InstanceConfig.StopGracePeriod)
				shutdown()
			}
		}
	}()
	signal.Notify(signalChan, syscall.SIGUSR1)
	signal.Notify(signalChan, syscall.SIGINT)

	// Initialize marketstore services.
	// --------------------------------
	log.Info("initializing marketstore...")

	//
	executor.NewInstanceSetup(
		utils.InstanceConfig.RootDirectory,
		utils.InstanceConfig.InitCatalog,
		utils.InstanceConfig.InitWALCache,
		utils.InstanceConfig.BackgroundSync,
		utils.InstanceConfig.WALBypass)

	// New server.
	server, _ := frontend.NewServer()

	// Set rpc handler.
	log.Info("launching rpc data server...")
	go http.Handle("/rpc", server)

	// Set websocket handler.
	log.Info("initializing websocket...")
	stream.Initialize()
	go http.HandleFunc("/ws", stream.Handler)

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
