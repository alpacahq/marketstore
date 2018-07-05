package start

import (
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
	. "github.com/alpacahq/marketstore/utils/log"
	"github.com/spf13/cobra"
)

const (
	startUsage            = "start"
	startShortDesc        = "Starts a marketstore database server"
	startLongDesc         = "This command starts a marketstore database server.. Lorem ipsum.."
	startExample          = "marketstore start --config PATH_TO_FILE"
	defaultConfigFilePath = "./mkts.yml"
	configDesc            = "Set the path for the marketstore YAML configuration file using --config or -c"
)

var (
	// Cmd is the start command.
	Cmd = &cobra.Command{
		Use:        startUsage,
		Short:      startShortDesc,
		Long:       startLongDesc,
		Aliases:    []string{"s"},
		SuggestFor: []string{"boot", "up"},
		Example:    startExample,
		Run:        executeStart,
	}
	// configFilePath set flag for a path to the config file.
	configFilePath string
)

func init() {
	utils.InstanceConfig.StartTime = time.Now()
	Cmd.Flags().StringVarP(&configFilePath, "config", "c", defaultConfigFilePath, configDesc)
}

// executeStart implements the start command.
func executeStart(cmd *cobra.Command, args []string) {

	// Attempt to read config file.
	data, err := ioutil.ReadFile(configFilePath)
	if err != nil {
		Log(FATAL, "failed to read configuration file - Error: %v", err)
		return
	}

	// Log config location.
	Log(INFO, "using %v for configuration", configFilePath)

	// Attempt to set configuration.
	err = utils.InstanceConfig.Parse(data)
	if err != nil {
		Log(FATAL, "failed to parse configuration file error: %v", err)
		return
	}

	// Spawn a goroutine and listen for a signal.
	signalChan := make(chan os.Signal)
	go func() {
		for s := range signalChan {
			switch s {
			case syscall.SIGUSR1:
				Log(INFO, "dumping stack traces due to SIGUSR1 request")
				pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
			case syscall.SIGINT:
				Log(INFO, "initiating graceful shutdown due to SIGINT request")
				atomic.StoreUint32(&frontend.Queryable, uint32(0))
				Log(INFO, "waiting a grace period of %v to shutdown...", utils.InstanceConfig.StopGracePeriod)
				time.Sleep(utils.InstanceConfig.StopGracePeriod)
				shutdown()
			}
		}
	}()
	signal.Notify(signalChan, syscall.SIGUSR1)
	signal.Notify(signalChan, syscall.SIGINT)

	// Initialize marketstore services.
	// --------------------------------
	Log(INFO, "initializing marketstore...")

	//
	executor.NewInstanceSetup(utils.InstanceConfig.RootDirectory, true, true, true)
	// New server.
	server, _ := frontend.NewServer()

	// Set rpc handler.
	Log(INFO, "launching rpc data server...")
	go http.Handle("/rpc", server)

	// Set websocket handler.
	Log(INFO, "initializing websocket...")
	stream.Initialize()
	go http.HandleFunc("/ws", stream.Handler)

	// Initialize any provided plugins.
	InitializeTriggers()
	RunBgWorkers()

	// Start heartbeat.
	Log(INFO, "launching heartbeat service...")
	go frontend.Heartbeat(utils.InstanceConfig.ListenPort)

	Log(INFO, "enabling query access...")
	atomic.StoreUint32(&frontend.Queryable, 1)

	// Serve.
	// TODO: Not quite sure why no handler is specified..
	Log(INFO, "launching tcp listener for all services...")
	if err := http.ListenAndServe(utils.InstanceConfig.ListenPort, nil); err != nil {
		Log(FATAL, "failed to start server - error: %s", err)
	}
}

func shutdown() {
	executor.ThisInstance.ShutdownPending = true
	executor.ThisInstance.WALWg.Wait()
	Log(INFO, "exiting...")
	os.Exit(0)
}
