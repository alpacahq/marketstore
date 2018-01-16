package main

import (
	"flag"
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
	"github.com/alpacahq/marketstore/utils"
	. "github.com/alpacahq/marketstore/utils/log"
)

// This is the launcher for all marketstore services

func init() {
	utils.InstanceConfig.StartTime = time.Now()
	configFlag := flag.String("config", "mkts_config.yaml", "MarketStore YAML configuration file")
	printVersion := flag.Bool("version", false, "print version string and exits")

	flag.Parse()

	// set logging to output to standard error
	flag.Lookup("logtostderr").Value.Set("true")

	if configFlag != nil {
		data, err := ioutil.ReadFile(*configFlag)
		if err != nil {
			Log(FATAL, "Failed to read configuration file - Error: %v", err)
		}
		err = utils.InstanceConfig.Parse(data)
		if err != nil {
			Log(FATAL, "Failed to parse configuration file - Error: %v", err)
		}
	} else {
		Log(FATAL, "No configuration file provided.")
	}

	sigChannel := make(chan os.Signal)
	go func() {
		for sig := range sigChannel {
			switch sig {
			case syscall.SIGUSR1:
				Log(INFO, "Dumping stack traces due to SIGUSR1 request")
				pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
			case syscall.SIGINT:
				Log(INFO, "Initiating graceful shutdown due to SIGINT request")
				atomic.StoreUint32(&frontend.Queryable, uint32(0))
				Log(INFO, "Waiting a grace period of %v to shutdown...", utils.InstanceConfig.StopGracePeriod)
				time.Sleep(utils.InstanceConfig.StopGracePeriod)
				shutdown()
			}
		}
	}()
	signal.Notify(sigChannel, syscall.SIGUSR1)
	signal.Notify(sigChannel, syscall.SIGINT)

	Log(INFO, "Initializing MarketStore...")

	if *printVersion {
		fmt.Printf("marketstore version %s (%s)\n", utils.Version, utils.Sha1hash)
		os.Exit(0)
	}
}

func main() {
	Run()
}

func shutdown() {
	executor.ThisInstance.ShutdownPending = true
	executor.ThisInstance.WALWg.Wait()
	Log(INFO, "Exiting...")
	os.Exit(0)
}

func Run() {
	executor.NewInstanceSetup(utils.InstanceConfig.RootDirectory, true, true, true)
	InitializeTriggers()
	RunBgWorkers()
	//server, service := frontend.NewServer()
	server, _ := frontend.NewServer()

	Log(INFO, "Launching rpc data server...")
	go http.Handle("/rpc", server)
	Log(INFO, "Launching heartbeat service...")
	go frontend.Heartbeat(utils.InstanceConfig.ListenPort)
	Log(INFO, "Enabling Query Access...")
	atomic.StoreUint32(&frontend.Queryable, 1)
	/*
		Running tcp listener mux
	*/
	Log(INFO, "Launching tcp listener for all services...")
	err := http.ListenAndServe(utils.InstanceConfig.ListenPort, nil)
	if err != nil {
		Log(FATAL, "Failed to start server - Error: %s", err)
	}
}
