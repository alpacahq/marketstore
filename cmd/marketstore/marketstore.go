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
	"github.com/alpacahq/marketstore/frontend/stream"
	"github.com/alpacahq/marketstore/utils"
	. "github.com/alpacahq/marketstore/utils/log"
)

// This is the launcher for all marketstore services

func init() {
	utils.InstanceConfig.StartTime = time.Now()
	configFlag := flag.String("config", "mkts.yml", "MarketStore YAML configuration file")
	printVersion := flag.Bool("version", false, "MarketStore version information")

	flag.Parse()

	if *printVersion {
		fmt.Printf("Version Tag: %v\n", utils.Tag)
		fmt.Printf("Git Commit Hash: %v\n", utils.GitHash)
		fmt.Printf("UTC Build Time: %v\n", utils.BuildStamp)
		os.Exit(0)
	}

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
}

func main() {
	executor.NewInstanceSetup(utils.InstanceConfig.RootDirectory, true, true, true)

	server, _ := frontend.NewServer()

	Log(INFO, "Launching rpc data server...")
	go http.Handle("/rpc", server)

	Log(INFO, "Initializing websocket...")
	stream.Initialize()
	go http.HandleFunc("/ws", stream.Handler)

	InitializeTriggers()

	RunBgWorkers()

	Log(INFO, "Launching heartbeat service...")
	go frontend.Heartbeat(utils.InstanceConfig.ListenPort)

	Log(INFO, "Enabling Query Access...")
	atomic.StoreUint32(&frontend.Queryable, 1)

	/*
		Running tcp listener mux
	*/
	Log(INFO, "Launching tcp listener for all services...")
	if err := http.ListenAndServe(utils.InstanceConfig.ListenPort, nil); err != nil {
		Log(FATAL, "Failed to start server - Error: %s", err)
	}
}

func shutdown() {
	executor.ThisInstance.ShutdownPending = true
	executor.ThisInstance.WALWg.Wait()
	Log(INFO, "Exiting...")
	os.Exit(0)
}
