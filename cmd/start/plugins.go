package start

import (
	"github.com/alpacahq/marketstore/v4/plugins"
	"github.com/alpacahq/marketstore/v4/plugins/bgworker"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

func RunBgWorkers(bgWorkers []*utils.BgWorkerSetting) {
	log.Info("InitializeBgWorkers")
	for _, bgWorkerSetting := range bgWorkers {
		// bgWorkerSetting may contain sensitive data such as a password or token.
		log.Debug("bgWorkerSetting = %v", bgWorkerSetting)
		bgWorker := NewBgWorker(bgWorkerSetting)
		if bgWorker != nil {
			// we should probably keep track of this process status
			// and may want to kill it or get info.  utils.Process may help
			// but will figure it out later.
			log.Info("Start running BgWorker %s...", bgWorkerSetting.Name)
			go bgWorker.Run()
		}
	}
	log.Info("InitializeBgWorkers Done")
}

func NewBgWorker(s *utils.BgWorkerSetting) bgworker.BgWorker {
	loader, err := plugins.NewSymbolLoader(s.Module)
	if err != nil {
		log.Error("Unable to open plugin for bgworker in %s: %v", s.Module, err)
		return nil
	}
	bgWorker, err := bgworker.Load(loader, s.Config)
	if err != nil {
		log.Error("Failed to create bgworker: %v", err)
	}
	return bgWorker
}
