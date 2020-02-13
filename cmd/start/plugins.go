package start

import (
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/plugins"
	"github.com/alpacahq/marketstore/v4/plugins/bgworker"
	"github.com/alpacahq/marketstore/v4/plugins/trigger"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

func InitializeTriggers() {
	log.Info("InitializeTriggers")
	config := utils.InstanceConfig
	theInstance := executor.ThisInstance
	for _, triggerSetting := range config.Triggers {
		log.Info("triggerSetting = %v", triggerSetting)
		tmatcher := NewTriggerMatcher(triggerSetting)
		if tmatcher != nil {
			theInstance.TriggerMatchers = append(
				theInstance.TriggerMatchers, tmatcher)
		}
	}
	log.Info("InitializeTriggers - Done")
}

func NewTriggerMatcher(ts *utils.TriggerSetting) *trigger.TriggerMatcher {
	loader, err := plugins.NewSymbolLoader(ts.Module)
	if err != nil {
		log.Error("Unable to open plugin for trigger in %s: %v", ts.Module, err)
		return nil
	}
	trig, err := trigger.Load(loader, ts.Config)
	if err != nil {
		log.Error("Error returned while creating a trigger: %v", err)
		return nil
	}
	return trigger.NewMatcher(trig, ts.On)
}

func RunBgWorkers() {
	log.Info("InitializeBgWorkers")
	config := utils.InstanceConfig
	for _, bgWorkerSetting := range config.BgWorkers {
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
