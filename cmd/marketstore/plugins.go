package main

import (
	"github.com/golang/glog"

	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/plugins"
	"github.com/alpacahq/marketstore/plugins/bgworker"
	"github.com/alpacahq/marketstore/plugins/trigger"
	"github.com/alpacahq/marketstore/utils"
)

func InitializeTriggers() {
	glog.Info("InitializeTriggers")
	config := utils.InstanceConfig
	theInstance := executor.ThisInstance
	for _, triggerSetting := range config.Triggers {
		glog.Infof("triggerSetting = %v", triggerSetting)
		tmatcher := NewTriggerMatcher(triggerSetting)
		if tmatcher != nil {
			theInstance.TriggerMatchers = append(
				theInstance.TriggerMatchers, tmatcher)
		}
	}
}

func NewTriggerMatcher(ts *utils.TriggerSetting) *trigger.TriggerMatcher {
	loader, err := plugins.NewSymbolLoader(ts.Module)
	if err != nil {
		glog.Errorf("Unable to open plugin for trigger in %s: %v", ts.Module, err)
		return nil
	}
	trig, err := trigger.Load(loader, ts.Config)
	if err != nil {
		glog.Errorf("Error returned while creating a trigger: %v", err)
		return nil
	}
	return trigger.NewMatcher(trig, ts.On)
}

func RunBgWorkers() {
	glog.Info("InitializeBgWorkers")
	config := utils.InstanceConfig
	for _, bgWorkerSetting := range config.BgWorkers {
		glog.Infof("bgWorkerSetting = %v", bgWorkerSetting)
		bgWorker := NewBgWorker(bgWorkerSetting)
		if bgWorker != nil {
			// we should probably keep track of this process status
			// and may want to kill it or get info.  utils.Process may help
			// but will figure it out later.
			glog.Infof("Start running BgWorker %s...", bgWorkerSetting.Name)
			go bgWorker.Run()
		}
	}
}

func NewBgWorker(s *utils.BgWorkerSetting) bgworker.BgWorker {
	loader, err := plugins.NewSymbolLoader(s.Module)
	if err != nil {
		glog.Errorf("Unable to open plugin for bgworker in %s: %v", s.Module, err)
		return nil
	}
	bgWorker, err := bgworker.Load(loader, s.Config)
	if err != nil {
		glog.Errorf("Failed to create bgworker: %v", err)
	}
	return bgWorker
}
