package main

import (
	"github.com/golang/glog"

	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/plugins"
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
		theInstance.TriggerMatchers = append(
			theInstance.TriggerMatchers, tmatcher)
	}
}

func NewTriggerMatcher(ts *utils.TriggerSetting) *trigger.TriggerMatcher {
	pi, err := plugins.Load(ts.Module)
	if err != nil {
		glog.Errorf("Unable to open plugin %s: %v", ts.Module, err)
		return nil
	}
	sym, err := pi.Lookup("NewTrigger")
	if err != nil {
		glog.Errorf("Unable to lookup plugin symbol Trigger: %v", err)
		return nil
	}

	newFunc, ok := sym.(func(map[string]interface{}) (trigger.Trigger, error))
	if !ok {
		glog.Errorf("NewTrigger does not comply trigger.NewFuncType")
		return nil
	}
	trig, err := newFunc(ts.Config)
	if err != nil {
		glog.Errorf("Error returned while creating a trigger: %v", err)
		return nil
	}
	return trigger.NewMatcher(trig, ts.On)
}
