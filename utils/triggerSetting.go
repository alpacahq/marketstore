package utils

import (
	"github.com/golang/glog"

	plugins "github.com/alpacahq/marketstore/plugins"
	"github.com/alpacahq/marketstore/plugins/trigger"
)

type TriggerSetting struct {
	Module string
	On     string
	Config map[string]interface{}
}

// NewInstance creates trigger.TriggerMatcher instance by loading the plugin
// as specified in the setting.
func (ts *TriggerSetting) NewInstance() *trigger.TriggerMatcher {
	pi, err := plugins.LoadFromGOPATH(ts.Module)
	if err != nil {
		glog.Errorf("Unable to open plugin %s: %v", ts.Module, err)
		return nil
	}
	sym, err := pi.Lookup("NewTrigger")
	if err != nil {
		glog.Errorf("Unable to lookup plugin symbol Trigger: %v", err)
		return nil
	}

	newFunc := sym.(func(map[string]interface{}) (trigger.Trigger, error))
	//if !ok {
	//	glog.Errorf("NewTrigger does not comply trigger.NewFuncType")
	//	return nil
	//}
	trig, err := newFunc(ts.Config)
	if err != nil {
		glog.Errorf("Error returned while creating a trigger: %v", err)
		return nil
	}
	return trigger.NewMatcher(trig, ts.On)
}
