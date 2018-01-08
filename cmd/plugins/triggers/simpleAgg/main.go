package main

import (
	"github.com/golang/glog"

	"github.com/alpacahq/marketstore/plugins/trigger"
)

type SimpleAggTrigger struct {
	config map[string]interface{}
}

var _ trigger.Trigger = &SimpleAggTrigger{}

func NewTrigger(config map[string]interface{}) (trigger.Trigger, error) {
	glog.Infof("NewTrigger")
	return &SimpleAggTrigger{
		config: config,
	}, nil
}

func (s *SimpleAggTrigger) Fire(keyPath string, offsets []int64) {
	glog.Infof("keyPath=%s offsets=%v", keyPath, offsets)
}

func main() {
}
