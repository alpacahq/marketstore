package main

import (
	"github.com/alpacahq/marketstore/contrib/ondiskagg/aggtrigger"
	"github.com/alpacahq/marketstore/plugins/trigger"
)

// NewTrigger returns a new on-disk aggregate trigger based on the configuration.
func NewTrigger(conf map[string]interface{}) (trigger.Trigger, error) {
	return aggtrigger.NewTrigger(conf)
}

func main() {
}
