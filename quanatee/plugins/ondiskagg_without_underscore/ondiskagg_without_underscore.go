// This is a shim package for buiding a plugin module wrapping
// the importable aggtrigger package.  For more details, see aggtrigger.
package main

import (
	"github.com/rymdluo/marketstore/quanatee/plugins/quanatee_ondiskagg_without_underscore/aggtrigger"
	"github.com/rymdluo/marketstore/quanatee/plugins/quanatee_trigger_without_underscore"
)

// NewTrigger returns a new on-disk aggregate trigger based on the configuration.
func NewTrigger(conf map[string]interface{}) (trigger.Trigger, error) {
	return aggtrigger.NewTrigger(conf)
}

func main() {
}
