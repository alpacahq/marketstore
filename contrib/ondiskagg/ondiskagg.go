// This is a shim package for buiding a plugin module wrapping
// the importable aggtrigger package.  For more details, see aggtrigger.
package main

import (
	"github.com/alpacahq/marketstore/v4/contrib/ondiskagg/aggtrigger"
	"github.com/alpacahq/marketstore/v4/plugins/trigger"
)

// NewTrigger returns a new on-disk aggregate trigger based on the configuration.
func NewTrigger(conf map[string]interface{}) (trigger.Trigger, error) {
	return aggtrigger.NewTrigger(conf)
}

func main() {
}
