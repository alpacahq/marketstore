package di

import (
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/plugins/trigger"
)

func (c *Container) InjectTriggerMatchers(ms []*trigger.Matcher) {
	c.triggerMatchers = ms
}

func (c *Container) GetTriggerMatchers() []*trigger.Matcher {
	if c.triggerMatchers != nil {
		return c.triggerMatchers
	}
	c.triggerMatchers = trigger.NewTriggerMatchers(c.mktsConfig.Triggers)
	return c.triggerMatchers
}

func (c *Container) GetStartTriggerPluginDispatcher() *executor.TriggerPluginDispatcher {
	if c.tpd != nil {
		return c.tpd
	}

	c.tpd = executor.StartNewTriggerPluginDispatcher(c.GetTriggerMatchers())
	return c.tpd
}
