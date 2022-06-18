package di

import "github.com/alpacahq/marketstore/v4/sqlparser"

// GetAggRunner gets Aggregation Functions registry
func (c *Container) GetAggRunner() *sqlparser.AggRunner {
	if c.aggRunner != nil {
		return c.aggRunner
	}
	c.aggRunner = sqlparser.NewDefaultAggRunner(c.GetCatalogDir())
	return c.aggRunner
}
