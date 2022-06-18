package di

import (
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/frontend"
)

func (c *Container) GetWriter() frontend.Writer {
	if c.writer != nil {
		return c.writer
	}

	if c.mktsConfig.Replication.MasterHost != "" {
		// WRITE is not allowed on a read replica
		c.writer = &executor.ErrorWriter{}
		return c.writer
	}

	var err error
	c.writer, err = executor.NewWriter(c.GetCatalogDir(), c.GetInitWALFile())
	if err != nil {
		panic(err)
	}

	return c.writer
}
