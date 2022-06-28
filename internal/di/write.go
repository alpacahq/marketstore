package di

import (
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/frontend"
)

// GetWriter returns a CSM writer.
// it returns ErrorWriter to replica instances because write API is disabled on replicas.
func (c *Container) GetWriter() frontend.Writer {
	if c.writer != nil {
		return c.writer
	}

	if c.mktsConfig.Replication.MasterHost != "" {
		// WRITE is not allowed on a read replica
		c.writer = &executor.ErrorWriter{}
		return c.writer
	}

	c.writer = c.GetDefaultWriter()

	return c.writer
}

// GetDefaultWriter returns a writable writer.
// Replica instances can use it only for data writes for replication.
func (c *Container) GetDefaultWriter() frontend.Writer {
	writer, err := executor.NewWriter(c.GetCatalogDir(), c.GetInitWALFile())
	if err != nil {
		panic(err)
	}
	return writer
}
