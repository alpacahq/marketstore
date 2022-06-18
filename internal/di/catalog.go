package di

import (
	"errors"
	"github.com/alpacahq/marketstore/v4/catalog"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

func (c *Container) GetCatalogDir() *catalog.Directory {
	if !c.mktsConfig.InitCatalog {
		return nil
	}
	if c.catalogDir != nil {
		return c.catalogDir
	}

	// Initialize a global catalog
	catalogDir, err := catalog.NewDirectory(c.GetAbsRootDir())
	if err != nil {
		var e catalog.ErrCategoryFileNotFound
		if errors.As(err, &e) {
			log.Debug("new root directory found:" + c.GetAbsRootDir())
		} else {
			log.Error("Could not create a catalog directory: %s.", err.Error())
			panic(err)
		}
	}

	c.catalogDir = catalogDir
	return c.catalogDir
}
