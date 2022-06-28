package executor

import (
	"github.com/alpacahq/marketstore/v4/catalog"
)

var ThisInstance *InstanceMetadata

type InstanceMetadata struct {
	CatalogDir *catalog.Directory
	WALFile    *WALFileType
}

func NewInstanceSetup(catalogDir *catalog.Directory, walfile *WALFileType) *InstanceMetadata {
	if ThisInstance == nil {
		ThisInstance = new(InstanceMetadata)
	}
	ThisInstance.WALFile = walfile
	ThisInstance.CatalogDir = catalogDir

	return ThisInstance
}
