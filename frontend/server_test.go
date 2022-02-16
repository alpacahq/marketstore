package frontend_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/alpacahq/marketstore/v4/frontend"
	"github.com/alpacahq/marketstore/v4/sqlparser"
)

func TestNewServer(t *testing.T) {
	rootDir, metadata, writer, q := setup(t)

	serv, _ := frontend.NewServer(rootDir, metadata.CatalogDir, sqlparser.NewAggRunner(nil), writer, q)
	assert.True(t, serv.HasMethod("DataService.Query"))
}
