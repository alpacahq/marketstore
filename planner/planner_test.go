package planner_test

import (
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/alpacahq/marketstore/v4/catalog"
	"github.com/alpacahq/marketstore/v4/planner"
	"github.com/alpacahq/marketstore/v4/utils/test"
)

func setup(t *testing.T, testName string,
) (tearDown func(), rootDir string, catalogDir *catalog.Directory) {
	t.Helper()

	rootDir, _ = ioutil.TempDir("", fmt.Sprintf("planner_test-%s", testName))
	test.MakeDummyCurrencyDir(rootDir, false, false)
	catalogDir, err := catalog.NewDirectory(rootDir)
	if err != nil {
		t.Fatal("failed to create a catalog dir.err=" + err.Error())
	}

	return func() { test.CleanupDummyDataDir(rootDir) }, rootDir, catalogDir
}

func TestQuery(t *testing.T) {
	tearDown, _, catalogDir := setup(t, "TestQuery")
	defer tearDown()

	q := planner.NewQuery(catalogDir)
	q.AddRestriction("Symbol", "NZDUSD")
	q.AddRestriction("AttributeGroup", "OHLC")
	q.AddRestriction("Symbol", "USDJPY")
	q.AddRestriction("Timeframe", "1Min")
	q.SetRange(
		time.Date(2001, 1, 1, 12, 0, 0, 0, time.UTC),
		time.Date(2002, 12, 20, 12, 0, 0, 0, time.UTC),
	)
	pr, _ := q.Parse()
	assert.Len(t, pr.QualifiedFiles, 6)

	q = planner.NewQuery(catalogDir)
	q.AddRestriction("Symbol", "BBBYYY")
	pr, err := q.Parse()
	assert.NotNil(t, err)
	assert.Len(t, pr.QualifiedFiles, 0)

	q = planner.NewQuery(catalogDir)
	q.AddRestriction("YYYYYY", "BBBYYY")
	_, err = q.Parse()
	assert.NotNil(t, err)

	q = planner.NewQuery(catalogDir)
	q.AddRestriction("AttributeGroup", "OHLC")
	pr, _ = q.Parse()
	qfs := pr.QualifiedFiles
	assert.Len(t, qfs, 54)
}
