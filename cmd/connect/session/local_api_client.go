package session

import (
	"fmt"
	"os"
	"time"

	"github.com/alpacahq/marketstore/v4/catalog"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/frontend"
	"github.com/alpacahq/marketstore/v4/planner"
	"github.com/alpacahq/marketstore/v4/sqlparser"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

// NewLocalAPIClient builds a new client struct in local mode.
func NewLocalAPIClient(dir string) (lc *LocalAPIClient, err error) {
	// Configure db settings.
	initCatalog, initWALCache, backgroundSync, WALBypass := true, true, false, true
	walRotateInterval := 5
	instanceConfig, _, _, err := executor.NewInstanceSetup(dir,
		nil, nil, walRotateInterval, initCatalog, initWALCache, backgroundSync, WALBypass,
	)
	if err != nil {
		return nil, fmt.Errorf("create a new instance setup for local API client: %w", err)
	}

	ar := sqlparser.NewDefaultAggRunner(instanceConfig.CatalogDir)
	qs := frontend.NewQueryService(instanceConfig.CatalogDir)
	writer, err := executor.NewWriter(instanceConfig.CatalogDir, instanceConfig.WALFile)
	if err != nil {
		return nil, fmt.Errorf("init writer: %w", err)
	}
	return &LocalAPIClient{dir: dir, catalogDir: instanceConfig.CatalogDir, aggRunner: ar, writer: writer, query: qs},
		nil
}

type LocalAPIClient struct {
	// dir is the optional filesystem location of a local db instance.
	dir string
	// catalogDir is an in-memory cache for directory structure under the /data directory
	catalogDir *catalog.Directory
	aggRunner  *sqlparser.AggRunner
	writer     *executor.Writer
	query      *frontend.QueryService
}

func (lc *LocalAPIClient) PrintConnectInfo() {
	fmt.Fprintf(os.Stderr, "Connected to local instance at path: %v\n", lc.dir)
}
func (lc *LocalAPIClient) Connect() error {
	// Nothing to do here yet..
	return nil
}

func (lc *LocalAPIClient) Write(reqs *frontend.MultiWriteRequest, responses *frontend.MultiServerResponse) error {
	ds := frontend.NewDataService(lc.dir, lc.catalogDir, lc.aggRunner, lc.writer, lc.query)
	err := ds.Write(nil, reqs, responses)
	if err != nil {
		return err
	}

	// because a marketstore process has cache of the data directory structure (executor.ThisInstance.CatalogDir)
	// and it can't be updated by the CSV import using the local mode,
	// the imported data is not returned to query responses until restart marketstore,
	// the data is correctly written to the data file though.
	fmt.Println("Note: The imported data won't be returned to query responses until restart marketstore" +
		" due to the cache of the marketstore process.")
	return nil
}

func (lc *LocalAPIClient) Show(tbk *io.TimeBucketKey, start, end *time.Time,
) (csm io.ColumnSeriesMap, err error) {

	if start == nil && end == nil {
		fmt.Println("No suitable date range supplied...")
		return
	}
	if start == nil {
		start = &planner.MinTime
	}
	if end == nil {
		end = &planner.MaxTime
	}
	fmt.Printf("Query range: %v to %v\n", start, end)

	qs := frontend.NewQueryService(lc.catalogDir)
	csm, err = qs.ExecuteQuery(tbk, *start, *end, 0, false, nil)
	if err != nil {
		log.Error("Error return from query: %v", err)
		return
	}

	return csm, nil
}

func (lc *LocalAPIClient) Create(reqs *frontend.MultiCreateRequest, responses *frontend.MultiServerResponse) error {
	ds := frontend.NewDataService(lc.dir, lc.catalogDir, lc.aggRunner, lc.writer, lc.query)
	return ds.Create(nil, reqs, responses)
}

func (lc *LocalAPIClient) Destroy(reqs *frontend.MultiKeyRequest, responses *frontend.MultiServerResponse) error {
	ds := frontend.NewDataService(lc.dir, lc.catalogDir, lc.aggRunner, lc.writer, lc.query)
	return ds.Destroy(nil, reqs, responses)
}

func (lc *LocalAPIClient) GetBucketInfo(reqs *frontend.MultiKeyRequest, responses *frontend.MultiGetInfoResponse) error {
	ds := frontend.NewDataService(lc.dir, lc.catalogDir, lc.aggRunner, lc.writer, lc.query)
	return ds.GetInfo(nil, reqs, responses)
}

func (lc *LocalAPIClient) SQL(line string) (cs *io.ColumnSeries, err error) {
	queryTree, err := sqlparser.BuildQueryTree(line)
	if err != nil {
		return nil, err
	}
	es, err := sqlparser.NewExecutableStatement(queryTree)
	if err != nil {
		return nil, err
	}
	cs, err = es.Materialize(lc.aggRunner, lc.catalogDir)
	if err != nil {
		return nil, err
	}
	return cs, nil
}
