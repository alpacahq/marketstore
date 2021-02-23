package session

import (
	"fmt"
	"github.com/alpacahq/marketstore/v4/catalog"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/frontend"
	"github.com/alpacahq/marketstore/v4/planner"
	"github.com/alpacahq/marketstore/v4/sqlparser"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
	"os"
	"time"
)

// NewLocalAPIClient builds a new client struct in local mode.
func NewLocalAPIClient(dir string) (lc *LocalAPIClient, err error) {
	// Configure db settings.
	initCatalog, initWALCache, backgroundSync, WALBypass := true, true, false, true
	walRotateInterval := 5
	instanceConfig, _, _ := executor.NewInstanceSetup(dir,
		nil, walRotateInterval, initCatalog, initWALCache, backgroundSync, WALBypass,
	)
	return &LocalAPIClient{dir: dir, catalogDir: instanceConfig.CatalogDir}, nil
}

type LocalAPIClient struct {
	// dir is the optional filesystem location of a local db instance.
	dir string
	// catalogDir is an in-memory cache for directory structure under the /data directory
	catalogDir *catalog.Directory
	// enableLastKnown is an optimization to reduce the size of dara reading for query
	enableLastKnown bool
}

func (lc *LocalAPIClient) PrintConnectInfo() {
	fmt.Fprintf(os.Stderr, "Connected to local instance at path: %v\n", lc.dir)
}
func (lc *LocalAPIClient) Connect() error {
	// Nothing to do here yet..
	return nil
}

func (lc *LocalAPIClient) Write(reqs *frontend.MultiWriteRequest, responses *frontend.MultiServerResponse) error {
	ds := frontend.NewDataService(lc.enableLastKnown, lc.dir, lc.catalogDir)
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
	query := planner.NewQuery(lc.catalogDir)
	query.AddTargetKey(tbk)

	if start == nil && end == nil {
		fmt.Println("No suitable date range supplied...")
		return
	} else if start == nil {
		query.SetRange(planner.MinTime, *end)
	} else if end == nil {
		query.SetRange(*start, planner.MaxTime)
	}

	fmt.Printf("Query range: %v to %v\n", start, end)

	pr, err := query.Parse()
	if err != nil {
		fmt.Println("No results")
		log.Error("Parsing query: %v", err)
		return
	}

	scanner, err := executor.NewReader(pr, lc.enableLastKnown)
	if err != nil {
		log.Error("Error return from query scanner: %v", err)
		return
	}
	csm, err = scanner.Read()
	if err != nil {
		log.Error("Error return from query scanner: %v", err)
		return
	}

	return csm, nil
}

func (lc *LocalAPIClient) Create(reqs *frontend.MultiCreateRequest, responses *frontend.MultiServerResponse) error {
	ds := frontend.NewDataService(lc.enableLastKnown, lc.dir, lc.catalogDir)
	return ds.Create(nil, reqs, responses)
}

func (lc *LocalAPIClient) Destroy(reqs *frontend.MultiKeyRequest, responses *frontend.MultiServerResponse) error {
	ds := frontend.NewDataService(lc.enableLastKnown, lc.dir, lc.catalogDir)
	return ds.Destroy(nil, reqs, responses)
}

func (lc *LocalAPIClient) GetBucketInfo(reqs *frontend.MultiKeyRequest, responses *frontend.MultiGetInfoResponse) error{
	ds := frontend.NewDataService(lc.enableLastKnown, lc.dir, lc.catalogDir)
	return ds.GetInfo(nil, reqs, responses)
}

func (lc *LocalAPIClient) SQL(line string) (cs *io.ColumnSeries, err error){
	ast, err := sqlparser.NewAstBuilder(line)
	if err != nil {
		return nil, err
	}
	es, err := sqlparser.NewExecutableStatement(lc.catalogDir, ast.Mtree)
	if err != nil {
		return nil, err
	}
	cs, err = es.Materialize()
	if err != nil {
		return nil, err
	}
	return cs, nil
}


