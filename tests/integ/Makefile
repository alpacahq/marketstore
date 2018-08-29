################################################################################
# Commands to automate integration tests
################################################################################

# Utils
################################################################################

test_mktsdb_conn:
	curl -s --data-binary '{"jsonrpc":"2.0", "method":"DataService.ListSymbols", "id":1, "params": {"parameters": {}}}' -H 'Content-Type: application/json' http://localhost:5993/rpc

reset_marketstore:
	$(MAKE) -C dockerfiles/marketstore build rm run resetdb rm
	echo Waiting initialization of marketstore
	sleep 2
	$(MAKE) -C dockerfiles/marketstore rm run
	sleep 2
	$(MAKE) test_mktsdb_conn

# Tests
################################################################################

LATEST_TAG = $(shell bin/dockertags alpacamarkets/marketstore | tail -1)

test_run_latest:
	#if [ ! -d project ]; then mkdir -p project/data/mktsdb; fi
	#docker run -i -v `pwd`/project:/project -p 5993:5993 alpacamarkets/marketstore:${LATEST_TAG}
	docker run -i -p 5993:5993 alpacamarkets/marketstore:${LATEST_TAG}

_test_import_csv: reset_marketstore
	TEST_NUM=$(TEST_NUM) make -C dockerfiles/marketstore import_csv
	$(MAKE) test_mktsdb_conn
	make -C dockerfiles/pyclient rm build run
	TEST_FILENAME='/project/tests/test_not_empty.py' make -C dockerfiles/pyclient test
	make -C dockerfiles/marketstore rm

test_import_csv_1:
	TEST_NUM=1 $(MAKE) _test_import_csv

test_import_csv_2:
	TEST_NUM=2 $(MAKE) _test_import_csv

test_client: reset_marketstore
	$(MAKE) test_mktsdb_conn
	make -C dockerfiles/pyclient rm build run
	TEST_FILENAME='/project/tests/test_ticks.py' make -C dockerfiles/pyclient test
