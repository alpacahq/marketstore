################################################################################
# Commands to automate integration tests
################################################################################

LATEST_TAG = $(shell bin/dockertags alpacamarkets/marketstore | tail -1)

# Utils
################################################################################

connect:
	curl -s --data-binary '{"jsonrpc":"2.0", "method":"DataService.ListSymbols", "id":1, "params": {"parameters": {}}}' -H 'Content-Type: application/json' http://localhost:5993/rpc

init:
	if [ ! -d data ]; then mkdir data; else rm -rf data && mkdir data; fi
	wget https://s3.amazonaws.com/dev.alpaca.markets/gobroker/mktsdb.tar.gz -O /tmp/mktsdb.tgz && tar -C data -xzf /tmp/mktsdb.tgz && rm /tmp/mktsdb.tgz

# Tests
################################################################################

test_run_latest:
	docker run --entrypoint=/bin/ash -i alpacamarkets/marketstore:${LATEST_TAG} -c 'echo OK'

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
