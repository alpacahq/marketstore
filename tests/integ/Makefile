################################################################################
# Commands to automate integration tests
################################################################################

LATEST_TAG ?= $(shell bin/dockertags alpacamarkets/marketstore | tail -1)
IMAGE_NAME ?= alpacamarkets/marketstore:${LATEST_TAG}
CONTAINER_NAME = integration_tests_mstore

SHELL = bash
.ONESHELL:

# User targets
################################################################################
connect: run
	@curl -s --data-binary '{"jsonrpc":"2.0", "method":"DataService.ListSymbols", "id":1, "params": {"parameters": {}}}' -H 'Content-Type: application/json' http://localhost:5993/rpc ; if [ $$? -ne 0 ]; then echo "Failed"; else echo "Passed"; fi


run: _init
	@if [ `bin/check_running ${CONTAINER_NAME}` -eq 0 ]; then $(MAKE) _startup; fi

stop:
	-if [ `bin/check_running ${CONTAINER_NAME}` -eq 1 ]; then \
		docker stop ${CONTAINER_NAME}; fi
	-docker rm -f ${CONTAINER_NAME}

test:
	@rm -rf testdata test_ticks.csv && mkdir -p testdata/mktsdb
	@marketstore connect -d `pwd`/testdata/mktsdb <<- EOF >& /dev/null && diff -q bin/ticks-example-1-output.csv test_ticks.csv && echo "Passed" || echo "Test Failed"
	\create TEST/1Min/TICK:Symbol/Timeframe/AttributeGroup Bid,Ask/float32 variable
	\getinfo TEST/1Min/TICK
	\load TEST/1Min/TICK bin/ticks-example-1.csv bin/ticks-example-1.yaml
	\o test_ticks.csv
	\show TEST/1Min/TICK 1970-01-01
	EOF
	@rm -f test_ticks.csv
	@marketstore connect -d `pwd`/testdata/mktsdb <<- EOF >& /dev/null && diff -q bin/ticks-example-2-output.csv test_ticks.csv && echo "Passed" || echo "Test Failed"
	\create TEST/1Min/TICK:Symbol/Timeframe/AttributeGroup Bid,Ask/float32 variable
	\getinfo TEST/1Min/TICK
	\load TEST/1Min/TICK bin/ticks-example-2.csv bin/ticks-example-2.yaml
	\o test_ticks.csv
	\show TEST/1Min/TICK 1970-01-01
	EOF
	@rm -rf testdata test_ticks.csv

clean: stop
	rm -rf data

# Utils
################################################################################
_init:
	@if [ ! -d data/mktsdb ]; then rm -rf data; $(MAKE) _get_data; fi

_get_data:
	@mkdir data
	@if [ ! -f /tmp/mktsdb.tgz ]; then \
	wget https://s3.amazonaws.com/dev.alpaca.markets/gobroker/mktsdb.tar.gz -O /tmp/mktsdb.tgz; fi
	@tar -C data -xzf /tmp/mktsdb.tgz; fi


_startup: stop
	docker run --name ${CONTAINER_NAME} -d -p 5993:5993 -v $(CURDIR):/project -w /project $(IMAGE_NAME) \
		start --config /project/bin/mkts.yml
	@sleep 2
	@if [ `bin/check_running ${CONTAINER_NAME}` -eq 0 ]; then \
		echo "Failed to start a marketstore instance"; false; fi



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
