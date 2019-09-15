################################################################################
# Commands to automate integration tests
################################################################################

IMAGE_NAME ?= alpacahq/integrationtests.marketstore
MARKETSTORE_DEV_DB_URL = https://s3.amazonaws.com/dev.alpaca.markets/gobroker/mktsdb.tar.gz
MARKETSTORE_DEV_DB_TMPFILE = /tmp/mktsdb.tgz

# Utils
################################################################################

.PHONY: clean
clean:
	rm -rf data tests/__pycache__

.PHONY: _get_data
_get_data:
	@rm -rf /tmp/data && mkdir /tmp/data
	@if [ ! -f ${MARKETSTORE_DEV_DB_TMPFILE} ]; then \
		wget ${MARKETSTORE_DEV_DB_URL} -O ${MARKETSTORE_DEV_DB_TMPFILE}; \
	fi
	@tar -C /tmp/data -xzf ${MARKETSTORE_DEV_DB_TMPFILE}

start_marketstore:
	# the background operator (&) will give us the PID at the command prompt.
	nohup marketstore start --config bin/mkts.yml & echo $$! > save_pid.txt

stop_marketstore:
	# stop marketstore process
	kill `cat save_pid.txt`
	rm save_pid.txt
	# stop data written by the integration tests
	rm -rf ../../data/*

.PHONY: _start_pyclient_container
_start_pyclient_container:
	make -C dockerfiles rm build run

# Tests
################################################################################

.PHONY: test_import_csv
test_import_csv:
	@bin/runtests.sh

# run all tests including CSV Import
.PHONY: test
test:
	$(MAKE) test_import_csv
	$(MAKE) clean
	$(MAKE) _get_data

	@echo "Starting a marketstore at localhost..."
	$(MAKE) start_marketstore

	@echo "Starting a pymarketstore container..."
	TEST_FILENAME='/project/tests/$@.py';
	$(MAKE) _start_pyclient_container

	# stop marketstore process
	make stop_marketstore
