################################################################################
# Commands to automate integration tests
################################################################################

IMAGE_NAME ?= alpacahq/integrationtests.marketstore
CONTAINER_NAME = integration_tests_mstore
MARKETSTORE_DEV_DB_URL = https://s3.amazonaws.com/dev.alpaca.markets/gobroker/mktsdb.tar.gz
MARKETSTORE_DEV_DB_TMPFILE = /tmp/mktsdb.tgz

# User targets
################################################################################

# build marketstore docker container for integration test
build_mstore:
	docker build -t ${IMAGE_NAME} ../..

# start a marketstore docker container and check if ListSymbols API can be consumed
.PHONY: connect
connect: run
	@curl -s \
	--data-binary '{"jsonrpc":"2.0", "method":"DataService.ListSymbols", "id":1, "params": {"parameters": {}}}' \
	-H 'Content-Type: application/json' http://localhost:5993/rpc ; \
	\
	if [ $$? -ne 0 ]; then \
		echo "Failed"; \
	else \
		echo "Passed"; \
	fi

.PHONY: run
run: _init
	@if [ `bin/check_running ${CONTAINER_NAME}` -eq 0 ]; then \
		$(MAKE) _startup; \
	fi

.PHONY: stop
stop:
	@if [ `bin/check_running ${CONTAINER_NAME}` = "1" ]; then \
		docker stop ${CONTAINER_NAME}; fi
	-docker rm -f ${CONTAINER_NAME}

.PHONY: clean
clean: stop
	rm -rf data tests/__pycache__


# Utils
################################################################################
.PHONY: _init
_init: build_mstore
	@if [ ! -d data/mktsdb ]; then \
		rm -rf data; \
		$(MAKE) _get_data; \
	fi

.PHONY: _get_data
_get_data:
	@rm -rf data && mkdir data
	@if [ ! -f ${MARKETSTORE_DEV_DB_TMPFILE} ]; then \
		wget ${MARKETSTORE_DEV_DB_URL} -O ${MARKETSTORE_DEV_DB_TMPFILE}; \
	fi
	@tar -C data -xzf ${MARKETSTORE_DEV_DB_TMPFILE}

.PHONY: _startup
_startup: stop
	@echo "Starting a marketstore instance..."

	# copy the test config and data to the container and start it after the container is created
	# because mounting folders from docker executor is not supported in Circle CI2.0.
	# https://discuss.circleci.com/t/why-circleci-2-0-does-not-support-mounting-folders/11605
	docker create --name ${CONTAINER_NAME} -p 5993:5993 -w /project  $(IMAGE_NAME) start --config /project/bin/mkts.yml
	docker cp $(CURDIR)/bin ${CONTAINER_NAME}:/project/
	docker cp $(CURDIR)/data ${CONTAINER_NAME}:/project/
	docker start ${CONTAINER_NAME}

	@sleep 2
	@if [ `bin/check_running ${CONTAINER_NAME}` -eq 0 ]; then \
		echo "Failed to start a marketstore instance"; \
		false; \
	fi


# Tests
################################################################################
.PHONY: _start_pyclient_container
_start_pyclient_container:
	make -C dockerfiles/pyclient rm build run

.PHONY: test_import_csv
test_import_csv:
	@bin/runtests.sh

# run all tests including CSV Import
.PHONY: test
test: test_import_csv clean run _start_pyclient_container connect
	TEST_FILENAME='/project/tests/$@.py'; \
	make -C dockerfiles/pyclient test
