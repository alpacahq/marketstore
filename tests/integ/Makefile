################################################################################
# Commands to automate integration tests
################################################################################

IMAGE_NAME ?= alpacahq/integrationtests.marketstore
CONTAINER_NAME = integration_tests_mstore
INCLUDE_PLUGINS ?= false

# User targets
################################################################################

# build marketstore docker container for integration test
build_mstore:
	docker build -t ${IMAGE_NAME} --build-arg INCLUDE_PLUGINS=${INCLUDE_PLUGINS} ../..

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
run: build_mstore
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
.PHONY: _startup
_startup: stop
	@echo "Starting a marketstore instance..."

	# copy the test config and data to the container and start it after the container is created
	# because mounting folders from docker executor is not supported in Circle CI2.0.
	# https://discuss.circleci.com/t/why-circleci-2-0-does-not-support-mounting-folders/11605
	docker create --name ${CONTAINER_NAME} -p 5993:5993 -p 5995:5995 -w /project  $(IMAGE_NAME) start --config /project/bin/mkts.yml
	docker cp $(CURDIR)/bin ${CONTAINER_NAME}:/project/
	@rm -rf data && mkdir -p data/mktsdb
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

# run all integration tests by json-rpc client
.PHONY: test-jsonrpc
test-jsonrpc: clean run _start_pyclient_container connect
	TEST_FILENAME='/project/tests/$@.py'; \
	make -C dockerfiles/pyclient test-jsonrpc

# run all integration tests by grpc client
.PHONY: test-grpc
test-grpc: clean run _start_pyclient_container connect
	TEST_FILENAME='/project/tests/$@.py'; \
	make -C dockerfiles/pyclient test-grpc

# run all integration tests for the contrib/ice plugin
.PHONY: test-contrib
test-contrib: INCLUDE_PLUGINS = true
test-contrib: clean run _start_pyclient_container connect 
	make -C contrib/ice test
