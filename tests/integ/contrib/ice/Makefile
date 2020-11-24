MAKEFILE_PATH := $(shell dirname $(abspath $(lastword $(MAKEFILE_LIST))))
PLUGIN_ROOT := $(abspath $(MAKEFILE_PATH)/)

MKTS_CONTAINER_NAME ?= integration_tests_mstore
PYCLIENT_CONTAINER_NAME ?= integrationtests.pyclient

mkts_copy_data:
	docker exec ${MKTS_CONTAINER_NAME} bash -c "mkdir -p /project/data/contrib/ice/data"
	docker cp ${PLUGIN_ROOT}/data ${MKTS_CONTAINER_NAME}:/project/data/contrib/ice/

mkts_import_ca:
	docker exec ${MKTS_CONTAINER_NAME} bash -c "/bin/ice reorg import /project/data/mktsdb /project/data/contrib/ice/data --fallback-to-cusip"

mkts_import_price:
	docker exec ${MKTS_CONTAINER_NAME} bash -x /project/data/contrib/ice/data/load_price.sh

mkts_restart:
	docker restart ${MKTS_CONTAINER_NAME}

prepare_mkts: mkts_copy_data mkts_import_ca mkts_import_price mkts_restart

prepare_client:
	docker exec ${PYCLIENT_CONTAINER_NAME} bash -c "mkdir -p /project/tests/contrib/ice"
	docker cp ${PLUGIN_ROOT}/tests/* ${PYCLIENT_CONTAINER_NAME}:/project/tests/contrib/ice
	docker cp ${PLUGIN_ROOT}/data/aapl_raw.csv ${PYCLIENT_CONTAINER_NAME}:/project/tests/contrib/ice
	docker cp ${PLUGIN_ROOT}/data/aapl_adj.csv ${PYCLIENT_CONTAINER_NAME}:/project/tests/contrib/ice


prepare: prepare_mkts prepare_client

test: prepare
	docker exec -e MARKETSTORE_PORT=5993 -e USE_GRPC=false $(PYCLIENT_CONTAINER_NAME) \
	    bash -c \
		"pytest -v -v -v tests/contrib/ice/$(TEST_FILENAME)"


debug:
	docker cp ${PLUGIN_ROOT}/tests/* ${PYCLIENT_CONTAINER_NAME}:/project/tests/contrib/ice
	docker exec -e MARKETSTORE_PORT=5993 -e USE_GRPC=false $(PYCLIENT_CONTAINER_NAME) \
	    bash -c \
		"pytest -v -v -v tests/contrib/ice/$(TEST_FILENAME)"

