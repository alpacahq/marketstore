include ../Makefile.common

CONTAINER_NAME=integrationtests.pyclient
DOCKER_CMD=tail -f /dev/null
DOCKER_ADDOPTS=	-v $(CURDIR)/../../tests:/project/tests \
	--net=host \


test-jsonrpc:
	# for json-RPC API
	docker exec -e MARKETSTORE_PORT=5993 -e USE_GRPC=false $(CONTAINER_NAME)  \
	    bash -c \
		"pytest -v -v -v $(TEST_FILENAME)"

test-grpc:
	# for gRPC API
	docker exec -e MARKETSTORE_PORT=5995 -e USE_GRPC=true $(CONTAINER_NAME)  \
	    bash -c \
		"pytest -v -v -v $(TEST_FILENAME)"
