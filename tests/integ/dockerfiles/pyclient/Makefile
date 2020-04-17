include ../Makefile.common

CONTAINER_NAME=integrationtests.pyclient
DOCKER_CMD=tail -f /dev/null
DOCKER_ADDOPTS=	-v $(CURDIR)/../../tests:/project/tests \
	--net=host \


test:
	docker exec $(CONTAINER_NAME) bash -c \
		"pytest -v -v -v $(TEST_FILENAME)"
