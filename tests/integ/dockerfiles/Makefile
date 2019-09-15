MAKEFILE_PATH := $(shell dirname $(abspath $(lastword $(MAKEFILE_LIST))))
PROJECT_ROOT := $(abspath $(MAKEFILE_PATH)/)
DATA_DIR ?= $(PROJECT_ROOT)/data

CONTAINER_NAME=integrationtests.pyclient
IMAGE_NAME=alpacahq/$(CONTAINER_NAME)

# build pymarketstore docker container for integration test
build:
	docker build -t $(IMAGE_NAME) pyclient

run:
	docker run $(IMAGE_NAME) bash -c "pytest -v -v -v $(TEST_FILENAME)"

rm:
	docker rm -f $(CONTAINER_NAME) || echo

logs:
	docker logs -f --tail 100 $(CONTAINER_NAME)

bash:
	docker exec -i -t $(CONTAINER_NAME) bash

cpsource:
	docker cp ../. $(CONTAINER_NAME):/project
