include ../Makefile.common

CONTAINER_NAME=integrationtests.marketstore
DOCKER_CMD=marketstore start --config /project/dockerfiles/marketstore/mkts.yml
DOCKER_ADDOPTS=-p 5993:5993 \
	-v $(CURDIR)/../../:/project \
	-w /project/dockerfiles/marketstore

loaddb:
	docker run $(DOCKER_OPTS) -it \
		$(IMAGE_NAME) \
		/project/dockerfiles/marketstore/loaddb.sh \
		https://s3.amazonaws.com/dev.alpaca.markets/gobroker/mktsdb.tar.gz

resetdb:
	docker exec -i -t $(CONTAINER_NAME) /project/dockerfiles/marketstore/resetdb.sh

ls_data:
	docker exec -i -t $(CONTAINER_NAME) ls -alR /project/data/

import_csv:
	docker exec -i -t $(CONTAINER_NAME) bash -c "/project/tests/setup_mkts/tick/$(TEST_NUM)/load_csv.sh"
