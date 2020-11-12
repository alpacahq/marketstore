
MARKETSTORE_IMAGE_NAME := alpacahq/replicationtests.marketstore
PYMARKETSTORE_IMAGE_NAME := alpacahq/replicationtests.pymarketstore

MASTER_CONTAINER_NAME := replication_tests_mstore_master
REPLICA_CONTAINER_NAME := replication_tests_mstore_replica1
PYMARKETSTORE_CONTAINER_NAME := replication_tests_pymarketstore

.PHONY:clean
clean:
	# "| true" is a trick to continue even if the command is failed

	# stop master container
	docker stop ${MASTER_CONTAINER_NAME} | true
	docker rm -f ${MASTER_CONTAINER_NAME} | true

	# stop replica container(s)
	docker stop ${REPLICA_CONTAINER_NAME} | true
	docker rm -f ${REPLICA_CONTAINER_NAME} | true

	# stop pyclient container
	docker stop $(PYMARKETSTORE_CONTAINER_NAME) | true
	docker rm -f ${PYMARKETSTORE_CONTAINER_NAME} | true

# build marketstore docker container for replication test
build-marketstore:
	docker build -t ${MARKETSTORE_IMAGE_NAME} --build-arg INCLUDE_PLUGINS=false ../..

build-pymarketstore:
	docker build -t $(PYMARKETSTORE_IMAGE_NAME) ../integ/dockerfiles/pyclient/

.PHONY:create-network
create-network:
	docker network create marketstore | true

# json-rpc port: 5996, grpc port:5997, replication listening port: 5998
.PHONY:run-master
run-master:
	# copy the test config and data to the container and start it after the container is created
	# because mounting folders from docker executor is not supported in Circle CI2.0.
	# https://discuss.circleci.com/t/why-circleci-2-0-does-not-support-mounting-folders/11605
	docker create --name ${MASTER_CONTAINER_NAME} --network=marketstore -p 5996:5996 -p 5997:5997 -p 5998:5998 --workdir /project  $(MARKETSTORE_IMAGE_NAME) start --config /project/config/mkts-master.yml
	docker cp $(CURDIR)/config ${MASTER_CONTAINER_NAME}:/project/
	@rm -rf data && mkdir -p data/mktsdb
	docker cp $(CURDIR)/data ${MASTER_CONTAINER_NAME}:/project/
	docker start ${MASTER_CONTAINER_NAME}

	@sleep 2
	@if [ `../integ/bin/check_running ${MASTER_CONTAINER_NAME}` -eq 0 ]; then \
		echo "Failed to start a marketstore master instance"; \
		false; \
	fi

# json-rpc port: 5999, grpc port:6000
.PHONY:run-replica
run-replica:
	docker create --name ${REPLICA_CONTAINER_NAME} --network=marketstore -p 5999:5999 -p 6000:6000 --workdir /project  $(MARKETSTORE_IMAGE_NAME) start --config /project/config/mkts-replica.yml
	docker cp $(CURDIR)/config ${REPLICA_CONTAINER_NAME}:/project/
	@rm -rf data && mkdir -p data/mktsdb
	docker cp $(CURDIR)/data ${REPLICA_CONTAINER_NAME}:/project/
	docker start ${REPLICA_CONTAINER_NAME}

	@sleep 2
	@if [ `../integ/bin/check_running ${REPLICA_CONTAINER_NAME}` -eq 0 ]; then \
		echo "Failed to start a marketstore replica instance"; \
		false; \
	fi

run-pyclient:
	docker create --name $(PYMARKETSTORE_CONTAINER_NAME) --net=host $(PYMARKETSTORE_IMAGE_NAME) tail -f /dev/null
	docker cp $(CURDIR)/tests $(PYMARKETSTORE_CONTAINER_NAME):/project/tests
	docker start $(PYMARKETSTORE_CONTAINER_NAME)

	@sleep 2
	@if [ `../integ/bin/check_running ${PYMARKETSTORE_CONTAINER_NAME}` -eq 0 ]; then \
		echo "Failed to start a pymarketstore client instance"; \
		false; \
	fi

test:
	# for json-RPC API
	docker exec -e MASTER_PORT=5996 -e REPLICA_PORT=5999 -e USE_GRPC=false $(PYMARKETSTORE_CONTAINER_NAME)  \
	    bash -c \
		"pytest -v -v -v /project/tests/"

.PHONY: test-replication
test-replication: clean build-marketstore build-pymarketstore create-network run-master run-replica run-pyclient test