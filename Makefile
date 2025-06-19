.PHONY: all
all: clean fmt test pitest build_release

VERSION := 4.1.0-inkless-SNAPSHOT

.PHONY: build
build:
	./gradlew :core:build :storage:inkless:build :metadata:build -x test

core/build/distributions/kafka_2.13-$(VERSION).tgz:
	echo "Building Kafka distribution with version $(VERSION)"
	./gradlew releaseTarGz

.PHONY: build_release
build_release: core/build/distributions/kafka_2.13-$(VERSION).tgz

.PHONY: docker_build_prep
docker_build_prep:
	cd docker && \
	  [ -d .venv ] || python3 -m venv .venv && \
	  .venv/bin/pip install -r requirements.txt

# download prometheus jmx exporter
docker/extra/prometheus-jmx-javaagent/jmx_prometheus_javaagent.jar:
	curl -o docker/extra/prometheus-jmx-javaagent/jmx_prometheus_javaagent.jar \
	  https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/1.0.1/jmx_prometheus_javaagent-1.0.1.jar

.PHONY: docker_build
docker_build: build_release docker_build_prep
	cp -R core/build/distributions docker/resources/.
	# use existing docker tooling to build image
	cd docker && \
	  .venv/bin/python3 docker_build_test.py -b aivenoy/kafka --image-tag=$(VERSION) --image-type=inkless

DOCKER := docker

.PHONY: docker_push
docker_push: docker_build
	# use existing docker tooling to push image
	$(DOCKER) push aivenoy/kafka:$(VERSION)

.PHONY: fmt
fmt:
	./gradlew :core:spotlessJavaApply
	./gradlew :metadata:spotlessJavaApply
	./gradlew :storage:inkless:spotlessJavaApply

.PHONY: test
test:
	./gradlew :storage:inkless:test :storage:inkless:integrationTest
	./gradlew :metadata:test --tests "org.apache.kafka.controller.*"
	./gradlew :core:test --tests "*Inkless*"

.PHONY: pitest
pitest:
	./gradlew :storage:inkless:pitest

.PHONY: integration_test
integration_test_core:
	./gradlew :core:test --tests "kafka.api.*" --max-workers 1

.PHONY: clean
clean:
	./gradlew clean

DEMO := s3-local
.PHONY: demo
demo:
	$(MAKE) -C docker/examples/docker-compose-files/inkless $(DEMO) KAFKA_VERSION=$(VERSION)

core/build/distributions/kafka_2.13-$(VERSION): core/build/distributions/kafka_2.13-$(VERSION).tgz
	tar -xf $< -C core/build/distributions
	touch $@  # prevent rebuilds

# Local development
CLUSTER_ID := ervoWKqFT-qvyKLkTo494w

.PHONY: kafka_storage_format
kafka_storage_format:
	./bin/kafka-storage.sh format -c config/inkless/single-broker-0.properties -t $(CLUSTER_ID)

.PHONY: local_pg
local_pg:
	cd docker/examples/docker-compose-files/inkless && \
		$(DOCKER) compose up -d postgres


.PHONY: local_minio
local_minio:
	cd docker/examples/docker-compose-files/inkless && \
		$(DOCKER) compose -f docker-compose.yml -f docker-compose.s3-local.yml up -d create_bucket

.PHONY: local_gcs
local_gcs:
	cd docker/examples/docker-compose-files/inkless && \
		$(DOCKER) compose -f docker-compose.yml -f docker-compose.gcs-local.yml up -d create_bucket

.PHONY: local_azure
local_azure:
	cd docker/examples/docker-compose-files/inkless && \
		$(DOCKER) compose -f docker-compose.yml -f docker-compose.azure-local.yml up -d create_bucket

.PHONY: cleanup
cleanup:
	cd docker/examples/docker-compose-files/inkless && \
		$(DOCKER) compose down --remove-orphans
	rm -rf ./_data

# make create_topic ARGS="topic"
.PHONY: create_topic
create_topic: core/build/distributions/kafka_2.13-$(VERSION)
	$</bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --create --config diskless.enable=true --topic $(ARGS)


.PHONY: dump_postgres_schema
dump_postgres_schema:
	docker compose -f dump-schema-compose.yml up
	docker compose -f dump-schema-compose.yml down --remove-orphans
	@echo "Dumped: ./storage/inkless/build/postgres_schema.sql"
