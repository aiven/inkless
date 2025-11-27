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

.PHONY: docs
docs:
	./gradlew genInklessConfigDoc genInklessTopicConfigDoc genInklessMetricsDoc

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
	$(DOCKER) compose -f dump-schema-compose.yml up
	$(DOCKER) compose -f dump-schema-compose.yml down --remove-orphans
	@echo "Dumped: ./storage/inkless/build/postgres_schema.sql"

.PHONY: antithesis_test_generator
antithesis_test_generator:
	docker build antithesis/test_generator/ -t inkless-antithesis-testgen:latest

.PHONY: antithesis_generate_tests
antithesis_generate_tests: antithesis_test_generator
	$(DOCKER) run --rm -ti -u "$(shell id -u):$(shell id -g)" -v "$(PWD):/workdir" \
		inkless-antithesis-testgen:latest \
		/workdir/antithesis/test_generator/docker_script.sh

.PHONY: antithesis_build_for_tests
antithesis_build_for_tests:
	./gradlew systemTestLibs

.PHONY: antithesis_base_ducker_image
antithesis_base_ducker_image:
	tests/docker/ducker-ak up
	tests/docker/ducker-ak down

.PHONY: antithesis_docker_images
antithesis_docker_images:
	$(DOCKER) build . -f antithesis/Dockerfile-driver \
		-t us-central1-docker.pkg.dev/molten-verve-216720/aiven-repository/inkless-systest-driver:latest
	$(DOCKER) build . -f antithesis/Dockerfile-vm \
		-t us-central1-docker.pkg.dev/molten-verve-216720/aiven-repository/inkless-systest-vm:latest

.PHONY: antithesis_push_docker_images
antithesis_push_docker_images:
	$(DOCKER) push us-central1-docker.pkg.dev/molten-verve-216720/aiven-repository/inkless-systest-driver:latest
	$(DOCKER) push us-central1-docker.pkg.dev/molten-verve-216720/aiven-repository/inkless-systest-vm:latest

CONFIG_IMAGE := 'us-central1-docker.pkg.dev/molten-verve-216720/aiven-repository/inkless-systest-config:latest'
.PHONY: antithesis_build_and_push_config_docker_image
antithesis_build_and_push_config_docker_image:
	$(DOCKER) build . -f antithesis/Dockerfile-config -t $(CONFIG_IMAGE)
	$(DOCKER) push $(CONFIG_IMAGE)
