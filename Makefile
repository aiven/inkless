.PHONY: all
all: clean fmt test pitest

.PHONY: local_minio
local_minio:
	docker compose up -d minio minio-create_bucket

.PHONY: bucket
bucket:
	AWS_ACCESS_KEY_ID='minioadmin' AWS_SECRET_KEY='minioadmin' AWS_SECRET_ACCESS_KEY='minioadmin' aws --endpoint-url http://127.0.0.1:9000 s3api create-bucket --bucket inkless1 --region us-east-1

CLUSTER_ID := ervoWKqFT-qvyKLkTo494w

.PHONY: kafka_storage_format
kafka_storage_format:
	./bin/kafka-storage.sh format -c config/inkless/single-broker-0.properties -t $(CLUSTER_ID)

.PHONY: local_destroy
local_destroy:
	docker compose down
	rm -rf ./_data

VERSION := 4.1.0-inkless-SNAPSHOT

.PHONY: build
build:
	./gradlew :core:build :storage:inkless:build :metadata:build -x test

core/build/distributions/kafka_2.13-$(VERSION).tgz:
	./gradlew releaseTarGz

.PHONY: build_release
build_release: core/build/distributions/kafka_2.13-$(VERSION).tgz

.PHONY: docker_build_prep
docker_build_prep:
	cd docker && \
	  [ -d .venv ] || python3 -m venv .venv && \
	  source .venv/bin/activate && \
	  pip install -r requirements.txt

# download prometheus jmx exporter
docker/extra/prometheus-jmx-javaagent/jmx_prometheus_javaagent.jar:
	curl -o docker/extra/prometheus-jmx-javaagent/jmx_prometheus_javaagent.jar \
	  https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/1.0.1/jmx_prometheus_javaagent-1.0.1.jar

.PHONY: docker_build
docker_build: build_release docker_build_prep
	cp -R core/build/distributions docker/resources/.
	# use existing docker tooling to build image
	cd docker && \
	  .venv/bin/python3 docker_build_test.py -b kafka/test --image-tag=$(VERSION) --image-type=inkless

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
	./gradlew :core:test --tests "kafka.api.*Producer*Test" --max-workers 1

.PHONY: clean
clean:
	./gradlew clean

core/build/distributions/kafka_2.13-$(VERSION): core/build/distributions/kafka_2.13-$(VERSION).tgz
	tar -xf $< -C core/build/distributions
	touch $@  # prevent rebuilds

# make create_topic ARGS="topic"
.PHONY: create_topic
create_topic: core/build/distributions/kafka_2.13-$(VERSION)
	$</bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --create --config inkless.enable=true --topic $(ARGS)
