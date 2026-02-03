DOCKER := docker

# Default to edge (latest development build from main). Other options:
#   KAFKA_VERSION=latest (latest stable release)
#   KAFKA_VERSION=4.1.0-0.33 (specific Kafka+Inkless version)
#   KAFKA_VERSION=local (locally built image, requires: make docker_build from repo root)
KAFKA_VERSION ?= edge

.PHONY: in-memory
in-memory:
	KAFKA_VERSION=$(KAFKA_VERSION) $(DOCKER) compose -f docker-compose.yml -f docker-compose.monitoring.yml -f docker-compose.demo.yml up
	$(MAKE) destroy

.PHONY: s3-local
s3-local:
	KAFKA_VERSION=$(KAFKA_VERSION) $(DOCKER) compose -f docker-compose.yml -f docker-compose.monitoring.yml -f docker-compose.demo.yml -f docker-compose.s3-local.yml up
	$(MAKE) destroy

.PHONY: gcs-local
gcs-local:
	KAFKA_VERSION=$(KAFKA_VERSION) $(DOCKER) compose -f docker-compose.yml -f docker-compose.monitoring.yml -f docker-compose.demo.yml -f docker-compose.gcs-local.yml up
	$(MAKE) destroy

.PHONY: azure-local
azure-local:
	KAFKA_VERSION=$(KAFKA_VERSION) $(DOCKER) compose -f docker-compose.yml -f docker-compose.monitoring.yml -f docker-compose.demo.yml -f docker-compose.azure-local.yml up
	$(MAKE) destroy

.PHONY: s3-aws
s3-aws:
	KAFKA_VERSION=$(KAFKA_VERSION) $(DOCKER) compose -f docker-compose.yml -f docker-compose.monitoring.yml -f docker-compose.demo.yml -f docker-compose.s3-aws.yml up
	$(MAKE) destroy

.PHONY: destroy
destroy:
	$(DOCKER) compose down --remove-orphans

# make create_topic ARGS="topic"
.PHONY: create_topic
create_topic:
	$(DOCKER) compose exec broker \
		/opt/kafka/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --create --config diskless.enable=true --topic $(ARGS)
