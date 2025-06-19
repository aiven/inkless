DOCKER := docker

KAFKA_VERSION ?= 4.1.0-inkless-SNAPSHOT

.PHONY: in-memory
in-memory:
	docker compose -f docker-compose.yml -f docker-compose.monitoring.yml -f docker-compose.demo.yml up
	$(MAKE) destroy

.PHONY: s3-local
s3-local:
	$(DOCKER) compose -f docker-compose.yml -f docker-compose.monitoring.yml -f docker-compose.demo.yml -f docker-compose.s3-local.yml up
	$(MAKE) destroy

.PHONY: gcs-local
gcs-local:
	$(DOCKER) compose -f docker-compose.yml -f docker-compose.monitoring.yml -f docker-compose.demo.yml -f docker-compose.gcs-local.yml up
	$(MAKE) destroy

.PHONY: azure-local
azure-local:
	$(DOCKER) compose -f docker-compose.yml -f docker-compose.monitoring.yml -f docker-compose.demo.yml -f docker-compose.azure-local.yml up
	$(MAKE) destroy

.PHONY: s3-aws
s3-aws:
	$(DOCKER) compose -f docker-compose.yml -f docker-compose.monitoring.yml -f docker-compose.demo.yml -f docker-compose.s3-aws.yml up
	$(MAKE) destroy

.PHONY: destroy
destroy:
	$(DOCKER) compose down --remove-orphans

# make create_topic ARGS="topic"
.PHONY: create_topic
create_topic:
	$(DOCKER) compose exec broker \
		/opt/kafka/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --create --config diskless.enable=true --topic $(ARGS)
