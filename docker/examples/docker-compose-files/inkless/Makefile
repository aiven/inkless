.PHONY: in-memory
in-memory:
	docker compose up

.PHONY: s3-minio
s3-minio:
	docker compose -f docker-compose.yml -f docker-compose.s3-local.yml up

.PHONY: gcs-local
gcs-local:
	docker compose -f docker-compose.yml -f docker-compose.gcs-local.yml up

.PHONY: azure-local
azure-local:
	docker compose -f docker-compose.yml -f docker-compose.azure-local.yml up

.PHONY: s3-aws
s3-aws:
	docker compose -f docker-compose.yml -f docker-compose.s3-aws.yml up

.PHONY: destroy
destroy:
	docker compose down --remove-orphans

# make create_topic ARGS="topic"
.PHONY: create_topic
create_topic:
	docker compose exec broker \
		/opt/kafka/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --create --config inkless.enable=true --topic $(ARGS)
