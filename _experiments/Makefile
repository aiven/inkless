.PHONY: topic
topic:
	~/kafka/kafka_2.13-3.8.0/bin/kafka-topics.sh \
	  --bootstrap-server 127.0.0.1:9092 \
	  --create \
	  --topic topic

.PHONY: producer
producer:
	~/kafka/kafka_2.13-3.8.0/bin/kafka-console-producer.sh \
	  --bootstrap-server 127.0.0.1:9092 \
	  --topic topic

.PHONY: localstack
localstack:
	docker compose -f docker-compose.yml up

.PHONY: bucket
bucket:
	aws --endpoint-url http://127.0.0.1:4566 s3api create-bucket --bucket my-bucket --region x
