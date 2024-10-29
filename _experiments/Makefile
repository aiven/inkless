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

.PHONY: compose
compose:
	docker compose -f docker-compose.yml up

.PHONY: bucket
bucket:
	aws --endpoint-url http://127.0.0.1:4566 s3api create-bucket --bucket my-bucket --region x

.PHONY: init_db
init_db:
	docker compose exec postgresql /usr/bin/psql postgresql://postgres:pwd@127.0.0.1:5432/postgres -c " \
CREATE TABLE IF NOT EXISTS logs ( \
	topic VARCHAR(255), \
	partition INT, \
	start_offset BIGINT, \
	high_watermark BIGINT, \
	PRIMARY KEY (topic, partition) \
); \
\
CREATE TABLE IF NOT EXISTS batch_coordinates ( \
	topic VARCHAR(255), \
	partition INT, \
	base_offset BIGINT, \
	file_path TEXT, \
	byte_offset INT, \
	byte_size INT, \
	number_of_records INT, \
	PRIMARY KEY (topic, partition, base_offset) \
); \
"

.PHONY: psql
psql:
	psql postgresql://postgres:pwd@127.0.0.1:5432/postgres
