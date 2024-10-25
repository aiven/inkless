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
