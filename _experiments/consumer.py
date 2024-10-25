from kafka import KafkaConsumer, TopicPartition

consumer = KafkaConsumer(
    bootstrap_servers="127.0.0.1:9092",
)
consumer.assign([TopicPartition("topic", 0)])

r = consumer.poll(timeout_ms=1000)
print(r)
