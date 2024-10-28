from kafka import KafkaConsumer, TopicPartition

consumer = KafkaConsumer(
    bootstrap_servers="127.0.0.1:9092",
)
tp = TopicPartition("topic", 0)
consumer.assign([tp])
consumer.seek(tp, 2)

for i in range(5):
    batch = consumer.poll(timeout_ms=1000)
    print(f"Batch {i}")
    if not batch.get(tp):
        continue
    for r in batch.get(tp):
        print(r.offset, r.value)
    print()
