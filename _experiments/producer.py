from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="127.0.0.1:9092",
)

print("Sending...")
f1 = producer.send("topic", partition=0, value=b"aaa")
f2 = producer.send("topic", partition=0, value=b"aaa")
print("Waiting...")
r1 = f1.get(30_000)
r2 = f2.get(30_000)
print("Result:", r1, r2)
