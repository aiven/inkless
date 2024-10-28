from kafka import KafkaProducer
import sys

producer = KafkaProducer(
    bootstrap_servers="127.0.0.1:9092",
)

for i in range(5):
    print("Sending...")
    sys.stdout.flush()
    f0 = producer.send("topic", partition=0, value=f"b{i}-0".encode())
    f1 = producer.send("topic", partition=0, value=f"b{i}-1".encode())
    print("Waiting...")
    sys.stdout.flush()
    r0 = f0.get(30_000)
    r1 = f1.get(30_000)
    print("Result:", r0, r1)
    sys.stdout.flush()
