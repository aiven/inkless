1. Format the directory
```shell
cd ..
~/kafka/kafka_2.13-3.9.0/bin/kafka-storage.sh format --config config/inkless/single-broker-0.properties --cluster-id ervoWKqFT-qvyKLkTo494w
```

2. Create and fill the topic:
```shell
gradle run --args="create"
```

3. Check it has 100 batches:
```shell
~/kafka/kafka_2.13-3.9.0/bin/kafka-dump-log.sh --files ../_data/00000000000000000000.log
```

4. Consume:
```shell
gradle run --args="consume"
```
