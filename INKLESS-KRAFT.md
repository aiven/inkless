1. Build Docker image:
```shell
rm -f core/build/distributions/kafka_2.13-4.1.0-inkless-SNAPSHOT.tgz && make docker_build
```

2. Run controllers:
```shell
docker compose -f docker-compose-kraft.yaml up
```

3. Run test script:
```shell
./bin/kafka-run-class.sh org.apache.kafka.tools.TestCaller 100
```
