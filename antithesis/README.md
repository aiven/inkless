# Antithesis tests for Inkless

The Antithesis test setup for Inkless is based on Kafka system tests. The setup runs:
- the necessary containers working as nodes for Ducktape;
- PostgreSQL;
- Minio; 
- the container with Ducktape-based test scripts ("driver").

The test entry points are scripts in `/opt/antithesis/test/v1/inkless` in the driver container. Antithesis test composer will run them in the Antithesis runtime. However, it's possible to run them as normal shell scripts (see below).

## Preparation

Generate test scripts:
```shell
make antithesis_generate_tests
```

Build Kafka/Inkless libraries for test:
```shell
make antithesis_build_for_tests
```

Build the base Ducker (sic!) image used for Kafka systests (needed only once):
```make
make antithesis_base_ducker_image
```

Build the Inkless test Docker images:
```shell
make antithesis_docker_images
```

## Running locally

Run Docker Compose:
```shell
docker compose -f antithesis/docker-compose.yaml up
```

Run a test, for example:
```shell
docker exec -ti antithesis-ducker01-1 \
  /opt/antithesis/test/v1/inkless/singleton_driver__inkless_produce_consume_test__InklessProduceConsumeTest__test_inkless__1.sh
```

To look inside Postgres:
```shell
docker exec -ti antithesis-postgres-1 psql postgresql://admin:admin@127.0.0.1:5432/inkless
```

## Running in Antithesis

Login into Antithesis Docker repository as described [here](https://antithesis.com/docs/getting_started/setup/#push-your-containers).

Push the Docker images (if needed, e.g. if changed):
```shell
make antithesis_push_docker_images antithesis_build_and_push_config_docker_image
```

Execute the test as describe [here](https://antithesis.com/docs/getting_started/setup/#test-run), for example:
```shell
curl --fail -u '<login>:<password>' \
-X POST https://aiven.antithesis.com/api/v1/launch/basic_test \
-d '{"params": { "antithesis.description":"Inkless systest",
    "antithesis.duration":"60",
    "antithesis.config_image":"us-central1-docker.pkg.dev/molten-verve-216720/aiven-repository/inkless-systest-config:latest",
    "antithesis.images":"us-central1-docker.pkg.dev/molten-verve-216720/aiven-repository/inkless-systest-driver:latest,us-central1-docker.pkg.dev/molten-verve-216720/aiven-repository/inkless-systest-vm:latest", 
    "antithesis.report.recipients":"<email>"
    }}'
```

Wait for the specified amount of minutes (or a bit longer) to receive the email. Check the run status [here](https://aiven.antithesis.com/runs).