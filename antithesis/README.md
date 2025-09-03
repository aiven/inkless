# Antithesis tests for Inkless

The [workload](workload/) is taken from https://github.com/antithesishq/kafka-workload.

## Running locally

Build the Inkless and workload Docker images (if needed, e.g. if changed):
```shell
make build_inkless_docker build_workload_docker
```

Go to the scenario directory, for example:
```shell
cd scenarios/single-broker-in-memory
```

Run the workload:
```shell
docker compose -f docker-compose.yaml up
```

Clean afterward:
```shell
docker compose -f docker-compose.yaml down --remove-orphans
```

## Running in Antithesis

Login into Antithesis Docker repository as described [here](https://antithesis.com/docs/getting_started/setup/#push-your-containers).

Push the Inkless and workload Docker images (if needed, e.g. if changed):
```shell
make push_inkless_docker push_workload_docker
```

Build and push the config Docker image:
```shell
make build_and_push_config_docker_image SCENARIO=single-broker-in-memory
```

Execute the test as describe [here](https://antithesis.com/docs/getting_started/setup/#test-run), for example:
```shell
curl --fail -u '<login>:<password>' \
-X POST https://aiven.antithesis.com/api/v1/launch/basic_test \
-d '{"params": { "antithesis.description":"Single broker, everything in memory",
    "antithesis.duration":"30",
    "antithesis.config_image":"us-central1-docker.pkg.dev/molten-verve-216720/aiven-repository/single-broker-in-memory:latest",
    "antithesis.images":"us-central1-docker.pkg.dev/molten-verve-216720/aiven-repository/inkless-workload:latest;us-central1-docker.pkg.dev/molten-verve-216720/aiven-repository/kafka:4.1.0-inkless-SNAPSHOT", 
    "antithesis.report.recipients":"<email>"
    }}'
```

Wait for the specified amount of minutes (or a bit longer) to receive the email.
