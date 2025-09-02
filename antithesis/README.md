# Antithesis tests for Inkless

The [workload](workload/) is taken from https://github.com/antithesishq/kafka-workload.

## Running

Build the Inkless Docker image (if needed, e.g. if changed):
```shell
make inkless_docker
```

Build the workload Docker image (if needed, e.g. if changed):
```shell
make workload_docker
```

Run the workload:
```shell
docker compose -f single-broker-in-memory.yml up
```

Clean afterward:
```shell
docker compose -f single-broker-in-memory.yml down --remove-orphans
```
