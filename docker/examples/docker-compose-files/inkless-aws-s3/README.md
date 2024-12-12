# Docker compose: inkless-aws-s3

This example demonstrates how to use Inkless with remote data and metadata stores,
using a Remote Postgres instance for metadata and AWS S3 for data storage.

## Prerequisites

Set the environment variables:

For AWS S3:

```properties
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_SESSION_TOKEN=
AWS_REGION=
AWS_S3_BUCKET_NAME=
```

For Postgres:

```properties
POSTGRES_JDBC_URL=
POSTGRES_USERNAME=
POSTGRES_PASSWORD=
```

And if you'd like to make the broker accessible on a different IP:

```properties
HOST_IP=10.0.0.10
```

## Running the example

Start the broker with:
```bash
docker compose up -d
```

And if you'd like to also start the monitoring stack along the broker:

```bash
docker compose -f monitoring-compose.yml up -d
```

Then, run you Kafka applications as usual.