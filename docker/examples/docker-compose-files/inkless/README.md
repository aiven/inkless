# Inkless demo

Contains examples of how to run Inkless with Postgres as batch coordinator and different object storage back-ends using Docker Compose.

Running the make task for a setup will start the necessary services and run the Inkless demo, including topic creation, producer/consumer perf clients, and monitoring with Prometheus and Grafana.

## Run the demo

### in-memory

This setup uses an in-memory object storage and Postgres as a metadata store.

```
make in-memory
```

### S3 local

This setup uses MinIO as a local S3-compatible object storage and Postgres as a metadata store.

```
make s3-local
```

### GCS local

This setup uses `fake-gcs-server` as a local GCS-compatible object storage and Postgres as a metadata store.

```
make gcs-local
```

### Azure local

This setup uses `azurite` as a local Azure-compatible object storage and Postgres as a metadata store.

```
make azure-local
```

### AWS S3

This example demonstrates how to use Inkless with remote data and metadata stores,
using a Remote Postgres instance for metadata and AWS S3 for data storage.

#### Prerequisites

Set the environment variables:

For AWS S3:

```properties
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_SESSION_TOKEN=
AWS_REGION=
AWS_S3_BUCKET_NAME=
```

Then run

```bash
make s3-aws
```

