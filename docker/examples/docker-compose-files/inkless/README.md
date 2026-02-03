# Inkless Docker Compose Demo

Docker Compose examples for running Inkless with different storage backends.

See the [Quickstart guide](../../../../docs/inkless/QUICKSTART.md#dockerized-demo) for complete setup instructions.

## Quick reference

| Command               | Description                                       |
|-----------------------|---------------------------------------------------|
| `make s3-local`       | MinIO S3-compatible storage (default)             |
| `make gcs-local`      | Fake GCS server                                   |
| `make azure-local`    | Azurite Azure-compatible storage                  |
| `make s3-aws`         | Real AWS S3 (requires AWS credentials)            |
| `make destroy`        | Manual cleanup (automatic on Ctrl+C)              |

## Image version

By default, the demo pulls the `edge` image. Override with `KAFKA_VERSION`:

```bash
make s3-local KAFKA_VERSION=latest        # Latest stable release
make s3-local KAFKA_VERSION=4.1.0-0.33    # Specific version
make s3-local KAFKA_VERSION=local         # Locally built image
```
