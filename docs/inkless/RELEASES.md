# Inkless Release Artifacts

Inkless is distributed as both Docker images and binary distributions.

## Docker Images

**Registry:** `ghcr.io/aiven/inkless`

### Available Tags

| Tag             | Description                                           |
|-----------------|-------------------------------------------------------|
| `latest`        | Latest stable Inkless release                         |
| `X.Y`           | Specific Inkless version (e.g., `0.33`)               |
| `A.B.C-X.Y`     | Kafka version + Inkless version (e.g., `4.1.0-0.33`)  |
| `A.B-latest`    | Highest patch release for a Kafka minor (e.g., `4.1-latest` points to `4.1.1-0.33` not `4.1.0-0.33`) |
| `edge`          | Latest development build from `main`                  |
| `edge-<sha>`    | Development build for a specific commit               |

### Pulling Images

```bash
# Latest stable release
docker pull ghcr.io/aiven/inkless:latest

# Specific Inkless version
docker pull ghcr.io/aiven/inkless:0.33

# Specific Kafka + Inkless version
docker pull ghcr.io/aiven/inkless:4.1.0-0.33

# Latest for Kafka 4.1.x
docker pull ghcr.io/aiven/inkless:4.1-latest

# Development build (unstable)
docker pull ghcr.io/aiven/inkless:edge
```

### Platforms

Images are available for:
- `linux/amd64`
- `linux/arm64`

Docker automatically pulls the correct image for your platform.

## Binary Distributions

Binary distributions are available as `.tgz` files attached to [GitHub Releases](https://github.com/aiven/inkless/releases).

### Naming Convention

Binary names follow the Gradle build output:

```
kafka_2.13-<kafka-version>-inkless.tgz
```

Examples:
- `kafka_2.13-4.0.0-inkless.tgz`
- `kafka_2.13-4.1.0-inkless.tgz`

### Downloading

```bash
# Download all binaries for a release
gh release download inkless-release-0.33 --pattern "*.tgz"

# Download specific Kafka version
gh release download inkless-release-0.33 --pattern "*4.1.0*.tgz"

# Direct download via curl
curl -LO https://github.com/aiven/inkless/releases/download/inkless-release-0.33/kafka_2.13-4.1.0-inkless.tgz
```

## Versioning

For details on how Inkless versions work, see [Versioning Strategy](VERSIONING-STRATEGY.md).

**Quick summary:**
- Same Inkless version (e.g., `0.33`) across Kafka versions = same Inkless features
- Higher Inkless version = newer features
- Choose Kafka version based on your compatibility needs
