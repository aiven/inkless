# Inkless Release Artifacts

Inkless is distributed as both Docker images and binary distributions.

## Docker Images

**Registry:** `ghcr.io/aiven/inkless`

### Available Tags

#### Stable Tags

| Tag             | Description                                           | When to use |
|-----------------|-------------------------------------------------------|-------------|
| `latest`        | Latest stable Inkless release                         | Production, getting started |
| `X.Y`           | Specific Inkless version (e.g., `0.33`)               | Pin to Inkless version |
| `A.B.C-X.Y`     | Kafka version + Inkless version (e.g., `4.1.0-0.33`)  | Pin to exact versions |
| `A.B-latest`    | Highest patch for a Kafka minor (e.g., `4.1-latest`)  | Track Kafka minor updates |

#### Development Tags

| Tag             | Description                                           | When to use |
|-----------------|-------------------------------------------------------|-------------|
| `edge`          | Latest development build from `main`                  | Testing latest features |
| `edge-<sha>`    | Build for a specific commit (e.g., `edge-abc1234`)    | Debugging, reproducibility |

Development tags are built automatically on every push to the `main` branch. These are unstable and not recommended for production use.

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

> **Note:** The examples below use Inkless 0.33 with Kafka 4.1.1. Check the [Releases page](https://github.com/aiven/inkless/releases) for available versions.

```bash
# Download a specific Kafka version using GitHub CLI
gh release download inkless-release-0.33 --repo aiven/inkless --pattern "*4.1.1*"

# Download a specific Kafka version using curl
curl -LO https://github.com/aiven/inkless/releases/download/inkless-release-0.33/kafka_2.13-4.1.1-inkless.tgz

# Download all Kafka versions for an Inkless release
gh release download inkless-release-0.33 --repo aiven/inkless --pattern "*.tgz"
```

## Release Branches

### Active

These branches receive new Inkless releases. When a release is cut, both are updated
simultaneously with the same Inkless version number.

See [GitHub Releases](https://github.com/aiven/inkless/releases) for the latest version on each branch.

| Branch        | Kafka version |
|---------------|---------------|
| `inkless-4.2` | 4.2.1         |
| `inkless-4.1` | 4.1.2         |

### Inactive (no longer updated)

| Branch        | Kafka version | Last release         |
|---------------|---------------|----------------------|
| `inkless-4.0` | 4.0.2         | `inkless-4.0.2-0.37` |

### Planned

| Branch        | Kafka version | Status          |
|---------------|---------------|-----------------|
| `inkless-4.3` | 4.3.x         | Not yet created |

If a commit cannot be cleanly backported across active branches, they may diverge by one
version increment (see [Versioning Strategy](VERSIONING-STRATEGY.md)).

## Cutting a Release

The release process is fully automated via GitHub Actions once the release branches are ready.

### Prerequisites

- All target inkless commits are merged to `main`.
- Active release branches (`inkless-4.1`, `inkless-4.2`, ...) have been updated with those
  commits via the cherry-pick sync process (see `inkless-sync/CHERRY-PICK-SYNC-GUIDE.md`).
- Branches have been pushed to `origin`.

### Trigger the release

Go to **GitHub Actions → Inkless Release → Run workflow** and fill in:

| Input | Description | Default |
|---|---|---|
| `inkless_version` | Version to release (e.g. `0.44`) | Auto-increments from latest tag |
| `main_commit` | Commit on `main` to release | `main` HEAD |
| `extra_branches` | Space-separated branches to add (e.g. `inkless-4.3`) | Empty — use for new branches only |

The workflow then:

1. **Resolves** the version and discovers active branches from the previous release's tags
2. **Validates** each branch — asserts the target commit is present and no inkless commits are missing
3. **Creates and pushes tags** — `inkless-release-<N>` on main and `inkless-<kafka-version>-<N>` on each branch
4. **Builds** Docker images (amd64 + arm64) and binary distributions for each Kafka version in parallel
5. **Finalizes** — creates the GitHub Release, attaches all artifacts, publishes `latest` and `X.Y-latest` Docker aliases

If validation fails (a branch is missing commits), the workflow aborts before touching any tags. Fix the
branch with cherry-pick sync and re-trigger.

### Onboarding a new release branch

When a new Kafka minor branch is ready (e.g. `inkless-4.3`), add it to the first release via the
`extra_branches` input. After that first release creates `inkless-4.3.X-<N>`, it will be auto-discovered
in future runs.

## Versioning

For details on how Inkless versions work, see [Versioning Strategy](VERSIONING-STRATEGY.md).

**Quick summary:**
- Same Inkless version (e.g., `0.33`) across Kafka versions = same Inkless features
- Higher Inkless version = newer features
- Choose Kafka version based on your compatibility needs
