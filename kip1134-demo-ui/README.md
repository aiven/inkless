# KIP-1134 demo web UI

Developer-only single-page UI backed by `KafkaAdminClient`. It connects to a Kafka cluster, manages topics, and exercises virtual cluster Admin APIs (RPC keys 501–505 in this fork).

## Run

From the repository root:

```bash
./gradlew :kip1134-demo-ui:runKip1134DemoUi
```

Optional port (default `8080`):

```bash
./gradlew :kip1134-demo-ui:runKip1134DemoUi -Pkip1134DemoPort=9090
```

Or set the system property consumed by the main class (used when no program argument is passed):

```bash
./gradlew :kip1134-demo-ui:runKip1134DemoUi -Dkip1134.demo.port=9090
```

Or use the helper script (from repository root):

```bash
./kip1134-demo-ui/bin/kip1134-demo-ui.sh 8080
```

Then open `http://127.0.0.1:8080/` (or your chosen port).

## Demo authentication (SASL without TLS)

Example broker overrides, JAAS files, and admin client properties for **SASL/PLAIN** and **SASL/SCRAM-SHA-256** over `SASL_PLAINTEXT` are under [`config/demo/`](config/demo/README.md). They are intended for local development only.

## Cluster requirements

- **Metadata version**: The cluster must support virtual cluster metadata records (same expectation as VC integration tests that use a recent testing metadata version).
- **Authorization**: If an authorizer is enabled, virtual cluster RPCs require `CLUSTER` ACLs on the literal resource `kafka-cluster` or on the named virtual cluster (for example `ResourcePattern(ResourceType.CLUSTER, "my-vc", PatternType.LITERAL)` with `CREATE`, `ALTER`, `DELETE`, or `DESCRIBE` as appropriate). Grant these with `Admin#createAcls` outside this UI.
- **Optional data-plane demo**: To exercise link-based topic and group access for principals assigned to a virtual cluster, enable `virtual.cluster.enforcement.enable` on brokers and assign users or groups through **Alter virtual cluster** in this UI (see `ServerConfigs`).

## Security warning

The server keeps broker credentials and `AdminClient` configuration **in memory** per browser session. Do not expose it to untrusted networks or use it as an operational control plane.
