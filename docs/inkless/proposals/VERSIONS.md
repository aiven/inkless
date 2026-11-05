# Proposal versions

Each version folder stores a stable snapshot of a design proposal. A new
iteration goes into a new folder, so earlier versions keep their original text.

## Version 1

Folder: [`v1/diskless-metadata-coordinator/`](v1/diskless-metadata-coordinator/)

Version 1 proposes a diskless metadata coordinator. It's a third `ControlPlane`
backend, `DisklessMetadataControlPlane`, that keeps diskless batch, file, and
log state in Kafka instead of Postgres. The design is a scoped-down subset of
[KIP-1164](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/350783984/KIP-1164+Diskless+Coordinator).
In short, version 1 works as follows:

- The internal topic `__diskless_metadata` stores the coordinator state. It has
  a fixed number of partitions, 8 by default.
- Each diskless user partition maps to one coordinator partition through a hash
  of its topic ID and partition index. The leader of that coordinator partition
  is the only writer for the user partition.
- Each replica of a coordinator partition keeps a local SQLite copy of that
  partition's state. The replica rebuilds it from the log and from snapshots on
  broker disk.
- A commit splits by coordinator partition, and each part succeeds or fails on
  its own, with no two-phase commit. `FileCommitter` orders commits per
  coordinator partition.
- Each WAL object has one owner coordinator partition. The owner records an
  intent before the upload and deletes the object once every participant
  releases it.
- The design doesn't add public Kafka protocol. New requests use private ApiKeys
  in the 504+ range.

`spec.md` is the handoff spec. `design.md` is the design record behind it, and
`map.md` is the decision log. `deletion-comparison.md` compares owner-based
deletion with scanner deletion for the upstream KIPs. `explainer.html` and
`walkthrough.html` explain the design visually. `notion-build/` generates the
Notion version of the design, and `.backup-2026-11-05/` keeps an earlier copy of
the Markdown files.
