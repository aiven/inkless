# SQLite apply and checkpoint off the shard thread

Type: grilling
Status: closed
Assignee: viktor.somogyi-vass
Blocked by: 12

## Question

Where do SQLite checkpoints run, and how many records does one SQLite transaction apply?

v2's [Local state](../../diskless-metadata-coordinator-v2/design.md#local-state) runs SQLite in WAL mode with `synchronous=NORMAL` and applies each record in its own transaction with the applied offset. Auto-checkpoint then fsyncs on the thread that applies and replies, a periodic stall on the commit path. See [Fable's checkpoint suggestion](../reviews/fable.md#sqlite-checkpoint-fsync-on-the-shard-thread). The facts are in [Fsync and checkpoint cost per commit](12-fsync-checkpoint-cost.md).

Settle these points:

- Whether auto-checkpoint is off, with checkpoints on a background thread, and how that interacts with snapshot backups and WAL growth. A separate checkpoint connection hits SQLite's WAL-reset bug below 3.51.3, or the 3.44.6 and 3.50.7 backports, so the minimum SQLite version is part of the answer.
- Whether one SQLite transaction covers every record up to a high-watermark advance, and how the applied offset, the leader epoch check, and the apply-stop rule from v2's [Record compatibility](../../diskless-metadata-coordinator-v2/design.md#record-compatibility) behave when one record in the transaction stops the apply.
- The failure-mode rows and seam tests that change.

## Resolution

Resolved on 2026-11-13 in a grilling session.

No apply thread runs a checkpoint. A checkpoint thread for the whole broker runs `PASSIVE` checkpoints on a second connection for each database, so the shard thread and the follower applier keep only the WAL header sync at each WAL restart. One SQLite transaction applies every record up to a high-watermark advance, with a savepoint per record and a 4 MiB cap per group. The follower applier gets its own thread, so the replica fetcher never writes to SQLite.

### Checkpoints

- The apply connection sets `wal_autocheckpoint=0`.
- One checkpoint thread per broker serves every coordinator database that the broker hosts, with one checkpoint connection per database. At most one checkpoint writes to the disk at a time, so checkpoints don't pile up against the log flushes.
- The thread checkpoints a database once its WAL reaches 1000 pages, SQLite's own threshold. The threshold is a constant and the only trigger. A database that stays under it never syncs. Whether the thread polls the WAL size or the apply thread wakes it through `wal_hook` is the implementer's choice, because it depends on the JDBC driver, which isn't chosen yet.
- Checkpoints run in `PASSIVE` mode only. `PASSIVE` never takes the writer lock, so it never blocks an apply.
- The bundled SQLite must be 3.51.3 or later, or 3.44.6 or 3.50.7, which carry the fix for the WAL-reset bug. A checkpoint connection beside an apply connection is the pattern that triggers the bug. The broker checks the version at startup and refuses to start the coordinator backend below it.

Rejected alternatives:

- Keeping the automatic checkpoint with a higher threshold. The stall is rarer and longer, and it stays on the commit path.
- Checkpointing on the shard thread when its queue is idle. Under load the queue is never idle, and the checkpoint still blocks the shard.
- `synchronous=OFF`. A power loss can then corrupt the database in ways that the offset-and-epoch check on open doesn't catch.
- One checkpoint thread per database. Up to `h` checkpoints sync at once against the log flushes on the same disk.
- A time-based trigger, alone or next to the page threshold. It syncs databases with little traffic, and a WAL under 1000 pages already keeps reads fast.

### WAL growth

- A `PASSIVE` checkpoint stops at the oldest open reader, and the WAL resets only after a checkpoint completes with no reader in it. A snapshot backup copies the database in one step and is the only long reader, so the WAL grows by the apply rate times the backup's duration and drops back after the backup ends, as v2 already says.
- Nothing escalates. `SqliteWalPages` shows the WAL size of each database.

Rejected alternatives:

- Escalating to `RESTART` or `TRUNCATE` above a hard limit. Both take the writer lock and wait for readers, which stalls the apply and the commit path.
- Canceling the backup above a hard limit. Under steady load the backup may never finish, so no snapshot is taken, the log start never advances, and the coordinator log grows without bound instead.

### One transaction per high-watermark advance

- The leader, the follower applier, and the loader each apply every record from the applied offset up to the new high watermark in one SQLite transaction. The runtime already merges high-watermark updates into one event on the leader. The follower groups up to the high watermark of each fetch response, and the loader groups as it replays.
- Each record runs under its own `SAVEPOINT`. When a record stops the apply, whether it's unreadable, an impossible file-committed or sequences-leased record, or a record for a user partition with no partition-created row from [Image edge cases](03-image-edge-cases.md), the applier runs `ROLLBACK TO` on that record's savepoint. It then commits the group with the offset and epoch of the record before it. SQLite holds every record before the stop and none from the stop onward.
- The leader replies to the records before the stop and then resigns, as v2 says. The records after the stop fail with `NOT_COORDINATOR`.
- The transaction stores the last applied record's offset and epoch. The open check and the snapshot tag read that row, so both stay exact at group boundaries.
- No-op records still advance the applied offset, and the emptiness check stays in the transaction that removes the batches.
- A power loss rolls back whole groups, and the replica replays them from the log.

Group size:

- A group closes before the first record that would push it past 4 MiB of log bytes, and the next group starts with that record. A record larger than the cap gets a group of its own. The cap is a constant.
- The cap counts bytes for the same reason v2's snapshot trigger does: one file-committed record can carry hundreds of batches. 4 MiB is about the 1000-page checkpoint trigger, so one transaction can push the WAL to at most about twice the trigger before it ends and lets the WAL reset.

Rejected alternatives:

- A decode pass before the transaction, with no savepoints. It catches unreadable records only, not the three stop cases that depend on state.
- An uncapped group. A new leader, a returning follower, or a restarting loader can cover a large span of the log, and one long transaction keeps the WAL from resetting and holds every reply in the group.
- A record-count cap. Bytes per group then vary with the batches per record.
- A config for the cap. Nothing points at an operator need to tune it.

### Threads

- The leader's apply stays on the shard thread. It removes overlay entries and replies, and a commit reads SQLite plus the overlay on that thread.
- One follower-applier thread per broker serves every coordinator database that the broker follows. The replica fetcher only appends bytes and moves the high watermark. The applier reads from the local log up to the high watermark and applies in groups. Without checkpoints, an apply syncs only at a WAL restart, so one thread is enough.
- A follower whose applier falls behind stays in the ISR. If it becomes leader, the high-watermark gate holds commits until it catches up. `ApplyLagBytes` shows the lag.

Rejected alternatives:

- Applying on the replica fetcher thread. SQLite writes then delay the next fetch for every partition from that leader, classic ones included, and a slow fetcher drops the follower from the ISR.
- Running follower applies on the `CoordinatorRuntime` threads. They compete with the leader shards' commits, and the runtime would have to load shards for followers.

### Metrics

Both are gauges with a `partition` tag:

- `SqliteWalPages`: the WAL size of each coordinator database, in pages.
- `ApplyLagBytes`: the log bytes between the high watermark and the applied offset.

### Failure-mode rows

| Failure | Effect | Recovery | Test |
| --- | --- | --- | --- |
| A snapshot backup or a slow disk keeps the checkpoint behind, or a checkpoint fails | Applies continue. The WAL grows until the reader ends or a checkpoint succeeds, and `SqliteWalPages` rises. | The checkpoint thread logs the error and retries on its next pass. | Seam: SQLite checkpoint |
| A record stops the apply in the middle of a group | SQLite commits every record before it, and the applied offset is the record before the stop. Replies before the stop complete, and the leader resigns. | As v2's row for a record a replica can't read. | Backend: apply stop |
| Power loss or a crash during a group | The whole group rolls back. | The replica replays from the previous applied offset. v2's power-loss row changes from record to group granularity. | Cluster: unclean restart of every replica |
| The bundled SQLite is older than 3.51.3 and isn't 3.44.6 or 3.50.7 | The broker refuses to start the coordinator backend. | Ship a build with a fixed SQLite. | Seam: SQLite version check |
| The follower applier falls behind | The replica stays in the ISR, and `ApplyLagBytes` rises. | If it becomes leader, the high-watermark gate holds commits until it catches up. | Cluster: failover mid-commit |

### Tests

Placement in the test layers belongs to [Consequences for the cross-cutting sections](18-cross-cutting-consequences.md):

- Seam: SQLite checkpoint, new:
  - The apply connection has `wal_autocheckpoint=0`.
  - With the checkpoint thread paused, applies past 1000 pages run no checkpoint on the apply thread.
  - With a backup held open, applies keep their latency, and the WAL drops back after the backup ends.
  - The cap splits a group at 4 MiB, and a record larger than the cap gets a group of its own.
- Seam: SQLite version check, new: a version below the floor that isn't a backport refuses to start, and 3.44.6, 3.50.7, and 3.51.3 pass.
- Backend: apply stop, extended: a stopping record in the middle of a group commits the records before it and none after, on the leader, the follower applier, and the loader.
- Cluster: failover mid-commit, extended: with the follower's applier paused, the follower stays in the ISR, and after it becomes leader, commits wait for the gate.

### Consequences for the rewrite

- `design.md` Local state: the checkpoint thread, `PASSIVE` mode, the 1000-page trigger, the SQLite version floor, one transaction per high-watermark advance with savepoints, and the 4 MiB cap replace "each apply stores the applied offset" per record. The power-loss bullet changes to group granularity.
- `design.md` Write path: the follower applier runs on its own thread per broker, never on the replica fetcher.
- `design.md` Record compatibility and Apply rules: a stop inside a group rolls back to the record's savepoint.
- `design.md` Metrics gains `SqliteWalPages` and `ApplyLagBytes`. Failure modes gains the five rows, and Testing gains the two seam tests and the two extensions.
- Open items: the SQLite driver choice must ship a bundled SQLite at or above the floor, and the implementer picks polling or `wal_hook` for the checkpoint trigger.
