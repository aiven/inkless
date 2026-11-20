# Model of the fence, floor, and release protocol

Type: prototype
Status: closed
Assignee: viktor.somogyi-vass
Blocked by: 01, 02, 03, 05, 06

## Question

Does the fixed lease, fence, floor, release, and deletion-queue protocol keep its safety invariants under every interleaving and under a lost tail at each fsync point?

Ada and Feynman ask for a model check and fault injection of a lost tail at every fsync point. See the council's [lost-tail risk](../reviews/council.md#new-risks-in-v2) and [fifth fix](../reviews/council.md#what-would-close-the-gaps). Fable says its defects 1, 2, and 4 and the release ordering are what such a model catches. See [Fable's comparison](../reviews/fable.md#how-id-combine-them).

Build one bounded prototype, a small TLA+ spec or a deterministic property test, at a scale such as two or three coordinator partitions and one or two brokers. It covers leases and fallback, commits, fences, floors, releases, epoch retirement, idle lease ends, and the deletion queue, as the blocking tickets fixed them.

- Invariants: no object is deleted while a batch references it, and no part commits into a queued or deleted key.
- The model must first reproduce Fable's defects against v2's rules. That shows it's strong enough to trust. It must also reproduce v2's missed-sequence race: a missed-sequence fence that fails a part still retrying at another partition.
- The model covers finish reports, the `Recorded` flag, and the deferred missed-sequence and lease-end fences from [When the tracking partition fences silent participants](01-silent-participant-fences.md).
- Fault injection: drop the unflushed tail of any coordinator partition at each fsync point, and record which invariants fail. Under the map's clause 3, each failure feeds [Deletion bounds](10-deletion-bounds.md) or reopens a fix.
- The model covers keepalives, the switch to a fallback range, lease releases scoped to a range end, and early flushes of urgent finish reports from [Lease lifecycle at the broker](02-lease-lifecycle.md). It must show that a late release never fences a newer range.
- The model covers the entry states, stored early reports, and batched reports from [Release reports: ordering, batching, and a leak signal](05-release-reports.md). It checks two more invariants: no event moves an entry out of released, and a participant never holds a batch of a sequence it has released. It must also reproduce Fable's release-ordering leak against v2's rules.
- The model covers batched fences with hull-merged ranges and the broker-wide `finishedThrough` from [Fence fan-out under correlated failure](06-fence-fan-out.md). It must reproduce the cross-partition race against the per-partition definition: after a switch back to `t`, a missed-sequence or lease-end fence from `t` fails a lower-sequence part under `t + 1` that is still retrying at a participant.
- The model doesn't cover an empty restart of a coordinator partition. [Empty restart of a coordinator partition](19-empty-restart.md#consequences-if-a-later-version-builds-it) is future work and lists what the model gains if a later version builds it.
- Link the model as an asset. The full model and the fault-injection suite go into the spec's testing decisions as implementation work.

## Findings

This section records the three specs, their results, and the protocol changes they found. The resolution at the end gists them.

### Model

The model is three TLA+ specs checked by TLC. They live in [model/](../model/):

- `FencesFloors.tla`: one broker and coordinator partitions `P`. It covers leases, the switch and fallback, commits, late attempts, finish reports, fences, floors, releases, idle and epoch lease ends, retention removals, and the deletion queue. Each v3 fix is a flag: `FixFinish` (01), `FixLease` (02), `FixBrokerWide` (06), `FixPlannedWait`, and `FixMissedFirst`. The last two come from this ticket. `HullMerge` turns on the merged fences from 06.
- `FFScenarios.tla`: pins the broker's lease and file choices to one named race, and TLC checks every interleaving within it.
- `TrackingEntries.tla`: one broker, tracking partition `t`, and participants `P`, over one leased range. Fences, fence answers, release reports, and late attempts are separate messages that any interleaving can reorder, and a release report can repeat. Its flags are `FixTerminal` and `FixStoreEarly` (05), and `FixResolvedFloor` from this ticket. For more information, see [Tracking entries](#tracking-entries).
- `EpochRetirement.tla`: one broker ID, tracking partition `t`, and participant `p`, each with its own leader image that can lag the metadata log. Its `Rule` constant selects v2's rule, Fable's version, or ticket 03's `retiredThrough`. For more information, see [Epoch retirement](#epoch-retirement).
- `run-all.sh`: runs every check unattended and writes `runs/<timestamp>/summary.md`. The results of the latest run are in `model/runs-gcp/20261119-150953/`.

The model checks these invariants:

- `NoDeleteReferenced`: no object is deleted or queued while a batch references it, which also rules out a commit into a queued or deleted key.
- `NoCutLive`: no fence raises a floor past a part the broker still sends, or past a sequence it still plans to use.
- `NoLeak`: once an epoch ended and the coordinator is quiet, every uploaded key that no partition holds is queued.
- `EndedKnown`: once the lease covering a finished file ends, its tracking partition knows the key. It fails at the same gap as `NoLeak`, but within a few steps.

The model makes these abstractions:

- One append is atomic with its apply and its reply, and the overlay isn't modeled.
- A fence and its answer are one step, so an answer always reflects the participant's state. The late-answer races belong to the tracking-entries spec.
- Any set of pending fences to one participant and epoch can merge into their hull. This includes each batch that 06's rule of one request in flight per participant forms.
- Time is untimed. A file's deadline passes only after every earlier file finished. With `FixLease`, the idle end fires only for a paused or cut-off broker.
- There's one broker and no tracking leader change.

The bounds are 3 partitions, ranges of 2 sequences, up to 4 files, 1 late attempt in flight, 1 pause, and 1 or 2 epochs.

### Results

Each fix with its flag off reproduces the race it closes, within 2 seconds:

| Scenario | Fix off | Race |
| --- | --- | --- |
| A, an empty tracking part | `FixFinish` | Fable's defect 1 |
| B, a missed sequence | `FixFinish` | v2's missed-sequence race |
| C, a fallback at a low rate | `FixLease` | Fable's defect 2 |
| D, a switch back to `t` | `FixLease` | Fable's defect 2 after a switch back |
| E, a cross-partition part | `FixBrokerWide` | 06's cross-partition race |
| F, a lost lease reply | `FixPlannedWait` | A release cuts a planned sequence |
| H, an abandoned range | `FixMissedFirst` | An orphaned object leaks |

With every fix on and merged fences, no invariant fails:

- A, B, F, and H explore every reachable state.
- C, D, E, and G (an epoch end) reach the 1-hour limit at 87 to 123 million states and depths 19 to 22.
- The random simulation of the unpinned model finds nothing, but it draws too few traces to add much.

Every scenario reaches a state with two pending fences to one participant and epoch, so merges happen. In A, B, F, and H, a merge has the same effect as applying the fences one after another, so their state counts don't change. Only E reaches an entry between two merged ranges, at depth 10, which is the case 06's rule for the answers in between covers.

### Protocol changes

`FencesFloors` found two gaps in v3 as the tickets wrote it, and `TrackingEntries` found a third, described in [Tracking entries](#tracking-entries). All three fixes are decided, and the rewrite takes them:

- A lease release waits until no planned sequence is at or below `b`. When a renewal gets no answer, the broker releases the requested range, scoped to its `b`. The release's lease-end fence runs from the sequence after `finishedThrough` to `b`, and a participant keeps one floor per broker. The fence therefore covers sequences that the broker still plans to use in its live range at another partition, and their parts fail at participants. The broker-wide wait from 06 covers files, not sequences that no file uses yet. This change adds a condition to the release wait in [Lease release](02-lease-lifecycle.md#lease-release).
- A sequence in a range leased to the broker at `t` is missed once a recorded `finishedThrough` reaches it while `t` has no record of it. It no longer needs a later recorded sequence. The tracking partition applies this rule before any lease end in the same record. This change replaces "detects a missed sequence when it records a later one" in [Fences](01-silent-participant-fences.md#fences).

  Before this change, the following path leaked an object. The broker's tracking part at `t` fails, the broker switches to another partition, and the release reaches `t` with `finishedThrough` at or past the sequence. No later record at `t` makes the sequence missed, and the lease-end range starts at `finishedThrough + 1`, so nothing fences the key. Fencing at the report, rather than widening the lease end, was chosen because keepalives can hold a lease open for a long time.

### Lost tail

The fault drops any suffix of acknowledged records at one coordinator partition, once in each behavior. Storage keeps its deletes.

Every lost-tail run first hits release inversion: a lost retention removal brings back a batch whose object is gone. Resurrection is the other known path. Both are accepted risks in [Accepted risks in v3](10-deletion-bounds.md#accepted-risks-in-v3), and both hit only the partition that lost its tail. The lost-tail runs therefore exempt that partition from `NoDeleteReferenced` and fail only if a lost tail hits another partition, a path that ticket 10 says doesn't exist.

No lost-tail run finds such a path. Every run reaches the 1-hour limit at 243 to 330 million states and depths 17 to 22. Because the fence and its answer are one step, this result doesn't cover a stale answer that survives a lost tail. The tracking-entries spec covers it.

### Tracking entries

`TrackingEntries` checks the entry states from ticket 05 against every order of fences, answers, release reports, and late attempts. Leases, switches, and floors across partitions stay in `FencesFloors`. The tracking partition isn't a participant, fences aren't hull-merged, and each release report carries one sequence. [Batched release reports](#batched-release-reports) explains why batching doesn't need a model. The bounds are 2 participants, 2 sequences, and 1 late attempt in flight.

It checks `NoDeleteReferenced`, `NoCutLive`, and `NoLeak` as `FencesFloors` does, and three more invariants:

- `ReleasedTerminal`: no event moves an entry out of released.
- `NoHoldAfterRelease`: a participant never holds a batch of a sequence it has released.
- `NoEntryForDeleted`: no entry outlives its deleted object.

Each fix with its flag off fails within seconds:

| Fix off | Invariant | Path |
| --- | --- | --- |
| `FixTerminal` | `ReleasedTerminal` | A late "referenced" answer overwrites released. This is Fable's release-ordering leak. |
| `FixStoreEarly` | `NoLeak` | The tracking partition drops an early report and accepts it. The finish report then moves the entry to held, and nothing releases it. |
| `FixResolvedFloor` | `NoEntryForDeleted` | A resent report recreates the entry of a deleted object. |

With every fix on, TLC explores every reachable state, 13.7 million states to depth 25, in under 4 minutes. No invariant fails.

With one lost-tail fault, the faulted partition is exempt as in `FencesFloors`. After the fault, only `NoDeleteReferenced` and `NoHoldAfterRelease` are checked, at the other partitions. A 13.6-hour run on 16 cores finds no violation. It explores every behavior up to depth 16 and part of depth 17, 5.0 billion distinct states, then fills the 941 GB disk with 3.6 billion states still queued. It covers only the shorter behaviors, because the run with every fix on needs depth 25. The results are in `model/runs-gcp/20261118-111024/`.

The spec found a third gap, and its fix is decided. Ticket 05 classifies a report for a sequence with no entry against the range's highest recorded sequence. A missed-sequence fence or a lease end can resolve a sequence above it:

1. A participant's part commits with every batch failed, so it releases at once, and the tracking part fails.
2. The lease ends, its fence resolves the sequence, and the object is deleted.
3. The participant's reply was lost, so it resends the report.
4. The sequence is above the highest recorded sequence, so the tracking partition stores it as an early report.

No record or fence ever clears the new entry. It's a stuck row, not data loss, and the gauges from 05 don't count it.

In the rewrite, the bound is the higher of the range's highest recorded sequence and the tracking partition's own fenced-through pair. Both the missed-sequence fence and the lease end raise that pair. This change replaces "the highest sequence it has recorded in that range" in [Reports for unrecorded sequences](05-release-reports.md#reports-for-unrecorded-sequences), and the table rows that use it.

#### Batched release reports

A batched report reaches only states that the spec already checks, so it doesn't need a model. In [Batching](05-release-reports.md#batching), the tracking partition applies a whole report in one transaction and one record. The participant appends one release-accepted record per report, and a retry resends the same report. The tracking partition classifies each sequence from its own entry, the range's highest recorded sequence, and the fenced-through pair. A release report changes neither of the last two. It removes another sequence's entry only when it queues that object, and a queued object has no pending participant left. So each sequence in a batch gets the classification it gets alone, and the state after the batch equals the state after its sequences arrive one after another with no event in between. The spec reaches that order. The following parts of the protocol add nothing new:

- A lost reply to a batch is a lost reply to each of its sequences.
- A lost tail drops the batch's record as a whole. The spec can also drop part of the sequences.
- The rule of one report in flight per tracking partition only removes interleavings.

The argument needs two rules that ticket 05 doesn't state, and the rewrite adds both to [Batching](05-release-reports.md#batching):

- A report's ranges cover only sequences that the participant released. Unlike a fence, a report can't merge into its hull. If a participant released sequences 1 and 3 and still holds 2, a range from 1 to 3 releases 2 while a batch references it.
- The tracking partition answers each `(brokerId, brokerEpoch)` group separately. It applies the valid groups and returns the non-retriable error only for a group outside its leased ranges. If the tracking partition rejected the whole report, the valid releases in it would never get a release-accepted record, and their entries would leak.

### Epoch retirement

`EpochRetirement` checks ticket 03's broker-epoch-retired record. A broker registers, restarts, unregisters, and registers again under one ID. Each partition's leader acts on its own image, and an election can give the new leader an older image. Lease requests, commit parts, and fences can arrive late or repeat. Sequences aren't modeled: each epoch has one leased range, and a lease end fences the whole epoch. The overlay isn't modeled either. The bound is a metadata log of 5 records.

It checks these invariants:

- `LiveNotRetired`: the broker's registered epoch is never stale at any partition, and its lease never ends.
- `NoDeleteReferenced`: no object is deleted while a partition holds its batch.
- `RetireComplete`: once `t`'s image is current and `t` has nothing left to retire, no lease of a dead epoch remains.

`LiveNotRetired` also covers Fable's defect 4, where a re-registered broker's new epoch is stale. TLC reports the shortest violation, which is ticket 03's third case. The checks reproduce these defects:

| Rule | Invariant | Path |
| --- | --- | --- |
| v2 | `LiveNotRetired` | `t` grants a lease at epoch 1, and its image at offset 0 shows no registration. The retired marker rejects the live broker forever. This is ticket 03's third case. |
| Fable's version | `LiveNotRetired` | The same path. `retiredThrough` becomes the newest held epoch, 1, which is the live epoch. |
| No retirement | `RetireComplete` | A dead epoch's lease never ends. This run checks that the invariant can fail. |

With ticket 03's rule, TLC explores every reachable state, 506 million states to depth 38, and no invariant fails. Without the guard that appends only when `retiredThrough` covers the newest held epoch, it also passes, at 724 million states. The results are in `model/runs-gcp/20261119-004930/`. Because epochs are metadata offsets, v3 never computes a `retiredThrough` that reaches a live epoch, so the guard is defense in depth. The guard also lists the overlay, which the model doesn't cover.

The spec also shows that retirement at a participant isn't needed for safety. A lease end at `t` fences the participants before any leftover object is deleted, so a late part from a retired epoch fails on the floor. Retirement at a participant keeps its acceptance rules tidy and frees a dead broker's row.

#### A Kafka dependency

The rule for an image with no registration relies on how the controller writes a registration. `QuorumController` sets the epoch to the offset of the first record the registration writes. For a new incarnation, `ClusterControlManager.registerBroker` first writes a `PartitionChangeRecord` for each partition where the old incarnation is in the ISR or ELR, then the `RegisterBrokerRecord`. The result isn't atomic, so it can span several batches, and an image can end between them.

On a restart, that gap is harmless: the image still shows the old registration, and v3 retires through the old epoch. With no prior registration, unregistration has already removed the broker from every ISR and ELR, so the `RegisterBrokerRecord` is the only record, at the epoch's offset. If a later Kafka version writes a record ahead of it in that case, v3 retires live brokers. With `PreWhenUnreg` on, the model allows that record, and `LiveNotRetired` fails in 6 steps.

The rewrite takes two changes from this finding:

- Narrow ticket 03's sentence "An image at offset `X` has therefore seen every registration at an epoch at or below `X`" to registrations of a broker with no prior registration.
- Add a test that pins the controller behavior. It registers a broker, makes it a replica in an ISR, unregisters it, and registers it again. It asserts that the `RegisterBrokerRecord` is the registration's first record, at the offset equal to the new epoch. If an upstream sync changes this behavior, the test fails. If that ever happens, the fallback is to retire on proof: retire through the epoch of an unregistration that the leader's image showed, and let the idle lease end from 02 cover a leader that loaded a snapshot.

### Switches driven by the assignment

[Tracking part under gateways](22-tracking-part-under-gateways.md) makes a broker switch its tracking partition when it loses that coordinator partition in the gateway assignment, about 4 times per epoch over a rolling restart near saturation. These switches need no new model work. `RequestLease(q)` is enabled for any partition whenever no renewal runs and the broker isn't paused, with no failure precondition, so the model already explores every such switch. Ticket 22's trigger and ordered fallback list only narrow when and where the broker switches, and narrowing explored behavior can't break a safety invariant. Scenarios D, E, and H pin the shapes that ticket 22 makes common: a switch away and back with a late release, a tracking partition that is also a participant while a part under another range retries, and a tracking partition that is the only participant before a switch. A fourth switch in an epoch, beyond the bound of 3 lease requests, repeats per-partition release interactions, because each release is scoped to its own range end at its own partition.

## Resolution

Resolved on 2026-11-20 in a prototype session.

Within its bounds, the fixed protocol keeps every safety invariant under every interleaving. A lost tail damages only the partition that lost it, through the two paths that ticket 10 accepts. The asset is the model in [model/](../model/): three TLA+ specs and `run-all.sh`, with the results of the full runs in `model/runs-gcp/`. For the details, see [Findings](#findings).

- Strength: against the old rules, the model reproduces Fable's defects 1, 2, and 4, Fable's release-ordering leak, v2's missed-sequence race, and 06's cross-partition race, each within seconds.
- v3: no invariant fails. TLC explores every reachable state of scenarios A, B, F, and H with merged fences, of `TrackingEntries`, and of `EpochRetirement` at 5 records. C, D, E, and G stop at the 1-hour limit at depths 19 to 22.
- Lost tail: the only damage is release inversion and resurrection at the faulted partition. No run finds a path to another partition, up to depth 17 to 22 in `FencesFloors` and through depth 16 in `TrackingEntries`.
- Protocol changes: a lease release also waits until no planned sequence is at or below `b`, a recorded `finishedThrough` marks a sequence missed without a later record, and a report for a sequence with no entry is classified against the fenced-through pair too. [Lease lifecycle at the broker](02-lease-lifecycle.md), [When the tracking partition fences silent participants](01-silent-participant-fences.md), and [Release reports](05-release-reports.md) carry "Amended by" pointers.
- Kafka dependency: ticket 03's `retiredThrough` rule needs the controller to write a new registration of a broker with no prior registration as its first record. The rewrite narrows ticket 03's sentence, a test pins the behavior, and retiring on proof is the fallback. [Image edge cases](03-image-edge-cases.md) carries the pointer.
- Batched release reports don't need a model. A report's ranges cover only released sequences, and the tracking partition answers each `(brokerId, brokerEpoch)` group separately.
- Out of the model: the overlay, a tracking leader change, more than one broker, and the empty restart from [Empty restart of a coordinator partition](19-empty-restart.md). The guard on `retiredThrough` stays in v3, because it also covers the overlay.

### Failure rows

| Failure | Behavior | Test |
| --- | --- | --- |
| A lease renewal gets no answer while the broker plans sequences in the requested range | The broker switches, and its release of the requested range waits until no planned sequence is at or below `b`. No part fails on the lease-end fence. | Named test 1 |
| A broker's tracking part fails, and it switches before a later record at `t` | `t` marks the sequence missed when the release's `finishedThrough` reaches it, and fences it before the lease end. The object is queued. | Named test 2 |
| A participant resends a release report after a lease end resolved its sequence | The tracking partition accepts the report as a no-op and doesn't store an entry. | Named test 3 |
| A batched release report holds a group outside the leased ranges | The tracking partition applies the valid groups and rejects only that group. `ReleaseReportRejectedRate` rises. | Named test 4 |
| An upstream change makes the controller write a record ahead of a new registration | The pinning test fails at the upstream sync, before v3 retires a live broker. | Named test 5 |

### Tests

Placement in the test layers belongs to [Consequences for the cross-cutting sections](18-cross-cutting-consequences.md):

1. A planned sequence and a lost lease reply: the broker's renewal at `t` gets no answer, and it switches to `t + 1` while it still plans to use a sequence at or below the requested range's `b`. The broker sends the release only once no planned sequence is at or below `b`, and every part under that sequence commits.
2. An abandoned range with a failed tracking part: the tracking part at `t` fails, the broker switches, and the release reaches `t` with `finishedThrough` at the sequence. `t` fences the sequence as missed, the participants answer, and the object is queued.
3. A resent report after a lease end: an all-fail participant releases, the tracking part fails, and the lease end resolves the sequence and deletes the object. The participant resends the report, and the tracking partition doesn't store an entry.
4. A batched report with one bad group: the report holds valid groups and a group outside the leased ranges. The valid groups apply in one record, the bad group gets the non-retriable error, and `ReleaseReportRejectedRate` rises. A participant that released sequences 1 and 3 and still holds 2 sends two ranges.
5. The controller's registration order, a controller test: a broker registers, joins an ISR, unregisters, and registers again. The `RegisterBrokerRecord` is the registration's first record, at the offset that equals the new epoch.

The testing decisions take the model as implementation work: the three specs and `run-all.sh` move into the repo, and a fault-injection suite drops the unflushed tail of a coordinator partition at each fsync point in the integration layer.

Follow-ups:

- [Consequences for the cross-cutting sections](18-cross-cutting-consequences.md): the five failure rows, the five tests, and the model in the testing decisions.
- [When the tracking partition fences silent participants](01-silent-participant-fences.md), [Lease lifecycle at the broker](02-lease-lifecycle.md), [Image edge cases](03-image-edge-cases.md), and [Release reports](05-release-reports.md): the rewrite applies the amendments that their pointers name.
