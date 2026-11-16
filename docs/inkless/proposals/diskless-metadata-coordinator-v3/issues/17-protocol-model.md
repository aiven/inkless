# Model of the fence, floor, and release protocol

Type: prototype
Status: open
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
