# Council review of v2

Verbatim copy of the council review of [v2](../../diskless-metadata-coordinator-v2/design.md), as the user pasted it on 2026-11-10.

v2 closes the two sharpest v1 data-loss paths. The council still wouldn't sign off on "safe, no data loss beyond Kafka parity, and performs well" as written.

I ran only the blind review round, not the final-position round, because the five reviews already converged. There was no cross-examination and no weighted tally. Each member read different parts of v2, and most didn't read all of spec.md, the issue files, or the research folder.

## Status of the earlier findings

| # | Finding from the v1 review | v2 status |
| --- | --- | --- |
| 1 | Delete after a possible partial send | Addressed (5 of 5). A backend capability is checked before the ControlPlaneUnavailableException branch, and the tracking partition's queue is the only delete path. A named shared-suite test covers a partial send. |
| 2 | Wall-clock expiry and the long-pause path | Addressed (5 of 5). Fences and the lease floor decide whether a key is queued, with no clock involved. A wall-clock grace period remains, but it only delays a delete. Ada rates it partially addressed overall because of that grace period. |
| 3 | Local-only snapshots, loss of all replicas | Accepted risk, weakly justified. Parity holds in kind, but the blast radius isn't parity. One replica-set loss orphans 1/N of all diskless partitions, where a classic loss costs one partition. Ada says parity holds only if RF=3 plus rack spread is enforced. |
| 4 | Correlated apply bug, version gate, repair tool | Partly. The version gate and apply-stop rule are in. The repair tool is accepted as a risk. Taleb disputes the parity argument, because here the queue permanently deletes objects other partitions still reference. A group coordinator bug loses offsets, which can be reset. |
| 5 | A stalled partition stalls most produces | Partly (3 of 5). Per-queue ordering removes head-of-line blocking across files. But a file still completes only when every part answers (design.md:275, spec.md:225), and a produce waits for its slowest part (design.md:680). Story 83 (spec.md:117) still overpromises. I checked these lines myself. |
| 6 | Benchmarks, flush fallback, load shedding, guards, stale reads | Partly. Fixed: the fallback is dropped with no unsafe mode, internal-topic guards are in, and a barrier covers stale-leader reads. Not fixed: benchmark pass criteria. design.md:704 says they "need more design", and the map puts benchmarks out of scope. There is also no load shedding or retry-amplification bound. |
| 7 | Sweep, footers, partial-commit release, intent anchor | Partly. The anchor is replaced, and partial commits release through fences. The scanner is rejected and left as future work. Footers are absent: my grep for "footer" and "manifest" found nothing in design.md or spec.md. |

## New risks in v2

- Lost-tail amplification.
  - The design lists cases such as release inversion and a lowered floor after a lost fence. In those cases a lost tail on one partition can delete or resurrect another partition's data.
  - A classic lost tail truncates one partition consistently, which is different. Ada, Torvalds, Feynman, and Taleb all say parity is claimed for the cause, not the consequence.
  - The only defense is flush.messages=1 with an honest fsync on every replica. If it fails, offsets can permanently fail to read, and consumers get stuck.
  - Ada says the protocol rests on prose and needs a TLA+ or property-test model.
- Tracking partition as hub.
  - A stall there blocks the file's tracking part for that broker. The lease fallback adds a rarely exercised second code path.
  - Taleb notes each missed sequence costs N fences. A correlated failure could cause a fence storm.
- Release reports. A lost release report leaks an object quietly, and no metric is named for it (Feynman).
- Co-tenancy and fsync cost. Both are unmeasured. The follower fsync on every commit is the main Postgres-bar risk.

## What would close the gaps

The members converged on the following list, from cheapest and highest value:

1. Add a small self-describing object footer that names the batches. It keeps recovery possible without building a restore tool (Meadows, Torvalds, Taleb).
2. Require soft-delete or bucket versioning, with about 7 days of noncurrent retention. Add a per-tracking-partition deletion-rate cap and alarm (Taleb).
3. Define benchmark pass criteria, including the partial-failure produce-latency test. Add an admission cap or retry budget (all five).
4. Fix story 83 and the file-completion wording, or complete responses per batch (three members).
5. Model-check the fence, floor, and release protocol, and fault-inject a lost tail at every fsync point (Ada, Feynman).
6. State the RF=3 and rack-spread requirement as an enforced guard, not only documentation (Ada).
