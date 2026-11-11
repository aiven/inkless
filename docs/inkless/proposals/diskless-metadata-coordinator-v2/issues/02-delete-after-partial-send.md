# Broker-side deletion after a partial send

Type: grilling
Status: closed
Assignee: Viktor Somogyi-Vass
Blocked by: none

## Question

Which rule stops a broker from deleting an uploaded object once any part of its file may have reached a coordinator leader, and where does that rule live?

`FileCommitJob.tryDeleteFile` deletes the object directly on `ControlPlaneUnavailableException`, on the assumption that the commit never left the broker. v1 keeps that path and calls it safe "because no part was sent", in [Deadline and unknown outcomes](../../../docs/inkless/proposals/v1/diskless-metadata-coordinator/design.md#deadline-and-unknown-outcomes). In v1, though, `FileCommitter` calls the per-part commit once per part, and each call passes through `AvailabilityGatedControlPlane` on its own. If part 1 is sent with an unknown outcome and part 2 then hits the gate, no part has confirmed, the file fails as a whole, and the broker deletes an object that part 1 can still commit into. A delegate that the reconciler retires and replaces mid-file opens the same path.

The rule holds in both ownership models: once any part has passed the gate, only the coordinator side may delete the object. Interfaces can change if that fits better.

Settle these points:

- Where the rule lives. Options include a per-file "sent" flag in `FileCommitJob`, a backend capability that turns off the direct delete, or one gate check per file instead of one per part.
- Whether this backend ever needs the direct delete, given that every upload already has an intent or a lease that the coordinator side resolves.
- That the Postgres and in-memory backends keep today's behavior.
- The tests: a partial send followed by `Unavailable`, and a delegate replaced between two parts of one file.

## Resolution

Resolved in a grilling session on 2026-11-09.

**Where the rule lives, and whether the backend needs the direct delete:** the coordinator backend declares a capability that turns off the broker's direct delete. The tracking partition's deletion queue is the only path that deletes an object on this backend. Postgres and in-memory don't declare it, so they keep today's behavior.

Consequences:

- The capability check in `FileCommitJob.tryDeleteFile` has to run before the `ControlPlaneUnavailableException` branch. That branch deletes without calling `isSafeToDeleteFile`, so a check only inside `isSafeToDeleteFile` misses the partial-send case in this ticket's question.
- This corrects the earlier single-partition resolution in [Deletes by the broker](../../diskless-metadata-coordinator/issues/06-object-deletion.md#deletes-by-the-broker). It says the direct delete after `Unavailable` is safe because the gate rejects before the backend. That holds for a file where no part was sent, but not for a file where part 1 was sent and part 2 hit the gate.
- The cost is storage held longer for a key whose sequence never commits: the broker's next commit or the lease end marks it, then the 1-minute grace period runs. This doesn't touch the produce path.

**Tests:** both cases go in the shared suite, asserting that the broker never deletes the uploaded object directly. The coordinator backend's test adds a check that the tracking partition's queue deletes the key later, after the grace period. Postgres and in-memory keep a test that the direct delete still happens.
