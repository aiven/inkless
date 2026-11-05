# Deleting WAL objects: owner-based deletion and scanner deletion

Status: decided for [design.md](design.md). Written to carry into the upstream KIPs.

## Context

A diskless coordinator that splits its state across several coordinator partitions assigns offsets per user partition, so each user partition keeps one writer. A WAL object still carries batches for many user partitions, so its batches land on several coordinator partitions. No single coordinator partition can tell when nothing references the object anymore. The cluster also needs a way to remove objects that no commit ever referenced, such as an upload that finished right before its broker crashed.

Two designs solve this:

- Owner-based deletion: one coordinator partition owns each object. It records the object before the upload, collects the release of every other coordinator partition, and deletes the object once all of them have released it.
- Scanner deletion: a background job lists the objects in storage, asks every coordinator partition whether it knows each key, and deletes the objects that none of them knows.

Both designs depend on one shared rule: a maximum commit age. Every commit part carries the time its broker first sent it, and a coordinator partition rejects a part older than the limit. Without that rule, neither design can tell a forgotten object from one whose commit is still in flight.

## Owner-based deletion

- The participants of an object are the coordinator partitions that its batches map to. The broker picks the owner among them by hashing the object key. If that participant can't take the intent, the broker tries the next participants in sorted order.
- Before the upload starts, the broker stores an intent at the owner: the object key, the participants, and the owner. If the intent fails, the broker uploads nothing. Every object in storage therefore has an intent recorded first.
- Every commit part names the owner. A participant that later holds no batch of the object reports its release to the owner, and records when the owner accepts the report.
- The owner fences only when a fence can unblock deletion. Once the intent is older than the maximum commit age, and the owner and every participant it has heard from are released, the owner fences the participants that are still silent. A fence is final: the participant either holds a commit for the key and answers "referenced," or it now rejects any commit for the key and counts as released.
- The owner deletes the object after every participant is released and the intent is older than the maximum commit age. A delete of a missing object counts as success.
- A fence answer names the owner recorded in the committed part. A coordinator partition that holds a stale intent for an object owned elsewhere drops its entry without deleting the object.

## Scanner deletion

- Each coordinator partition tracks only its own batches and forgets an object when it holds none of its batches.
- A scanner lists the objects in storage. For each object older than the maximum commit age plus clock skew, it asks every coordinator partition whether it knows the key, including pending entries that are not yet applied.
- An object that no coordinator partition knows becomes a candidate. The scanner deletes it if it is still unknown on a later pass, at least one deletion grace period after the first. The second pass protects readers that still hold cached batch coordinates.
- Any answer other than a clear "unknown," such as `NOT_COORDINATOR`, a leader that is still catching up, or a timeout, means the scanner skips the object for that pass.

The scanner needs no lock and no pause. Only a commit creates a reference to an object. The age check rejects any commit for an object older than the maximum commit age, so references to the objects the scanner examines can only disappear. An "unknown" answer for such an object stays true, and the scanner can ask the coordinator partitions one after another, at different moments, with no consistent snapshot across them.

## Comparison

### Direction of failure

This is the deciding difference.

- Owner-based deletion fails toward a leak. It deletes only objects it recorded and resolved. A bug or a lost record leaves an object in storage.
- Scanner deletion fails toward data loss. It deletes what no coordinator partition knows. If a coordinator partition answers "unknown" when it shouldn't, live data is deleted. For example, a replica rebuilds from a stale snapshot, a restore rolls coordinator state back, two clusters share a bucket prefix, or an operator configures the wrong prefix. In each case the scanner deletes every object the coordinator state doesn't know, possibly a large share of the cluster's data in one pass.

### Dependence on clocks

- Owner-based deletion: fences make the decision to delete, so deletion safety doesn't depend on clocks. Clocks decide only when tombstones and fences expire, and when the owner starts fencing.
- Scanner deletion: deletion is safe only if broker clocks, coordinator leader clocks, and the storage timestamp of each object agree to within the maximum commit age. A broker clock running ahead by more than that limit lets a delayed commit through after the scanner checked. That commit then points into a deleted object.

### Cost

- Owner-based deletion: the cost follows the commit rate. Each object adds one intent record at its owner, and one release report and one acceptance record per participant. A participant gets at most one fence per object, and only if it is still silent once the owner and every other participant it has heard from are released.
- Scanner deletion: the cost follows the number of live objects. Each pass lists every object and looks every key up on all coordinator partitions, because the scanner doesn't know an object's participants. As an illustration, 40 objects per second with seven days of retention keeps about 24 million objects live. An hourly pass then makes about 24,000 list requests and about 190 million key lookups across 8 coordinator partitions. Encoding the participants in the object key would cut the lookups. Sweeping each time window once, instead of every live object on every pass, would make the cost follow the upload rate.

### Deletion latency

- Owner-based deletion: the owner deletes an object soon after its last release, plus the deletion grace period. A never-committed object waits until its intent is older than the maximum commit age.
- Scanner deletion: an object waits up to one scan interval plus the grace period after its last batch is removed.

### Produce path

- Owner-based deletion: the intent adds one replicated append, including its fsync, before every upload. An owner that is down blocks the objects it would own, unless the broker falls back to another participant.
- Scanner deletion: nothing is added to the produce path.

### Architecture

- Owner-based deletion: coordinator partitions exchange release reports and fences. The design adds intent, release, acceptance, fence, and expiry records, an owner timer, and owner fallback.
- Scanner deletion: coordinator partitions stay independent, and one background job owns deletion. Brokers need permission to list the bucket, and either one broker runs the job or several brokers split the key space between them. A normal object listing also misses incomplete multipart uploads, so those still need storage lifecycle rules.

### What each needs from object storage

- Both: deleting a key removes whatever is stored under it, and deleting a missing key counts as success. An upload is never visible as a partial object. S3 writes each WAL object with one `PutObject`. GCS uses a single-request upload or a resumable session, and the object appears only when the upload finishes. Azure commits a block blob's blocks at once and drops uncommitted blocks on its own.
- Scanner deletion only: a strongly consistent listing and a last-modified time on each object that agrees with broker clocks.
- Neither design needs transactions across objects, or between storage and the coordinator.

## Decision

The design uses owner-based deletion as the only path that deletes objects. A leak costs storage. Deleting live data loses records that can't be recovered. The owner design also keeps deletion safety independent of clocks.

A report-only scanner stays possible as future work.

## Merged and multi-partition objects

Owner-based deletion doesn't need to change for objects that later work writes from existing data. Examples are a merged object that replaces many small WAL objects, or a tiered object that holds the segments of several partitions. This section follows from the preceding decision. It doesn't add a new one.

- The writer of a merged object stores an intent at its owner before the upload, as for a WAL object. The participants are the coordinator partitions whose logs the merged object holds data for.
- Each participant swaps its own rows on its own schedule. One record in its log replaces the batch rows that the merged object covers with extent rows that point at the merged object. Participants don't coordinate with each other.
- Once a participant holds no batch of an old object, it releases that object through the normal release path. The old object's owner deletes it once every participant has released it, as for any WAL object.
- The swap doesn't need a two-phase commit. Before a participant swaps, readers find the old rows. After it swaps, readers find the new ones. Both point at valid data for the same offsets, and a reader gets correct records either way. The deletion grace period covers a reader that still holds cached coordinates of an old object.
- If a merged object's upload fails, or its participants never swap, its own owner resolves it like any other object. The owner fences the silent participants once the intent is older than the maximum commit age, then deletes the object.

This answers a concern from the KIP-1163 discussion: that merging WAL objects needs several coordinators to cooperate, because one WAL object holds batches from several of them. With owner-based deletion, that cooperation is the release and fence protocol that every object already goes through. A tiered object that holds several partitions works the same way, once remote segment metadata lives in the coordinator. It would run the same check as the scanner, never delete anything, and count suspected orphans. That gives visibility into leaks from bugs without the data-loss risk. The design defers it until its metrics, such as open owner entries and their age, show a real leak.
