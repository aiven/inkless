# Object-storage soft-delete and versioning

Type: research
Status: closed
Assignee: Viktor Somogyi-Vass
Blocked by: none

## Question

What do S3, GCS, and Azure offer to make a delete recoverable for about 7 days, what does it cost, and can a broker check that it's on?

The council's [second fix](../reviews/council.md#what-would-close-the-gaps) requires soft-delete or bucket versioning with about 7 days of noncurrent retention. Under clause 3 of the map's data-loss bar, that is the bound for a lost tail or an apply bug that deletes an object another partition still references. Find, from first-party provider docs and the Inkless checkout:

- S3 versioning with a noncurrent-version expiration rule, GCS soft delete and object versioning, and Azure blob soft delete and versioning: what each keeps, for how long, and how an operator restores an object.
- How a delete behaves under each: does `DeleteObject` on a versioned bucket return success and leave a delete marker, and does that change what the Inkless storage backends see, including the "missing key counts as success" rule in v2's [`FileCleaner`](../../diskless-metadata-coordinator-v2/design.md#filecleaner)?
- The storage cost: noncurrent bytes retained for the window, given that every WAL object is eventually deleted, plus any per-request or minimum-duration charges.
- Whether a broker can check the setting at startup with the credentials Inkless already has, which API call does it on each provider, and what permission it needs.
- How the Inkless storage backends delete today: the classes, batch delete calls, and their error mapping.
- Whether S3-compatible stores that Inkless supports, such as MinIO in tests, implement versioning.

## Resolution

Resolved by research on 2026-11-11. The findings, with citations, are in [object-storage-soft-delete.md](../research/object-storage-soft-delete.md).

All three providers can keep a deleted WAL object recoverable for about 7 days, and none of them changes what the Inkless backends see. A delete still returns success, a later fetch still gets `KeyNotFoundException`, and a second delete still counts as success, so v2's "missing key counts as success" rule stays valid. The backends delete through `S3Storage` with `DeleteObjects` in chunks of 1,000, through `GcsStorage` with a `StorageBatch` of 50, and through `AzureBlobStorage` one `deleteIfExists` per key. Each confirms deleted and absent keys alike, counts per-key failures by code, and stops the pass on a request-level failure.

- S3: versioning `Enabled` turns a delete into a delete marker, and a `NoncurrentVersionExpiration` rule sets the window in whole days, rounded up to the next midnight UTC. `NoncurrentDays=7` gives 7 to 8 days. With versioning `Suspended`, a delete permanently removes objects written during the suspension. An operator restores by copying the noncurrent version onto the key. Versioning is bucket-wide, but a lifecycle rule can cover only `object.key.prefix`.
- GCS: soft delete is on by default for new buckets at 7 days, its minimum, and goes up to 90. Clients can't read a soft-deleted object, and `objects.restore` with its generation brings it back. The policy is bucket-wide, and a change applies only to later deletes.
- Azure: blob soft delete keeps a deleted blob for 1 to 365 days from the delete, and `Undelete Blob` restores it. It's on by default only for accounts created in the portal. With versioning on, a delete makes a previous version instead, and a lifecycle rule ages that version from the upload, not from the delete.
- Cost: deletes and expirations are free on all three providers. S3 Standard, GCS Standard, and Azure hot have no minimum duration. The retained bytes equal the upload rate times the window: about 57.7 TiB at 100 MiB/s for 7 days, about $1,200 a month at a GCS regional Standard price. For a 7-day-retention topic without consolidation, that roughly doubles the WAL bytes.
- Broker check: each provider has a call, but the usual object-write credentials lack the permission it needs. S3 needs `GetBucketVersioning` and `GetBucketLifecycleConfiguration`, with `s3:GetBucketVersioning` and `s3:GetLifecycleConfiguration`, and the check has to match lifecycle filters against the prefix. GCS needs `buckets.get` with `storage.buckets.get`, which `roles/storage.objectAdmin` and `roles/storage.objectUser` lack. Azure's data-plane `Get Blob Service Properties` returns the soft-delete policy but not versioning. It works with an account key, a connection string, or an account SAS with service scope. With Entra ID, it needs `blobServices/read`, which `Storage Blob Data Contributor` lacks.
- Per-delete signal: S3's verbose `DeleteObjects` response reports `DeleteMarker`, and Azure's `Delete Blob` returns `x-ms-delete-type-permanent`. Both arrive with no extra permission, and the backends ignore both today. GCS returns an empty body.
- Emulators: upstream MinIO supports versioning and noncurrent expiration, and its single-drive backend is the erasure format that versioning needs. The test container, a `pgsty/minio` fork, never enables versioning. `fake-gcs-server` has no soft-delete policy, Azurite doesn't support blob soft delete, and `InMemoryStorage` deletes at once.
- Baseline: a classic replica deletes a segment file 60 seconds after scheduling it, so a 7-day window goes beyond classic parity.

Which mechanism counts per provider, whether and how the broker checks, and whether the backends read the per-delete signal belong to [Deletion bounds](10-deletion-bounds.md).
