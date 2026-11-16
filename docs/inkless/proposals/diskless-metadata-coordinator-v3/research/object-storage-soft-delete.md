# Object-storage soft-delete and versioning

Research for ticket [08-object-storage-soft-delete](../issues/08-object-storage-soft-delete.md).

## Summary

All three providers can keep a deleted WAL object recoverable for about 7 days, and none
of them changes what the Inkless storage backends see. A delete still returns success, a
later fetch still gets `KeyNotFoundException`, and a second delete of the same key still
counts as success. The "missing key counts as success" rule in v2's `FileCleaner` holds
unchanged. The backends ignore the fields that report a soft delete, so Inkless can't tell
today whether a delete was recoverable.

The mechanisms differ in what they keep and how a broker can check them:

| Provider | Mechanism that gives about 7 days | Window | Default | Broker-side check | Permission the check needs |
| --- | --- | --- | --- | --- | --- |
| S3 | Versioning plus a `NoncurrentVersionExpiration` lifecycle rule | Whole days, rounded up to the next midnight UTC | Off | `GetBucketVersioning` and `GetBucketLifecycleConfiguration` | `s3:GetBucketVersioning`, `s3:GetLifecycleConfiguration` |
| GCS | Soft delete | 7 to 90 days, in seconds | On, 7 days, for new buckets | `buckets.get`, `softDeletePolicy` | `storage.buckets.get` |
| Azure | Blob soft delete | 1 to 365 days | On only for accounts created in the portal | `Get Blob Service Properties`, `DeleteRetentionPolicy` | Shared Key, an account SAS with service scope, or `Microsoft.Storage/storageAccounts/blobServices/read` |

On every provider, the usual object-write credentials lack the permission the check needs:

- S3: the check needs two bucket-configuration permissions besides the object
  permissions.
- GCS: `roles/storage.objectAdmin` and `roles/storage.objectUser` lack `storage.buckets.get`.
- Azure: with Microsoft Entra ID, `Storage Blob Data Contributor` lacks the action. The
  least privileged built-in role that has it is `Storage Account Contributor`. Account-key
  and connection-string credentials pass. A container SAS doesn't.

Azure versioning state isn't visible on the data plane at all. Only the Azure Resource Manager
API returns it, and the Inkless Azure config has no subscription or resource group.

Deletes are free on all three providers, and S3 Standard, GCS Standard, and Azure hot
storage have no minimum duration. The cost is the retained bytes. `FileCleaner` eventually
deletes each WAL object, once retention or consolidation empties it, so in steady state the
retained bytes equal the upload rate times the window. At
100 MiB/s and 7 days, that's about 57.7 TiB, about $1,200 a month at a GCS regional Standard
price. For a topic with Kafka's default 7-day retention and no consolidation, that roughly
doubles the WAL footprint.

No emulator in the test setup gives a recoverable delete without extra setup:

- MinIO supports versioning and noncurrent expiration, but the test container never
  enables them.
- `fake-gcs-server` models versioning but has no soft-delete policy.
- Azurite lists blob soft delete and undelete as unsupported.

## Sources

The research uses two local checkouts and first-party provider documentation. Citations
into a checkout use paths relative to its root.

- Inkless: `/Users/viktor.somogyi-vass/dev/github/aiven/workspace-inkless/diskless-metadata`
  at commit `2a412af730`.
- Kafka: `/Users/viktor.somogyi-vass/dev/github/apache/kafka` at commit `27d8bbc35e`.
- Provider docs: AWS, Google Cloud, and Microsoft Learn pages, fetched on 2026-11-11. Each
  claim links the page it comes from.
- Emulator sources: the GitHub repositories of MinIO, `fake-gcs-server`, and Azurite, read
  at their default branches on 2026-11-11.

Unless a citation says Kafka, it refers to the Inkless checkout. Statements marked as
inference go beyond what a source says.

## How the Inkless backends delete today

### The interface

- `StorageBackend` combines `ObjectUploader`, `ObjectFetcher`, and `ObjectDeleter`, and
  has no hook besides `configure`
  (`storage/inkless/src/main/java/io/aiven/inkless/storage_backend/common/StorageBackend.java:25-32`).
  `InklessConfig.storage` builds a backend by reflection and calls `configure` at once
  (`storage/inkless/src/main/java/io/aiven/inkless/config/InklessConfig.java:761-764`).
  `SharedState` builds up to four backend instances: fetch, lagging fetch, produce, and
  background (`storage/inkless/src/main/java/io/aiven/inkless/common/SharedState.java:169-178`).
- `ObjectDeleter` defines delete as idempotent: a missing object still succeeds. The batch
  form returns the keys it confirmed, which includes keys that were already absent. Omitted
  keys are safe to retry
  (`storage/inkless/src/main/java/io/aiven/inkless/storage_backend/common/ObjectDeleter.java:26-45`).
- `FileCleaner` and `FileCommitJob` call the deleter:
  - `FileCleaner.cleanFiles` calls the batch delete and dereferences only the confirmed
    keys in the control plane (`storage/inkless/src/main/java/io/aiven/inkless/delete/FileCleaner.java:150-174`).
    It runs on the background backend (`FileCleaner.java:65-74`).
  - `FileCommitJob.tryDeleteFile` calls the single-key delete for an uploaded file whose
    commit failed, and logs a `StorageBackendException`
    (`storage/inkless/src/main/java/io/aiven/inkless/produce/FileCommitJob.java:124-157`).
- `FileUploadJob` creates a new key for each WAL object, `<object.key.prefix>/<random UUID>`
  (`storage/inkless/src/main/java/io/aiven/inkless/produce/FileUploadJob.java:108`,
  `storage/inkless/src/main/java/io/aiven/inkless/common/ObjectKey.java:23-48`). Upload
  retries reuse that key (`FileUploadJob.java:122-129`). Inference: with versioning, a key
  stores one version unless a retry rewrote an upload that had already succeeded.

### S3

- `S3Storage.delete(ObjectKey)` sends one `DeleteObject` and maps a timeout to
  `StorageBackendTimeoutException` and any other `SdkException` to
  `StorageBackendException`
  (`storage/inkless/src/main/java/io/aiven/inkless/storage_backend/s3/S3Storage.java:160-169`).
- The batch delete sends `DeleteObjects` in chunks of 1,000 keys (`S3Storage.java:67`,
  `:171-218`). It doesn't set the `Quiet` element, so S3 sends the verbose response
  (`S3Storage.java:220-230`). It confirms every key in `response.deleted()`
  (`S3Storage.java:196-201`) and counts `response.errors()` by error code
  (`S3Storage.java:202-204`). It logs at `INFO` when every code is `SlowDown` or
  `RequestLimitExceeded`, and at `WARN` otherwise (`S3Storage.java:75`, `:207-215`).
  A request-level `SdkException` stops the pass (`S3Storage.java:189-194`).
- `fetch` maps HTTP 404 to `KeyNotFoundException` and 416 to `InvalidRangeException`
  (`S3Storage.java:139-145`).
- The `PutObject` request doesn't set a storage class (`S3Storage.java:102-105`), so S3
  stores the objects in S3 Standard.

### GCS

- `GcsStorage.delete(ObjectKey)` calls `storage.delete` and ignores its boolean result.
  It maps a timeout to `StorageBackendTimeoutException` and any other
  `BaseServiceException` to `StorageBackendException`
  (`storage/inkless/src/main/java/io/aiven/inkless/storage_backend/gcs/GcsStorage.java:168-178`).
- The batch delete submits a `StorageBatch` of 50 sub-requests at a time
  (`GcsStorage.java:68`, `:181-236`). A sub-request that returns `true` (deleted) or
  `false` (absent) confirms the key. A sub-request that throws counts its HTTP code as a
  failure (`GcsStorage.java:212-221`). A request-level failure stops the pass
  (`GcsStorage.java:201-211`). It logs at `INFO` when every code is 429 or 503
  (`GcsStorage.java:71`, `:225-233`).
- `fetch` throws `KeyNotFoundException` when `storage.get` returns `null` or the service
  answers 404 (`GcsStorage.java:272-274`, `:289-295`).

### Azure

- `AzureBlobStorage.delete(ObjectKey)` calls `deleteIfExists`
  (`storage/inkless/src/main/java/io/aiven/inkless/storage_backend/azure/AzureBlobStorage.java:208-217`).
- The batch delete has no batch API. It calls `deleteIfExists` per key and confirms the key
  on `true` or `false`. It counts a `BlobStorageException` by error code and stops the pass
  on any other `RuntimeException` (`AzureBlobStorage.java:219-255`). It logs at `INFO` when
  every code is `ServerBusy` (`AzureBlobStorage.java:68`, `:244-252`).
- `fetch` maps 404 to `KeyNotFoundException` and 416 to `InvalidRangeException`
  (`AzureBlobStorage.java:195-202`).
- `configure` builds a `BlobServiceClient` and keeps only its container client
  (`AzureBlobStorage.java:85-110`). The config takes an account name, an account key, a SAS
  token, a container, an endpoint, and a connection string. It has no subscription or
  resource group
  (`storage/inkless/src/main/java/io/aiven/inkless/storage_backend/azure/AzureBlobStorageConfig.java:33-53`).

### In-memory

`InMemoryStorage` removes the key from a map and confirms every key in the request
(`storage/inkless/src/main/java/io/aiven/inkless/storage_backend/in_memory/InMemoryStorage.java:105-115`).

## S3 versioning with noncurrent-version expiration

### Retention

- A bucket is in one of three states: unversioned, versioning-enabled, or
  versioning-suspended. After the owner enables versioning, the owner can suspend it but
  can't return the bucket to unversioned. The state applies to all objects in the bucket.
  S3 bills each version as a whole object
  ([Versioning](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Versioning.html)).
- On a versioning-enabled bucket, a `DELETE` without a version ID inserts a delete marker
  as the current version and keeps the object as a noncurrent version. On a suspended
  bucket, it removes the version whose ID is `null` and inserts a delete marker
  ([DeleteObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html)).
  Inference: objects written during a suspension get the `null` version ID
  ([Versioning](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Versioning.html)),
  so deleting them under suspension isn't recoverable.
- The `NoncurrentVersionExpiration` lifecycle action permanently deletes a version a set
  number of days after it becomes noncurrent. S3 adds the days to the creation time of the
  successor, here the delete marker, and rounds up to the next midnight UTC. Optionally,
  `NewerNoncurrentVersions` keeps 1 to 100 versions
  ([Lifecycle rules](https://docs.aws.amazon.com/AmazonS3/latest/userguide/intro-lifecycle-rules.html)).
  Inference: `NoncurrentDays=7` keeps a deleted object between 7 and 8 days.
- Removal after the expiration date is asynchronous and can lag, but S3 doesn't charge for
  storage after an object expires. A bucket policy can't block a lifecycle rule
  ([Expiring objects](https://docs.aws.amazon.com/AmazonS3/latest/userguide/lifecycle-expire-general-considerations.html)).
- A lifecycle rule can filter by key prefix, tags, and object size
  ([GetBucketLifecycleConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLifecycleConfiguration.html);
  [Lifecycle examples](https://docs.aws.amazon.com/AmazonS3/latest/userguide/lifecycle-configuration-examples.html)),
  so a rule can cover only `object.key.prefix`
  (`storage/inkless/src/main/java/io/aiven/inkless/config/InklessConfig.java:48`).
  Versioning itself is bucket-wide.
- Once its noncurrent version expires, a delete marker becomes an expired object delete
  marker. A rule with `ExpiredObjectDeleteMarker` removes those markers
  ([Lifecycle examples](https://docs.aws.amazon.com/AmazonS3/latest/userguide/lifecycle-configuration-examples.html)).
- Directory buckets, the S3 Express One Zone bucket type, don't support versioning
  ([DeleteObjects](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html)).

### Restore procedure

- List the key's versions with `ListObjectVersions`, which needs `s3:ListBucketVersions`
  ([S3 policy actions](https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-with-s3-policy-actions.html)).
- Copy the noncurrent version onto the same key with `CopyObject` and `?versionId=`. AWS
  calls this the safer method, because lifecycle is eventually consistent and can expire
  the version while a delete-marker removal propagates. Deleting the delete marker by its
  version ID also brings the object back
  ([Restoring previous versions](https://docs.aws.amazon.com/AmazonS3/latest/userguide/RestoringPreviousVersions.html);
  [Lifecycle rules](https://docs.aws.amazon.com/AmazonS3/latest/userguide/intro-lifecycle-rules.html)).
  Deleting by version ID needs `s3:DeleteObjectVersion`
  ([DeleteObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html)).
- `CopyObject` from a version needs `s3:GetObjectVersion`
  ([S3 policy actions](https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-with-s3-policy-actions.html)).
  For many objects, AWS points to batch `CopyObject` jobs
  ([Restoring previous versions](https://docs.aws.amazon.com/AmazonS3/latest/userguide/RestoringPreviousVersions.html)).

### Backend behavior

- `DeleteObjects` reports a key it doesn't find as deleted
  ([DeleteObjects](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html)).
  This fact is the basis of v2's rule
  (`.scratch/diskless-metadata-coordinator-v2/design.md:459`).
- On a versioned bucket, `DeleteObjects` without version IDs returns each key in
  `Deleted`, with `DeleteMarker` set to `true` and the marker's version ID
  ([DeleteObjects](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html)).
  `S3Storage` confirms each `Deleted` entry and ignores both fields
  (`S3Storage.java:196-201`), so it handles the response as it does today.
- S3 creates a delete marker for every `DeleteObject` on a versioning-enabled or suspended
  bucket ([Working with delete markers](https://docs.aws.amazon.com/AmazonS3/latest/userguide/DeleteMarker.html)).
  Inference: in v2's stale-list case, where `FileCleaner` deletes a key twice, AWS leaves
  two delete markers. The backend still confirms both deletes.
- A `GET` without a version ID on a key whose current version is a delete marker returns
  404 with `x-amz-delete-marker: true`
  ([Working with delete markers](https://docs.aws.amazon.com/AmazonS3/latest/userguide/DeleteMarker.html)).
  `S3Storage.fetch` maps that answer to `KeyNotFoundException` (`S3Storage.java:139-141`),
  as it does for an unversioned bucket.

## GCS soft delete and Object Versioning

### Retention

- Soft delete is on by default for every bucket that supports it, with 7 days of
  retention. The retention can be 7 to 90 days, or 0 to turn it off. A change applies only
  to objects deleted after it, and takes up to 30 seconds to propagate. An organization tag
  can change the default for new buckets, including turning it off
  ([Soft delete overview](https://cloud.google.com/storage/docs/soft-delete)).
- A client can't read, modify, or overwrite a soft-deleted object. It can only list or
  restore it. Object Lifecycle Management doesn't affect soft-deleted objects, and objects
  that lifecycle deletes become soft-deleted. The policy is bucket-wide
  ([Soft delete overview](https://cloud.google.com/storage/docs/soft-delete)).
- `objects.delete` on a bucket with a soft delete policy soft-deletes the object and sets
  `softDeleteTime` and `hardDeleteTime`. It can't hard-delete a soft-deleted object, and
  a delete of a soft-deleted object answers 404 `notFound`
  ([Objects: delete](https://cloud.google.com/storage/docs/json_api/v1/objects/delete)).
- `softDeletePolicy.hardDeletePause.enabled` stops hard deletes for the whole bucket
  ([Bucket resource](https://cloud.google.com/storage/docs/json_api/v1/buckets)), and
  soft-deleted objects then bill until the pause ends
  ([Soft delete overview](https://cloud.google.com/storage/docs/soft-delete)).
- Object Versioning keeps a noncurrent version when a client deletes a live object without
  a generation number. The version stays until something deletes it explicitly or through
  lifecycle. When soft delete is also on, deleting a noncurrent version soft-deletes it.
  Google recommends soft delete over versioning for protection against accidental or
  malicious deletes ([Object Versioning](https://cloud.google.com/storage/docs/object-versioning)).

### Restore procedure

- `objects.restore` takes the object name and a required `generation`, and creates a new
  live copy with a new generation in Standard storage. It needs `storage.objects.restore`
  and `storage.objects.create`. It answers 400 `SoftDeletePolicyRequired` without a policy,
  404 when the retention has passed, and 412 for a live or noncurrent object
  ([Objects: restore](https://cloud.google.com/storage/docs/json_api/v1/objects/restore)).
- A bulk restore runs as a long-running operation over objects that match a filter
  ([Soft delete overview](https://cloud.google.com/storage/docs/soft-delete)).
- `roles/storage.objectUser` includes `storage.objects.restore`
  ([IAM roles](https://cloud.google.com/storage/docs/access-control/iam-roles)).

### Backend behavior

- A soft delete returns an empty success response
  ([Objects: delete](https://cloud.google.com/storage/docs/json_api/v1/objects/delete)).
  It has no field that tells a soft delete from a hard one.
- A second delete of the key answers 404. `GcsStorage` treats the batch result for an
  absent blob as confirmed (`GcsStorage.java:212-217`). Inference: the client library
  turns that 404 into `false`, as the code's comment at `GcsStorage.java:193-195`
  states.
- A fetch of a soft-deleted object doesn't find a live object. `GcsStorage.fetch` throws
  `KeyNotFoundException` for a `null` blob or a 404 (`GcsStorage.java:272-274`,
  `:289-295`). With versioning, a read without a generation number also fails after a
  delete ([Object Versioning](https://cloud.google.com/storage/docs/object-versioning)).

## Azure blob soft delete and versioning

### Retention

- Blob soft delete is a storage-account setting with 1 to 365 days of retention. A delete
  marks the blob as soft-deleted, and the retention starts at the delete. Disabling soft
  delete leaves already soft-deleted blobs recoverable until their retention ends
  ([Soft delete for blobs](https://learn.microsoft.com/en-us/azure/storage/blobs/soft-delete-blob-overview)).
- Soft delete is on by default only for accounts created in the Azure portal. PowerShell
  and the Azure CLI create accounts without it
  ([Enable soft delete for blobs](https://learn.microsoft.com/en-us/azure/storage/blobs/soft-delete-blob-enable)).
- A client can't read a soft-deleted blob until someone restores it. Any operation other
  than `Undelete Blob` on it answers 404
  ([Soft delete for blobs](https://learn.microsoft.com/en-us/azure/storage/blobs/soft-delete-blob-overview);
  [Delete Blob](https://learn.microsoft.com/en-us/rest/api/storageservices/delete-blob)).
- With versioning on, a delete without a version ID turns the current version into a
  previous version. If soft delete is also on, its retention doesn't apply to that
  deleted blob. A previous version persists until something deletes it, and that delete
  is the one soft delete protects
  ([Blob versioning](https://learn.microsoft.com/en-us/azure/storage/blobs/versioning-overview)).
- A lifecycle rule ages a version from the blob's creation, not from when it became a
  previous version
  ([Blob versioning](https://learn.microsoft.com/en-us/azure/storage/blobs/versioning-overview)).
  Inference: on Azure, versioning with a 7-day version rule can delete a deleted WAL
  object at once, because the version's age counts from the upload.
- The permanent-delete feature, `allowPermanentDelete`, lets a caller remove soft-deleted
  versions and snapshots early. It doesn't apply to base blobs
  ([Delete Blob](https://learn.microsoft.com/en-us/rest/api/storageservices/delete-blob);
  [Blob Services, Get Service Properties](https://learn.microsoft.com/en-us/rest/api/storagerp/blob-services/get-service-properties)).
- Blob soft delete doesn't protect against deleting the container or the account. That
  needs container soft delete or a resource lock
  ([Soft delete for blobs](https://learn.microsoft.com/en-us/azure/storage/blobs/soft-delete-blob-overview)).

### Restore procedure

- `Undelete Blob` restores a soft-deleted blob with its snapshots and versions. It needs
  the `Microsoft.Storage/storageAccounts/blobServices/containers/write` action, which
  `Storage Blob Data Contributor` has, or a SAS with write permission. On a blob that isn't
  soft-deleted, it succeeds and doesn't change anything
  ([Undelete Blob](https://learn.microsoft.com/en-us/rest/api/storageservices/undelete-blob)).
- List soft-deleted blobs with `include=deleted`
  ([Soft delete for blobs](https://learn.microsoft.com/en-us/azure/storage/blobs/soft-delete-blob-overview)).
- With versioning, copy a previous version over the base blob
  ([Blob versioning](https://learn.microsoft.com/en-us/azure/storage/blobs/versioning-overview)).

### Backend behavior

- `Delete Blob` returns 202 under soft delete, with `x-ms-delete-type-permanent: false`
  ([Delete Blob](https://learn.microsoft.com/en-us/rest/api/storageservices/delete-blob)).
  `deleteIfExists` returns `true`, and `AzureBlobStorage` doesn't read the header
  (`AzureBlobStorage.java:231-232`).
- A second delete answers 404, which `deleteIfExists` maps to `false`. The backend confirms
  the key either way (`AzureBlobStorage.java:220-232`).
- A fetch answers 404, which the backend maps to `KeyNotFoundException`
  (`AzureBlobStorage.java:196-197`).
- Inference: with versioning, Azure answers a read or a delete without a version ID as
  missing for a key that has no current version. The versioning page describes the state
  ("no longer a current version") but not the status code.

## Storage cost

### Charges per provider

- Storage of retained data:
  - S3 bills every version, including noncurrent versions, at normal rates
    ([Deleting object versions](https://docs.aws.amazon.com/AmazonS3/latest/userguide/DeletingObjectVersions.html)).
    A delete marker costs the size of its key name in S3 Standard
    ([Working with delete markers](https://docs.aws.amazon.com/AmazonS3/latest/userguide/DeleteMarker.html)).
  - GCS bills live, noncurrent, and soft-deleted objects the same way, prorated to the
    sub-second ([Cloud Storage pricing](https://cloud.google.com/storage/pricing)).
  - Azure bills soft-deleted data and versions at the rate of active data, in the same tier
    ([Soft delete for blobs](https://learn.microsoft.com/en-us/azure/storage/blobs/soft-delete-blob-overview);
    [Blob versioning](https://learn.microsoft.com/en-us/azure/storage/blobs/versioning-overview)).
- Requests:
  - S3 doesn't charge for `DELETE` requests
    ([S3 pricing](https://aws.amazon.com/s3/pricing/)), or for an object's expiration
    ([Expiring objects](https://docs.aws.amazon.com/AmazonS3/latest/userguide/lifecycle-expire-general-considerations.html)).
  - GCS lists `storage.objects.delete` and lifecycle `Delete` as free operations
    ([Cloud Storage pricing](https://cloud.google.com/storage/pricing)).
  - Azure doesn't charge for `Delete Blob`, or for the versions or snapshots that a delete
    creates
    ([Delete Blob](https://learn.microsoft.com/en-us/rest/api/storageservices/delete-blob);
    [Soft delete for blobs](https://learn.microsoft.com/en-us/azure/storage/blobs/soft-delete-blob-overview)).
- Restores, which run only on recovery:
  - A GCS restore is one Class A operation per object, plus one per 1,000 objects that a
    bulk restore processes ([Cloud Storage pricing](https://cloud.google.com/storage/pricing)).
  - Azure bills `Undelete Blob` as a write operation
    ([Undelete Blob](https://learn.microsoft.com/en-us/rest/api/storageservices/undelete-blob)).
- Minimum durations:
  - S3 Standard has none. Standard-IA and One Zone-IA have 30 days and a 128 KB minimum
    object size ([S3 pricing](https://aws.amazon.com/s3/pricing/)).
  - GCS Standard has none. Nearline has 30 days, Coldline 90, and Archive 365. Time spent
    noncurrent or soft-deleted counts toward the minimum, and the charge waits for the hard
    delete ([Cloud Storage pricing](https://cloud.google.com/storage/pricing)).
  - Azure hot has none. Cool has 30 days, cold 90, and archive 180. A soft-deleted blob
    counts as deleted only when its retention ends
    ([Access tiers](https://learn.microsoft.com/en-us/azure/storage/blobs/access-tiers-overview)).
  - Inkless writes S3 objects without a storage class (`S3Storage.java:102-105`). Inference:
    WAL objects in Standard or hot don't pay a minimum-duration charge.

### Retained bytes for a 7-day window

- Inference: `FileCleaner` eventually deletes each WAL object, so in steady state the
  bytes deleted per second equal the bytes uploaded per second. The extra storage is the upload
  rate times the window. On S3, the window is 7 to 8 days because of the midnight
  rounding.
- Example, as inference: 100 MiB/s of WAL uploads times 604,800 seconds is about
  59,060 GiB, or 57.7 TiB. The first regional Standard price on the GCS pricing page is
  $0.000027397 per GiB-hour ([Cloud Storage pricing](https://cloud.google.com/storage/pricing)),
  about $0.020 per GiB for a 730-hour month. That makes about $1,200 a month.
- Relative to the live WAL footprint, the extra storage is the window over a WAL object's
  lifetime. Kafka's default retention is 7 days (Kafka
  `storage/src/main/java/org/apache/kafka/storage/internals/log/LogConfig.java:133`).
  Inference: a WAL object stays in storage at least as long as its newest batch's retention, unless
  consolidation removes it earlier. So a 7-day window about doubles the WAL bytes of a
  default-retention topic, and costs more relative to that footprint when consolidation
  shortens WAL lifetimes.
- GCS soft delete and Azure blob soft delete cover the whole bucket or account. An S3
  lifecycle rule can cover only a prefix. Inference: on GCS and Azure, anything else that
  shares the bucket or account and churns also pays the window.

## Checking the setting from a broker

### S3

- `GetBucketVersioning` returns `Enabled`, `Suspended`, or no status for a bucket that was
  never versioned. Its page says the caller must be the bucket owner
  ([GetBucketVersioning](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketVersioning.html)),
  and the IAM table names `s3:GetBucketVersioning`
  ([S3 policy actions](https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-with-s3-policy-actions.html)).
- `GetBucketLifecycleConfiguration` returns the rules and needs
  `s3:GetLifecycleConfiguration`. A bucket without a lifecycle configuration answers 404
  `NoSuchLifecycleConfiguration`
  ([GetBucketLifecycleConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLifecycleConfiguration.html)).
- Inference: a check needs both calls. Versioning alone keeps objects forever, and a
  rule with a small `NoncurrentDays` shortens the window. The check has to match each
  enabled rule's filter against `object.key.prefix`. A rule filtered by tag or size can
  also match, so a prefix-only comparison is incomplete.
- The SDK the backend uses, `S3Client` (`S3Storage.java:53`, `:92`), exposes both
  operations. The Inkless checkout doesn't state an IAM policy for the backend, and a search
  of `docs/inkless` doesn't find a permission list.

### GCS

- `buckets.get` returns the bucket resource with `softDeletePolicy.retentionDurationSeconds`,
  `softDeletePolicy.effectiveTime`, `softDeletePolicy.hardDeletePause`, and
  `versioning.enabled`. It needs `storage.buckets.get`
  ([Buckets: get](https://cloud.google.com/storage/docs/json_api/v1/buckets/get);
  [Bucket resource](https://cloud.google.com/storage/docs/json_api/v1/buckets)).
- `roles/storage.objectAdmin` and `roles/storage.objectUser` don't include
  `storage.buckets.get`. `roles/storage.legacyBucketReader`,
  `roles/storage.legacyBucketWriter`, and `roles/storage.bucketViewer` do
  ([IAM roles](https://cloud.google.com/storage/docs/access-control/iam-roles)).
- The client library Inkless pins, `google-cloud-storage` 2.45.0
  (`gradle/dependencies.gradle:68`), has `BucketInfo.SoftDeletePolicy`, as its jar in the
  local Gradle cache shows.
- A policy change applies only to later deletes
  ([Soft delete overview](https://cloud.google.com/storage/docs/soft-delete)). Inference:
  a check at startup can't see a change made while the broker runs.

### Azure

- `Get Blob Service Properties`, a data-plane call to
  `https://<account>.blob.core.windows.net/?restype=service&comp=properties`, returns
  `DeleteRetentionPolicy` with `Enabled` and `Days`. Its response has no versioning
  element. It accepts the following authorization
  ([Get Blob Service Properties](https://learn.microsoft.com/en-us/rest/api/storageservices/get-blob-service-properties)):
  - Shared Key.
  - An account SAS with signed service `b`, resource type `s`, and permission `r`.
  - Microsoft Entra ID with the action
    `Microsoft.Storage/storageAccounts/blobServices/read`. The least privileged built-in
    role with that action is `Storage Account Contributor`.
- `Storage Blob Data Contributor` and `Storage Blob Data Owner` don't include
  `blobServices/read`
  ([Storage built-in roles](https://learn.microsoft.com/en-us/azure/role-based-access-control/built-in-roles/storage)).
- `isVersioningEnabled` and `deleteRetentionPolicy` both appear in the Azure Resource
  Manager call to
  `management.azure.com/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/.../blobServices/default`
  ([Blob Services, Get Service Properties](https://learn.microsoft.com/en-us/rest/api/storagerp/blob-services/get-service-properties)).
  The Inkless Azure config has neither identifier (`AzureBlobStorageConfig.java:33-53`).
- Inference from `AzureBlobStorage.java:85-110`: with an account key or a connection
  string, the backend's own client can make the data-plane call. With a SAS token, the
  call works only if the token is an account SAS with service scope. With
  `DefaultAzureCredential`, it needs a role beyond the data roles.

### Soft-delete fields in delete responses

- S3's verbose `DeleteObjects` response has `DeleteMarker` per key
  ([DeleteObjects](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html)).
- Azure's `Delete Blob` response has `x-ms-delete-type-permanent`
  ([Delete Blob](https://learn.microsoft.com/en-us/rest/api/storageservices/delete-blob)).
- GCS returns an empty body
  ([Objects: delete](https://cloud.google.com/storage/docs/json_api/v1/objects/delete)).
- Inference: S3 and Azure can confirm per delete, with no extra permission, that the
  delete was recoverable. GCS can't.

## S3-compatible stores and emulators in the checkout

### MinIO

- The tests run the `pgsty/minio` image, tag `RELEASE.2026-08-04T00-00-00Z`, with
  `server /data`, on one drive. They create a bucket with no versioning
  (`storage/inkless/src/test/java/io/aiven/inkless/test_utils/MinioContainer.java:37-38`,
  `:58`, `:97-101`). The local Docker Compose example creates its bucket with `mc mb` and
  enables nothing else
  (`docker/examples/docker-compose-files/inkless/docker-compose.s3-local.yml:26`, `:43`).
- The image's GitHub repository describes itself as "S3-Compatible Object Storage. A MinIO
  fork maintained by PGSTY" ([pgsty/minio](https://github.com/pgsty/minio)). This research
  didn't read the fork's source.
- Upstream MinIO supports bucket versioning, `NoncurrentVersionExpiration` with
  `NoncurrentDays`, and `ExpiredObjectDeleteMarker`. Its versioning guide says versioning
  needs an erasure-coded setup
  ([versioning guide](https://github.com/minio/minio/blob/master/docs/bucket/versioning/README.md);
  [lifecycle guide](https://github.com/minio/minio/blob/master/docs/bucket/lifecycle/README.md)).
- A single-drive deployment formats its drive with the erasure backend `xl-single`
  (`newFormatErasureV3` in
  [MinIO format code](https://github.com/minio/minio/blob/master/cmd/format-erasure.go)).
  `PutBucketVersioningHandler` has no backend check
  ([MinIO versioning handler](https://github.com/minio/minio/blob/master/cmd/bucket-versioning-handler.go)).
  Inference: the single-drive test container accepts `PutBucketVersioning` if a test
  calls it.
- MinIO doesn't create a second delete marker for a repeated delete, unlike AWS
  ([versioning guide](https://github.com/minio/minio/blob/master/docs/bucket/versioning/README.md)).
- The build declares a LocalStack test dependency (`build.gradle:2651`), but no test class
  in the checkout uses `LocalStackContainer`.

### `fake-gcs-server`

The GCS tests run `FakeGcsServerContainer`
(`storage/inkless/src/test/java/io/aiven/inkless/storage_backend/gcs/integration/GcsStorageTest.java:58`).
The server's bucket model has `VersioningEnabled` and no soft-delete policy
([bucket model](https://github.com/fsouza/fake-gcs-server/blob/main/internal/backend/bucket.go)),
and its README doesn't mention soft delete
([README](https://github.com/fsouza/fake-gcs-server/blob/main/README.md)).

### Azurite

The Azure tests run `mcr.microsoft.com/azure-storage/azurite`
(`storage/inkless/src/test/java/io/aiven/inkless/storage_backend/azure/integration/AzuriteBlobStorageUtils.java:25-29`).
Azurite supports `Get Service Properties` and opt-in blob versioning, and doesn't support
blob soft delete or `Undelete Blob`. It doesn't integrate soft delete with versioning
([Azurite README](https://github.com/Azure/Azurite/blob/main/README.md)).

## The classic-topic baseline for a wrong delete

Under clause 1 of the data-loss bar, the baseline is what a classic topic survives. A
classic replica deletes a segment file `file.delete.delay.ms` after scheduling it, 60
seconds by default (Kafka
`clients/src/main/java/org/apache/kafka/common/config/TopicConfig.java:159-161`;
`server-common/src/main/java/org/apache/kafka/server/config/ServerLogConfigs.java:105`).
Inkless's `FileCleaner` grace period is also 1 minute by default
(`InklessConfig.java:152-154`). Inference: a classic topic has no multi-day undo for a
wrong delete. A 7-day window goes beyond classic parity. It's the bound that clause 3
needs for one partition's loss deleting data that another partition references.

## Implications for the design

The deletion-bounds ticket has to settle these points:

- Which mechanism counts per provider. The facts that bear on it:
  - S3: versioning `Enabled` keeps a deleted object, and a noncurrent expiration rule sets
    the window. `Suspended` doesn't keep objects written during the suspension.
  - GCS: soft delete keeps a deleted object for the policy's retention. It's on by default
    at the 7-day minimum.
  - Azure: blob soft delete keeps a deleted blob for its retention, counted from the
    delete. Versioning keeps it as a previous version, which a lifecycle rule ages from
    the blob's creation.
- Whether the broker checks, and how a failed check behaves. No provider's usual object
  permissions allow the check. Azure with Entra ID needs a control-plane role, and Azure
  versioning isn't visible to the data plane.
- Whether the backends read the per-delete signal that S3 and Azure already return, as a
  check with no extra permission.
- How the design states the cost. The retained bytes are the upload rate times the window,
  about double the WAL bytes of a 7-day topic. The default storage classes don't add a
  request or minimum-duration charge.
- How tests cover the bound. MinIO can enable versioning in a test. Neither
  `fake-gcs-server` nor Azurite models soft delete, and `InMemoryStorage` deletes at once.
- How an operator restores. A restore brings back bytes under the same key with a new
  version or generation. Nothing in Inkless re-registers the object, so recovery depends
  on the metadata that references the key, which the object-footer ticket covers.

The sources don't settle these points:

- What the `pgsty/minio` fork changes from upstream MinIO's versioning.
- The exact status codes a versioned Azure blob returns on read and delete after it loses
  its current version.
- Whether `GetBucketVersioning` works for an IAM principal in the bucket owner's account
  with only `s3:GetBucketVersioning`. The API page says "bucket owner," and the IAM table
  names the action.
- The upload rate and WAL lifetime of real deployments, which set the cost.
