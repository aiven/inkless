# Soft-delete probe with object-only credentials

Findings for [Deletion bounds: soft-delete requirement and a deletion-rate cap](../issues/10-deletion-bounds.md), gathered on 2026-11-13 from first-party provider docs. Lines marked "Inference" weren't read in a source.

## Summary

A broker can confirm a soft delete with the object roles it already has, on all three providers. S3 and Azure report it in the delete response. GCS needs a follow-up read of the deleted probe, which also returns the retention window.

## GCS

- `objects.get` with `softDeleted=true` and a required `generation` returns a soft-deleted object's metadata. It needs only `storage.objects.get`. `storage.objects.getIamPolicy` applies only to ACL projections ([objects.get](https://cloud.google.com/storage/docs/json_api/v1/objects/get), [IAM permissions for JSON methods](https://docs.cloud.google.com/storage/docs/access-control/iam-json)).
- `roles/storage.objectUser` and `roles/storage.objectAdmin` both include `storage.objects.get`, `storage.objects.list`, and `storage.objects.restore` ([IAM roles](https://docs.cloud.google.com/storage/docs/access-control/iam-roles)).
- The object resource has `softDeleteTime` and `hardDeleteTime`. `hardDeleteTime` is the soft-delete time plus the retention, fixed at deletion, so the difference is the window that applied ([object resource](https://cloud.google.com/storage/docs/json_api/v1/objects)).
- `objects.list` with `softDeleted=true` fails with `400 invalidArgument` on a bucket with no soft-delete policy ([objects.list](https://cloud.google.com/storage/docs/json_api/v1/objects/list)). Inference: the docs don't say whether a retention of 0 counts as no policy.
- The pinned Java client, `google-cloud-storage` 2.45.0, has `Storage.BlobGetOption.softDeleted(boolean)`, `Storage.BlobListOption.softDeleted(boolean)`, and `BlobInfo.getSoftDeleteTime()` and `getHardDeleteTime()` ([BlobGetOption](https://cloud.google.com/java/docs/reference/google-cloud-storage/2.45.0/com.google.cloud.storage.Storage.BlobGetOption), [BlobInfo](https://cloud.google.com/java/docs/reference/google-cloud-storage/2.45.0/com.google.cloud.storage.BlobInfo)). Inference: the probe read is `storage.get(BlobId.of(bucket, name, generation), BlobGetOption.softDeleted(true))`, and `null` means the delete wasn't soft.
- On a bucket with Object Versioning, deleting the live object only makes it noncurrent, and deleting the noncurrent version soft-deletes it ([soft delete overview](https://cloud.google.com/storage/docs/soft-delete)). Inference: the probe deletes by generation, or a versioned bucket looks like it has no soft delete. The object resource page still calls a delete by generation permanent, which conflicts with the soft-delete overview.
- Cost: soft-deleted bytes bill like live ones until the window ends. Standard has no minimum duration. Nearline, Coldline, and Archive bill 30, 90, and 365 days ([pricing](https://cloud.google.com/storage/pricing)). A get is a Class B operation.
- Soft delete doesn't cover XML API multipart parts or zonal Rapid buckets ([soft delete overview](https://cloud.google.com/storage/docs/soft-delete)).

## S3

- `DeleteObject` on a versioned bucket returns `x-amz-delete-marker` and the marker's `x-amz-version-id`, with `s3:DeleteObject` only ([DeleteObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html)). The verbose `DeleteObjects` response has `DeleteMarker` per key, as [object-storage-soft-delete.md](object-storage-soft-delete.md#soft-delete-fields-in-delete-responses) found.
- `HeadObject` on a key whose current version is a delete marker returns `x-amz-delete-marker: true` and needs `s3:GetObject` ([HeadObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html)). Inference: the page doesn't say whether a principal without `s3:ListBucket` still sees that header.
- `ListObjectVersions` needs `s3:ListBucketVersions` ([ListObjectVersions](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectVersions.html)).
- No call available to object credentials reports the noncurrent window.

## Azure

- `Delete Blob` returns `x-ms-delete-type-permanent`, `true` or `false`, from API version 2017-07-29 ([Delete Blob](https://learn.microsoft.com/en-us/rest/api/storageservices/delete-blob)). Inference: it arrives for every authorization type, because the page attaches no condition.
- `List Blobs` with `include=deleted` returns `Deleted`, `DeletedTime`, and `RemainingRetentionDays`, with `blobs/read`, which `Storage Blob Data Contributor` has ([List Blobs](https://learn.microsoft.com/en-us/rest/api/storageservices/list-blobs), [built-in roles](https://learn.microsoft.com/en-us/azure/role-based-access-control/built-in-roles/storage#storage-blob-data-contributor)). Inference: a probe can read the window from it in whole days.
