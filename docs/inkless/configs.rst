=================
Diskless Configs
=================
.. Generated from *Config.java classes by io.aiven.inkless.doc.ConfigsDocs

-----------------
InklessConfig
-----------------
Under ``inkless.``

``control.plane.class``
  The control plane implementation class

  * Type: class
  * Default: io.aiven.inkless.control_plane.InMemoryControlPlane
  * Valid Values: Any implementation of io.aiven.inkless.control_plane.ControlPlane
  * Importance: high

``produce.buffer.max.bytes``
  The max size of the buffer to accumulate produce requests. This is a best effort limit that cannot always be strictly enforced.

  * Type: int
  * Default: 8388608 (8 mebibytes)
  * Valid Values: [1,...]
  * Importance: high

``produce.commit.interval.ms``
  The interval with which produced data are committed.

  * Type: int
  * Default: 250
  * Valid Values: [1,...]
  * Importance: high

``storage.backend.class``
  The storage backend implementation class

  * Type: class
  * Default: io.aiven.inkless.storage_backend.in_memory.InMemoryStorage
  * Importance: high

``object.key.prefix``
  The object storage key prefix. It cannot start of finish with a slash.

  * Type: string
  * Default: ""
  * Valid Values: non-null string
  * Importance: medium

``produce.max.upload.attempts``
  The max number of attempts to upload a file to the object storage.

  * Type: int
  * Default: 3
  * Valid Values: [1,...]
  * Importance: medium

``produce.upload.backoff.ms``
  The number of millisecond to back off for before the next upload attempt.

  * Type: int
  * Default: 10
  * Valid Values: [0,...]
  * Importance: medium

``consume.batch.coordinate.cache.enabled``
  If true, the Batch Coordinate cache is enabled.

  * Type: boolean
  * Default: true
  * Importance: low

``consume.batch.coordinate.cache.ttl.ms``
  Time to live in milliseconds for an entry in the Batch Coordinate cache. The time to live must be <= than half of the value of of file.cleaner.interval.ms.

  * Type: int
  * Default: 5000 (5 seconds)
  * Valid Values: [1,...]
  * Importance: low

``consume.cache.block.bytes``
  The number of bytes to fetch as a single block from object storage when serving fetch requests.

  * Type: int
  * Default: 16777216 (16 mebibytes)
  * Importance: low

``consume.cache.expiration.lifespan.sec``
  The lifespan in seconds of a cache entry before it will be removed from all storages.

  * Type: int
  * Default: 60
  * Valid Values: [10,...]
  * Importance: low

``consume.cache.expiration.max.idle.sec``
  The maximum idle time in seconds before a cache entry will be removed from all storages. -1 means disabled, and entries will not be removed based on idle time.

  * Type: int
  * Default: -1
  * Valid Values: [-1,...]
  * Importance: low

``consume.cache.max.count``
  The maximum number of objects to cache in memory. If the cache exceeds this limit, and the cache persistence is enabled, the least recently used objects will be persisted to disk and removed from memory.

  * Type: long
  * Default: 1000
  * Valid Values: [1,...]
  * Importance: low

``fetch.data.thread.pool.size``
  Thread pool size to concurrently fetch data files from remote storage

  * Type: int
  * Default: 32
  * Valid Values: [1,...]
  * Importance: low

``fetch.find.batches.max.per.partition``
  The maximum number of batches to find per partition when processing a fetch request. A value of 0 means all available batches are fetched. This is primarily intended for environments where the batches fan-out on fetch requests can overload the control plane back-end.

  * Type: int
  * Default: 0
  * Valid Values: [0,...]
  * Importance: low

``fetch.metadata.thread.pool.size``
  Thread pool size to concurrently fetch metadata from batch coordinator

  * Type: int
  * Default: 8
  * Valid Values: [1,...]
  * Importance: low

``file.cleaner.interval.ms``
  The interval with which to clean up files marked for deletion.

  * Type: int
  * Default: 300000 (5 minutes)
  * Valid Values: [1,...]
  * Importance: low

``file.cleaner.retention.period.ms``
  The retention period for files marked for deletion.

  * Type: int
  * Default: 60000 (1 minute)
  * Valid Values: [1,...]
  * Importance: low

``file.merger.interval.ms``
  The interval with which to merge files.

  * Type: int
  * Default: 60000 (1 minute)
  * Valid Values: [1,...]
  * Importance: low

``file.merger.temp.dir``
  The temporary directory for file merging.

  * Type: string
  * Default: /tmp/inkless/merger
  * Valid Values: non-null string
  * Importance: low

``object.key.log.prefix.masked``
  Whether to log full object key path, or mask the prefix.

  * Type: boolean
  * Default: false
  * Importance: low

``produce.upload.thread.pool.size``
  Thread pool size to concurrently upload files to remote storage

  * Type: int
  * Default: 8
  * Valid Values: [1,...]
  * Importance: low

``retention.enforcement.interval.ms``
  The interval with which to enforce retention policies on a partition. This interval is approximate, because each scheduling event is randomized. The retention enforcement mechanism also takes into account the total number of brokers in the cluster: the more brokers, the less frequently each one of them enforces retention policy.

  * Type: int
  * Default: 300000 (5 minutes)
  * Valid Values: [1,...]
  * Importance: low

``retention.enforcement.max.batches.per.request``
  The maximum number of batches to delete per partition when enforcing retention. A value of 0 means all eligible batches are deleted in one request. Setting this to a lower value can help to reduce the load on the control plane back-end.

  * Type: int
  * Default: 0
  * Valid Values: [0,...]
  * Importance: low



-----------------
InMemoryControlPlaneConfig
-----------------
Under ``inkless.control.plane.``

``file.merge.lock.period.ms``
  The period of time when the file merge job is locked (assumed being performed).

  * Type: long
  * Default: 3600000 (1 hour)
  * Valid Values: [1,...]
  * Importance: medium

``file.merge.size.threshold.bytes``
  The total minimum volume of files to be merged together.

  * Type: long
  * Default: 104857600 (100 mebibytes)
  * Valid Values: [1,...]
  * Importance: medium



-----------------
PostgresControlPlaneConfig
-----------------
Under ``inkless.control.plane.``

``connection.string``
  PostgreSQL connection string

  * Type: string
  * Valid Values: non-empty string
  * Importance: high

``username``
  Username

  * Type: string
  * Valid Values: non-empty string
  * Importance: high

``password``
  Password

  * Type: password
  * Default: null
  * Importance: high

``file.merge.lock.period.ms``
  The period of time when the file merge job is locked (assumed being performed).

  * Type: long
  * Default: 3600000 (1 hour)
  * Valid Values: [1,...]
  * Importance: medium

``file.merge.size.threshold.bytes``
  The total minimum volume of files to be merged together.

  * Type: long
  * Default: 104857600 (100 mebibytes)
  * Valid Values: [1,...]
  * Importance: medium

``max.connections``
  Maximum number of connections to the database

  * Type: int
  * Default: 10
  * Valid Values: [1,...]
  * Importance: medium



-----------------
AzureBlobStorageConfig
-----------------
Under ``inkless.storage.``

``azure.container.name``
  Azure container to store log segments

  * Type: string
  * Valid Values: non-empty string
  * Importance: high

``azure.account.name``
  Azure account name

  * Type: string
  * Default: null
  * Valid Values: null or non-empty string
  * Importance: high

``azure.account.key``
  Azure account key

  * Type: password
  * Default: null
  * Valid Values: null or Non-empty password text
  * Importance: medium

``azure.connection.string``
  Azure connection string. Cannot be used together with azure.account.name, azure.account.key, and azure.endpoint.url

  * Type: password
  * Default: null
  * Valid Values: null or Non-empty password text
  * Importance: medium

``azure.sas.token``
  Azure SAS token

  * Type: password
  * Default: null
  * Valid Values: null or Non-empty password text
  * Importance: medium

``azure.upload.block.size``
  Size of blocks to use when uploading objects to Azure

  * Type: int
  * Default: 5242880
  * Valid Values: [102400,...,2147483647]
  * Importance: medium

``azure.endpoint.url``
  Custom Azure Blob Storage endpoint URL

  * Type: string
  * Default: null
  * Valid Values: null or Valid URL as defined in rfc2396
  * Importance: low



-----------------
GcsStorageConfig
-----------------
Under ``inkless.storage.``

``gcs.bucket.name``
  GCS bucket to store log segments

  * Type: string
  * Valid Values: non-empty string
  * Importance: high

``gcs.credentials.default``
  Use the default GCP credentials. Cannot be set together with "gcs.credentials.json" or "gcs.credentials.path"

  * Type: boolean
  * Default: null
  * Importance: medium

``gcs.credentials.json``
  GCP credentials as a JSON string. Cannot be set together with "gcs.credentials.path" or "gcs.credentials.default"

  * Type: password
  * Default: null
  * Valid Values: Non-empty password text
  * Importance: medium

``gcs.credentials.path``
  The path to a GCP credentials file. This can be standard GCP credentials format, or JSON with a single `access_token` field containing the access token with limited lifetime obtained from Google Authorization Server. Cannot be set together with "gcs.credentials.json" or "gcs.credentials.default"

  * Type: string
  * Default: null
  * Valid Values: non-empty string
  * Importance: medium

``gcs.endpoint.url``
  Custom GCS endpoint URL. To be used with custom GCS-compatible backends.

  * Type: string
  * Default: null
  * Valid Values: Valid URL as defined in rfc2396
  * Importance: low



-----------------
S3StorageConfig
-----------------
Under ``inkless.storage.``

``s3.bucket.name``
  S3 bucket to store log segments

  * Type: string
  * Valid Values: non-empty string
  * Importance: high

``s3.region``
  AWS region where S3 bucket is placed

  * Type: string
  * Importance: medium

``aws.access.key.id``
  AWS access key ID. To be used when static credentials are provided.

  * Type: password
  * Default: null
  * Valid Values: Non-empty password text
  * Importance: medium

``aws.checksum.check.enabled``
  This property is used to enable checksum validation done by AWS library. When set to "false", there will be no validation. It is disabled by default as Kafka already validates integrity of the files.

  * Type: boolean
  * Default: false
  * Importance: medium

``aws.credentials.file``
  This property is used to define a file where credentials are defined. The file must contain AWS credentials in the format as those would be in the properties file: inkless.storage.aws.access.key.id=, inkless.storage.aws.secret.access.key=, and inkless.storage.aws.session.token=.The file might be updated during process life cycle, and the credentials will be reloaded from the file.

  * Type: string
  * Default: null
  * Importance: medium

``aws.secret.access.key``
  AWS secret access key. To be used when static credentials are provided.

  * Type: password
  * Default: null
  * Valid Values: Non-empty password text
  * Importance: medium

``aws.session.token``
  The AWS session token. Retrieved from an AWS token service, used for authenticating that this user has received temporary permission to access some resource.

  * Type: password
  * Default: null
  * Valid Values: Non-empty password text
  * Importance: medium

``aws.certificate.check.enabled``
  This property is used to enable SSL certificate checking for AWS services. When set to "false", the SSL certificate checking for AWS services will be bypassed. Use with caution and always only in a test environment, as disabling certificate lead the storage to be vulnerable to man-in-the-middle attacks.

  * Type: boolean
  * Default: true
  * Importance: low

``aws.credentials.provider.class``
  AWS credentials provider. If not set, AWS SDK uses the default software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain

  * Type: class
  * Default: null
  * Valid Values: Any implementation of software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
  * Importance: low

``aws.http.max.connections``
  This max number of HTTP connections to keep in the client pool.

  * Type: int
  * Default: 150
  * Valid Values: [50,...]
  * Importance: low

``s3.api.call.attempt.timeout``
  AWS S3 API call attempt (single retry) timeout in milliseconds

  * Type: long
  * Default: null
  * Valid Values: null or [1,...,9223372036854775807]
  * Importance: low

``s3.api.call.timeout``
  AWS S3 API call timeout in milliseconds, including all retries

  * Type: long
  * Default: null
  * Valid Values: null or [1,...,9223372036854775807]
  * Importance: low

``s3.endpoint.url``
  Custom S3 endpoint URL. To be used with custom S3-compatible backends (e.g. minio).

  * Type: string
  * Default: null
  * Valid Values: Valid URL as defined in rfc2396
  * Importance: low

``s3.path.style.access.enabled``
  Whether to use path style access or virtual hosts. By default, empty value means S3 library will auto-detect. Amazon S3 uses virtual hosts by default (true), but other S3-compatible backends may differ (e.g. minio).

  * Type: boolean
  * Default: null
  * Importance: low



