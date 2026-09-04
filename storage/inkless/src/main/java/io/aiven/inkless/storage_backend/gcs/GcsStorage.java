/*
 * Inkless
 * Copyright (C) 2024 - 2025 Aiven OY
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package io.aiven.inkless.storage_backend.gcs;

import org.apache.kafka.common.metrics.Metrics;

import com.google.cloud.BaseServiceException;
import com.google.cloud.ReadChannel;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.groupcdg.pitest.annotations.CoverageIgnore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.storage_backend.common.InvalidRangeException;
import io.aiven.inkless.storage_backend.common.KeyNotFoundException;
import io.aiven.inkless.storage_backend.common.SizedReadableByteChannel;
import io.aiven.inkless.storage_backend.common.StorageBackend;
import io.aiven.inkless.storage_backend.common.StorageBackendException;
import io.aiven.inkless.storage_backend.common.StorageBackendTimeoutException;

@CoverageIgnore  // tested on integration level
public class GcsStorage extends StorageBackend {
    private static final Logger LOGGER = LoggerFactory.getLogger(GcsStorage.class);

    // The client splits a delete into HTTP batches of at most 100 sub-requests, and the server has to
    // process every sub-request before it answers, so the response wait grows with the batch. Half the
    // client's maximum keeps that wait clear of gcs.read.timeout; a default cleaner cycle of 20000
    // files then costs 400 round trips, well inside file.cleaner.interval.ms.
    private static final int MAX_DELETE_BATCH_SIZE = 50;

    private volatile Storage storage;
    private String bucketName;
    private ReloadableCredentialsProvider credentialsProvider;
    private StorageOptions.Builder storageOptionsBuilder;
    private final MetricCollector metricCollector;

    // needed for reflection based instantiation
    public GcsStorage() {
        this(new Metrics());
    }

    public GcsStorage(final Metrics metrics) {
        super(metrics);
        this.metricCollector = new MetricCollector(metrics);
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        final GcsStorageConfig config = new GcsStorageConfig(configs);
        this.bucketName = config.bucketName();

        final HttpTransportOptions.Builder httpTransportOptionsBuilder = HttpTransportOptions.newBuilder();
        httpTransportOptionsBuilder.setConnectTimeout((int) config.connectTimeout().toMillis());
        httpTransportOptionsBuilder.setReadTimeout((int) config.readTimeout().toMillis());

        // Create reloadable credentials provider
        this.credentialsProvider = config.reloadableCredentials();

        // Store the builder template for recreating storage clients
        this.storageOptionsBuilder = StorageOptions.newBuilder()
            .setTransportOptions(metricCollector.httpTransportOptions(httpTransportOptionsBuilder));
        if (config.endpointUrl() != null) {
            this.storageOptionsBuilder.setHost(config.endpointUrl());
        }

        // Set up credentials reload callback to recreate storage client
        this.credentialsProvider.setCredentialsUpdateCallback(this::updateStorageClient);

        // Create initial storage client
        updateStorageClient(credentialsProvider.getCredentials());
    }

    @Override
    public void upload(final ObjectKey key, final InputStream inputStream, final long length) throws StorageBackendException {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(inputStream, "inputStream cannot be null");
        if (length <= 0) {
            throw new IllegalArgumentException("length must be positive");
        }
        try {
            final BlobInfo blobInfo = BlobInfo.newBuilder(this.bucketName, key.value()).build();
            Blob blob = storage.createFrom(blobInfo, inputStream);
            long transferred = blob.getSize();
            if (transferred != length) {
                throw new StorageBackendException(
                        "Object " + key + " created with incorrect length " + transferred + " instead of " + length);
            }
        } catch (final IOException | BaseServiceException e) {
            if (isTimeout(e)) {
                throw new StorageBackendTimeoutException("Timed out to upload " + key, e);
            }
            throw new StorageBackendException("Failed to upload " + key, e);
        }
    }

    // The GCS client wraps the originating SocketTimeoutException in a BaseServiceException, so the
    // whole cause chain is inspected.
    private static boolean isTimeout(final Throwable e) {
        Throwable cause = e;
        while (cause != null) {
            if (cause instanceof SocketTimeoutException) {
                return true;
            }
            cause = cause.getCause() == cause ? null : cause.getCause();
        }
        return false;
    }

    @Override
    public void delete(final ObjectKey key) throws StorageBackendException {
        try {
            storage.delete(this.bucketName, key.value());
        } catch (final BaseServiceException e) {
            if (isTimeout(e)) {
                throw new StorageBackendTimeoutException("Timed out to delete " + key, e);
            }
            throw new StorageBackendException("Failed to delete " + key, e);
        }
    }

    @Override
    public Set<ObjectKey> delete(final Set<ObjectKey> keys) throws StorageBackendException {
        final List<ObjectKey> objectKeys = new ArrayList<>(keys);
        final Set<ObjectKey> deleted = new HashSet<>();

        for (int i = 0; i < objectKeys.size(); i += MAX_DELETE_BATCH_SIZE) {
            final List<ObjectKey> batch = objectKeys.subList(
                i,
                Math.min(i + MAX_DELETE_BATCH_SIZE, objectKeys.size())
            );
            final Set<BlobId> ids = batch.stream()
                    .map(k -> BlobId.of(this.bucketName, k.value()))
                    .collect(Collectors.toSet());
            try {
                // storage.delete returns a List<Boolean> of deleted-vs-already-absent, but a genuine
                // failure surfaces as a thrown BaseServiceException rather than a per-blob flag, so a
                // batch stays all-or-nothing: on success every key in it is gone (idempotent), and on
                // failure none of it is confirmed.
                storage.delete(ids);
            } catch (final BaseServiceException e) {
                // Deletion is idempotent, so stopping here is safe: the keys left unconfirmed stay
                // marked for deletion and the next FileCleaner cycle retries them.
                LOGGER.warn("Batch delete failed after {} of {} keys, stopping the pass",
                    deleted.size(), objectKeys.size(), e);
                break;
            }
            deleted.addAll(batch);
            metricCollector.recordBatchDeleteObjects(batch.size());
        }

        return deleted;
    }

    @Override
    public ReadableByteChannel fetch(ObjectKey key, ByteRange range) throws StorageBackendException, IOException {
        try {
            if (range != null && range.empty()) {
                return SizedReadableByteChannel.empty();
            }

            final Blob blob = getBlob(key);

            if (range != null && range.offset() >= blob.getSize()) {
                throw new InvalidRangeException("Failed to fetch " + key + ": Invalid range " + range + " for blob size " + blob.getSize());
            }

            final long contentLength = range == null
                ? blob.getSize()
                : Math.min(range.size(), blob.getSize() - range.offset());

            final ReadChannel reader = blob.reader();
            // A chunk size of 0 makes the client read straight into the destination buffer.
            // Any positive size stages a second buffer of that size first,
            // and the caller already allocates a destination the size of the whole payload.
            reader.setChunkSize(0);
            if (range != null) {
                reader.limit(range.endOffset() + 1);
                reader.seek(range.offset());
            }
            return SizedReadableByteChannel.of(reader, SizedReadableByteChannel.exactLength(key, contentLength));
        } catch (final IOException e) {
            if (isTimeout(e)) {
                throw new StorageBackendTimeoutException("Timed out to fetch " + key, e);
            }
            throw new StorageBackendException("Failed to fetch " + key, e);
        } catch (final BaseServiceException e) {
            if (e.getCode() == 404) {
                // https://cloud.google.com/storage/docs/json_api/v1/status-codes#404_Not_Found
                throw new KeyNotFoundException(this, key, e);
            } else if (e.getCode() == 416) {
                // https://cloud.google.com/storage/docs/json_api/v1/status-codes#416_Requested_Range_Not_Satisfiable
                throw new InvalidRangeException("Failed to fetch " + key + ": Invalid range " + range, e);
            } else if (isTimeout(e)) {
                // Reaches only the metadata request. The body is streamed from the channel this
                // method returns, so a stall mid-download surfaces to the caller as a plain
                // IOException; ReadableByteChannel.read cannot throw StorageBackendException.
                throw new StorageBackendTimeoutException("Timed out to fetch " + key, e);
            } else {
                throw new StorageBackendException("Failed to fetch " + key, e);
            }
        }
    }

    private Blob getBlob(final ObjectKey key) throws KeyNotFoundException {
        final Blob blob = storage.get(this.bucketName, key.value());
        if (blob == null) {
            throw new KeyNotFoundException(this, key);
        }
        return blob;
    }

    /**
     * Updates the storage client with new credentials.
     * This method is called when credentials are reloaded.
     *
     * @param credentials the new credentials to use
     */
    protected void updateStorageClient(final com.google.auth.Credentials credentials) {
        synchronized (this) {
            this.storage = storageOptionsBuilder
                .setCredentials(credentials)
                .build()
                .getService();
        }
    }

    @Override
    public String toString() {
        return "GCSStorage{"
            + "bucketName='" + bucketName + '\''
            + '}';
    }

    @Override
    public void close() throws IOException {
        try {
            if (storage != null) {
                storage.close();
            }
            if (credentialsProvider != null) {
                credentialsProvider.close();
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
}
