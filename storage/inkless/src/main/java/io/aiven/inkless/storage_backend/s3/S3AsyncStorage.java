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
package io.aiven.inkless.storage_backend.s3;

import org.apache.kafka.common.metrics.Metrics;

import com.groupcdg.pitest.annotations.CoverageIgnore;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.storage_backend.common.InvalidRangeException;
import io.aiven.inkless.storage_backend.common.KeyNotFoundException;
import io.aiven.inkless.storage_backend.common.Storage;
import io.aiven.inkless.storage_backend.common.StorageBackendException;
import io.aiven.inkless.storage_backend.common.StorageBackendTimeoutException;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException;
import software.amazon.awssdk.core.exception.ApiCallTimeoutException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * Async S3 storage backend using AWS CRT for true non-blocking I/O.
 *
 * <p>This implementation uses S3AsyncClient backed by AwsCrtAsyncHttpClient which provides
 * non-blocking network operations without consuming JVM threads during I/O.
 */
@CoverageIgnore  // tested on integration level
public final class S3AsyncStorage implements Storage {

    public static final int MAX_DELETE_KEYS_LIMIT = 1000;

    private final Metrics metrics;
    private S3AsyncClient s3Client;
    private String bucketName;

    // Needed for reflection based instantiation
    public S3AsyncStorage() {
        this(new Metrics());
    }

    public S3AsyncStorage(final Metrics metrics) {
        this.metrics = metrics;
    }

    public void configure(final Map<String, ?> configs) {
        final S3StorageConfig config = new S3StorageConfig(configs);
        this.s3Client = S3AsyncClientBuilder.build(metrics, config);
        this.bucketName = config.bucketName();
    }

    @Override
    public CompletableFuture<Void> upload(final ObjectKey key, final ByteBuffer byteBuffer) {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(byteBuffer, "byteBuffer cannot be null");
        if (byteBuffer.remaining() <= 0) {
            return CompletableFuture.failedFuture(
                new IllegalArgumentException("byteBuffer must have remaining bytes"));
        }

        final PutObjectRequest request = PutObjectRequest.builder()
            .bucket(bucketName)
            .key(key.value())
            .build();

        // AsyncRequestBody.fromByteBuffer() handles the buffer correctly
        final AsyncRequestBody body = AsyncRequestBody.fromByteBuffer(byteBuffer.duplicate());

        return s3Client.putObject(request, body)
            .thenApply(response -> (Void) null)
            .exceptionally(t -> mapUploadException(t, key));
    }

    @Override
    public CompletableFuture<ByteBuffer> fetch(final ObjectKey key, final ByteRange range) {
        Objects.requireNonNull(key, "key cannot be null");

        if (range != null && range.empty()) {
            return CompletableFuture.completedFuture(ByteBuffer.allocate(0));
        }

        var builder = GetObjectRequest.builder()
            .bucket(bucketName)
            .key(key.value());
        if (range != null) {
            builder = builder.range(formatRange(range));
        }
        final GetObjectRequest request = builder.build();

        return s3Client.getObject(request, AsyncResponseTransformer.toBytes())
            .thenApply(response -> response.asByteBuffer())
            .exceptionally(t -> mapFetchException(t, key, range));
    }

    @Override
    public CompletableFuture<Void> delete(final ObjectKey key) {
        Objects.requireNonNull(key, "key cannot be null");

        final DeleteObjectRequest request = DeleteObjectRequest.builder()
            .bucket(bucketName)
            .key(key.value())
            .build();

        return s3Client.deleteObject(request)
            .thenApply(response -> (Void) null)
            .exceptionally(t -> mapDeleteException(t, key));
    }

    @Override
    public CompletableFuture<Void> delete(final Set<ObjectKey> keys) {
        Objects.requireNonNull(keys, "keys cannot be null");

        if (keys.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        final List<ObjectKey> objectKeys = new ArrayList<>(keys);
        return deleteInBatches(objectKeys, 0);
    }

    private CompletableFuture<Void> deleteInBatches(final List<ObjectKey> keys, final int startIndex) {
        if (startIndex >= keys.size()) {
            return CompletableFuture.completedFuture(null);
        }

        final int endIndex = Math.min(startIndex + MAX_DELETE_KEYS_LIMIT, keys.size());
        final List<ObjectKey> batch = keys.subList(startIndex, endIndex);

        final Set<ObjectIdentifier> ids = batch.stream()
            .map(k -> ObjectIdentifier.builder().key(k.value()).build())
            .collect(Collectors.toSet());
        final Delete delete = Delete.builder().objects(ids).build();
        final DeleteObjectsRequest request = DeleteObjectsRequest.builder()
            .bucket(bucketName)
            .delete(delete)
            .build();

        return s3Client.deleteObjects(request)
            .thenCompose(response -> {
                if (!response.errors().isEmpty()) {
                    final var errors = response.errors().stream()
                        .map(e -> String.format("Error %s: %s (%s)", e.key(), e.message(), e.code()))
                        .collect(Collectors.joining(", "));
                    return CompletableFuture.failedFuture(
                        new StorageBackendException("Failed to delete keys: " + errors));
                }
                return deleteInBatches(keys, endIndex);
            })
            .exceptionally(t -> mapBatchDeleteException(t, keys));
    }

    private String formatRange(final ByteRange range) {
        return "bytes=" + range.offset() + "-" + range.endOffset();
    }

    private <T> T mapUploadException(final Throwable t, final ObjectKey key) {
        throw wrapException("upload " + key, unwrap(t));
    }

    private ByteBuffer mapFetchException(final Throwable t, final ObjectKey key, final ByteRange range) {
        final Throwable cause = unwrap(t);
        if (cause instanceof S3Exception s3ex) {
            if (s3ex.statusCode() == 404) {
                throw new CompletionException(new KeyNotFoundException(this, key, s3ex));
            }
            if (s3ex.statusCode() == 416) {
                throw new CompletionException(new InvalidRangeException(
                    "Failed to fetch " + key + ": Invalid range " + range, s3ex));
            }
        }
        throw wrapException("fetch " + key, cause);
    }

    private <T> T mapDeleteException(final Throwable t, final ObjectKey key) {
        throw wrapException("delete " + key, unwrap(t));
    }

    private <T> T mapBatchDeleteException(final Throwable t, final List<ObjectKey> keys) {
        final Throwable cause = unwrap(t);
        if (cause instanceof StorageBackendException) {
            throw new CompletionException(cause);
        }
        throw wrapException("delete keys " + keys, cause);
    }

    private Throwable unwrap(final Throwable t) {
        return t instanceof CompletionException ? t.getCause() : t;
    }

    private CompletionException wrapException(final String operation, final Throwable cause) {
        if (cause instanceof ApiCallTimeoutException || cause instanceof ApiCallAttemptTimeoutException) {
            return new CompletionException(new StorageBackendTimeoutException(
                "Failed to " + operation, cause));
        }
        if (cause instanceof SdkException) {
            return new CompletionException(new StorageBackendException(
                "Failed to " + operation, cause));
        }
        if (cause instanceof StorageBackendException) {
            return new CompletionException(cause);
        }
        return new CompletionException(new StorageBackendException(
            "Unexpected error during " + operation, cause));
    }

    @Override
    public void close() throws IOException {
        if (s3Client != null) {
            s3Client.close();
        }
    }
}
