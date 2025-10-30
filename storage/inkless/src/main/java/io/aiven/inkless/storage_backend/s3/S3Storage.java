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
import org.apache.kafka.common.utils.ByteBufferInputStream;

import com.groupcdg.pitest.annotations.CoverageIgnore;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.storage_backend.common.InvalidRangeException;
import io.aiven.inkless.storage_backend.common.KeyNotFoundException;
import io.aiven.inkless.storage_backend.common.StorageBackend;
import io.aiven.inkless.storage_backend.common.StorageBackendException;
import io.aiven.inkless.storage_backend.common.StorageBackendTimeoutException;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException;
import software.amazon.awssdk.core.exception.ApiCallTimeoutException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

@CoverageIgnore  // tested on integration level
public final class S3Storage extends StorageBackend {

    public static final int MAX_DELETE_KEYS_LIMIT = 1000;
    private S3Client s3Client;
    private String bucketName;

    // needed for reflection based instantiation
    public S3Storage() {
        this(new Metrics());
    }

    public S3Storage(final Metrics metrics) {
        super(metrics);
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        final S3StorageConfig config = new S3StorageConfig(configs);
        this.s3Client = S3ClientBuilder.build(metrics, config);
        this.bucketName = config.bucketName();
    }

    public void upload(final ObjectKey key, final InputStream inputStream, final long length) throws StorageBackendException {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(inputStream, "inputStream cannot be null");
        if (length <= 0) {
            throw new IllegalArgumentException("length must be positive");
        }
        final PutObjectRequest putObjectRequest = PutObjectRequest.builder()
            .bucket(bucketName)
            .key(key.value())
            .build();
        final RequestBody requestBody = RequestBody.fromInputStream(inputStream, length);
        try {
            s3Client.putObject(putObjectRequest, requestBody);
            int remaining = inputStream.read(new byte[]{1});
            if (remaining != -1) {
                throw new StorageBackendException(
                        "Object " + key + " created with incorrect length, input stream has remaining content");
            }
        } catch (final ApiCallTimeoutException | ApiCallAttemptTimeoutException e) {
            throw new StorageBackendTimeoutException("Failed to upload " + key, e);
        } catch (final IOException | SdkException e) {
            throw new StorageBackendException("Failed to upload " + key, e);
        }
    }

    @Override
    public ReadableByteChannel fetch(final ObjectKey key, final ByteRange range) throws StorageBackendException, IOException {
        try {
            if (range != null && range.empty()) {
                return Channels.newChannel(InputStream.nullInputStream());
            }

            var builder = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(key.value());
            if (range != null) {
                builder = builder.range(formatRange(range));
            }
            final GetObjectRequest getRequest = builder.build();
            // for the small 4-8MiB blobs expected here, reading the whole object into memory is more efficient
            // than streaming it via S3ObjectInputStream which has significant overhead per read call
            // and does not play well with the buffering done in ObjectFetcher.readToByteBuffer()
            final var buffer = s3Client.getObjectAsBytes(getRequest).asByteBuffer();
            return Channels.newChannel(new ByteBufferInputStream(buffer));
        } catch (final AwsServiceException e) {
            if (e.statusCode() == 404) {
                throw new KeyNotFoundException(this, key, e);
            }
            if (e.statusCode() == 416) {
                throw new InvalidRangeException("Failed to fetch " + key + ": Invalid range " + range, e);
            }

            throw new StorageBackendException("Failed to fetch " + key, e);
        } catch (final ApiCallTimeoutException | ApiCallAttemptTimeoutException e) {
            throw new StorageBackendTimeoutException("Failed to fetch " + key, e);
        } catch (final SdkClientException e) {
            throw new StorageBackendException("Failed to fetch " + key, e);
        }
    }

    private String formatRange(final ByteRange range) {
        return "bytes=" + range.offset() + "-" + range.endOffset();
    }

    @Override
    public void delete(final ObjectKey key) throws StorageBackendException {
        try {
            final var deleteRequest = DeleteObjectRequest.builder().bucket(bucketName).key(key.value()).build();
            s3Client.deleteObject(deleteRequest);
        } catch (final ApiCallTimeoutException | ApiCallAttemptTimeoutException e) {
            throw new StorageBackendTimeoutException("Failed to delete " + key, e);
        } catch (final SdkException e) {
            throw new StorageBackendException("Failed to delete " + key, e);
        }
    }

    @Override
    public void delete(final Set<ObjectKey> keys) throws StorageBackendException {
        final List<ObjectKey> objectKeys = new ArrayList<>(keys);
        try {
            for (int i = 0; i < objectKeys.size(); i += MAX_DELETE_KEYS_LIMIT) {
                final var batch = objectKeys.subList(
                    i,
                    Math.min(i + MAX_DELETE_KEYS_LIMIT, objectKeys.size())
                );

                final Set<ObjectIdentifier> ids = batch.stream()
                    .map(k -> ObjectIdentifier.builder().key(k.value()).build())
                    .collect(Collectors.toSet());
                final Delete delete = Delete.builder().objects(ids).build();
                final DeleteObjectsRequest deleteObjectsRequest = DeleteObjectsRequest.builder()
                    .bucket(bucketName)
                    .delete(delete)
                    .build();
                final DeleteObjectsResponse response = s3Client.deleteObjects(deleteObjectsRequest);

                if (!response.errors().isEmpty()) {
                    final var errors = response.errors().stream()
                        .map(e -> String.format("Error %s: %s (%s)", e.key(), e.message(), e.code()))
                        .collect(Collectors.joining(", "));
                    throw new StorageBackendException("Failed to delete keys " + keys + ": " + errors);
                }
            }
        } catch (final ApiCallTimeoutException | ApiCallAttemptTimeoutException e) {
            throw new StorageBackendTimeoutException("Failed to delete keys " + keys, e);
        } catch (final SdkException e) {
            throw new StorageBackendException("Failed to delete keys " + keys, e);
        }
    }

    @Override
    public void close() throws IOException {
        s3Client.close();
    }
}
