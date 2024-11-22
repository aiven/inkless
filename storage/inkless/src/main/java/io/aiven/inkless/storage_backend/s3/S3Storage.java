// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.storage_backend.s3;

import java.io.InputStream;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.storage_backend.common.InvalidRangeException;
import io.aiven.inkless.storage_backend.common.KeyNotFoundException;
import io.aiven.inkless.storage_backend.common.StorageBackend;
import io.aiven.inkless.storage_backend.common.StorageBackendException;
import io.aiven.inkless.storage_backend.common.StorageBackendTimeoutException;

import com.groupcdg.pitest.annotations.CoverageIgnore;
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
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

@CoverageIgnore  // tested on integration level
public class S3Storage implements StorageBackend {

    private S3Client s3Client;
    private String bucketName;

    @Override
    public void configure(final Map<String, ?> configs) {
        final S3StorageConfig config = new S3StorageConfig(configs);
        this.s3Client = S3ClientBuilder.build(config);
        this.bucketName = config.bucketName();
    }

    @Override
    public void upload(final ObjectKey key, final byte[] data) throws StorageBackendException {
        final PutObjectRequest putObjectRequest = PutObjectRequest.builder()
            .bucket(bucketName)
            .key(key.value())
            .build();
        final RequestBody requestBody = RequestBody.fromBytes(data);
        try {
            s3Client.putObject(putObjectRequest, requestBody);
        } catch (final ApiCallTimeoutException | ApiCallAttemptTimeoutException e) {
            throw new StorageBackendTimeoutException("Failed to upload " + key, e);
        } catch (final SdkException e) {
            throw new StorageBackendException("Failed to upload " + key, e);
        }
    }

    @Override
    public InputStream fetch(final ObjectKey key, final ByteRange range) throws StorageBackendException {
        try {
            if (range.empty()) {
                return InputStream.nullInputStream();
            }

            final GetObjectRequest getRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(key.value())
                .range(formatRange(range))
                .build();
            return s3Client.getObject(getRequest);
        } catch (final AwsServiceException e) {
            if (e.statusCode() == 404) {
                throw new KeyNotFoundException(this, key, e);
            }
            if (e.statusCode() == 416) {
                throw new InvalidRangeException("Invalid range " + range, e);
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
        try {
            final Set<ObjectIdentifier> ids = keys.stream()
                .map(k -> ObjectIdentifier.builder().key(k.value()).build())
                .collect(Collectors.toSet());
            final Delete delete = Delete.builder().objects(ids).build();
            final DeleteObjectsRequest deleteObjectsRequest = DeleteObjectsRequest.builder()
                .bucket(bucketName)
                .delete(delete)
                .build();
            s3Client.deleteObjects(deleteObjectsRequest);
        } catch (final ApiCallTimeoutException | ApiCallAttemptTimeoutException e) {
            throw new StorageBackendTimeoutException("Failed to delete keys " + keys, e);
        } catch (final SdkException e) {
            throw new StorageBackendException("Failed to delete keys " + keys, e);
        }
    }
}
