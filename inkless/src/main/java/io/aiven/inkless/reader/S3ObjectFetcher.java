package io.aiven.inkless.reader;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.kafka.common.record.MemoryRecords;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

public class S3ObjectFetcher implements ObjectFetcher {
    private final S3Client s3Client;

    public S3ObjectFetcher(final S3Client s3Client) {
        this.s3Client = s3Client;
    }

    @Override
    public byte[] fetch(final String objectKey, final long offset, final long size) {
        final GetObjectRequest getObjectRequest = GetObjectRequest.builder()
            .key(objectKey)
            .bucket("my-bucket")
            .range(String.format("bytes=%d-%d", offset, size))
            .build();
        final ResponseInputStream<GetObjectResponse> responseInputStream = s3Client.getObject(getObjectRequest);
        final byte[] data;
        try {
            data = responseInputStream.readNBytes((int) size);
        } catch (final IOException e) {
            // TODO handle
            throw new RuntimeException(e);
        }
        return data;
    }
}
