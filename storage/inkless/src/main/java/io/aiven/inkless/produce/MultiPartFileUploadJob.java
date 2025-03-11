// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import org.apache.kafka.common.utils.Time;

import java.io.InputStream;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Consumer;

import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.storage_backend.common.ObjectUploader;
import io.aiven.inkless.storage_backend.common.StorageBackendException;

/**
 * The job of uploading a file to the object storage using multipart upload.
 */
public class MultiPartFileUploadJob extends AbstractFileUploadJob {
    private final ObjectUploader objectUploader;
    private final InputStream data;


    public MultiPartFileUploadJob(final ObjectKeyCreator objectKeyCreator,
                                  final ObjectUploader objectUploader,
                                  final Time time,
                                  final int attempts,
                                  final Duration retryBackoff,
                                  final InputStream data,
                                  final Consumer<Long> durationCallback) {
        super(objectKeyCreator, time, attempts, retryBackoff, durationCallback);
        this.objectUploader = Objects.requireNonNull(objectUploader, "objectUploader cannot be null");
        this.data = Objects.requireNonNull(data, "data cannot be null");
    }


    @Override
    void upload(ObjectKey objectKey) throws StorageBackendException {
        objectUploader.uploadMultiPart(objectKey, data);
    }
}
