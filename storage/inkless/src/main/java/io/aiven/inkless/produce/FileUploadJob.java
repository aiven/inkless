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
package io.aiven.inkless.produce;

import org.apache.kafka.common.utils.Time;

import java.time.Duration;
import java.util.Objects;
import java.util.function.Consumer;

import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.storage_backend.common.ObjectUploader;
import io.aiven.inkless.storage_backend.common.StorageBackendException;

/**
 * The job of uploading a file to the object storage.
 */
public class FileUploadJob extends AbstractFileUploadJob  {
    private final ObjectUploader objectUploader;
    private final byte[] data;

    public FileUploadJob(final ObjectKeyCreator objectKeyCreator,
                                  final ObjectUploader objectUploader,
                                  final Time time,
                                  final int attempts,
                                  final Duration retryBackoff,
                                  final byte[] data,
                                  final Consumer<Long> durationCallback) {
        super(objectKeyCreator, time, attempts, retryBackoff, durationCallback);
        this.objectUploader = Objects.requireNonNull(objectUploader, "objectUploader cannot be null");
        this.data = Objects.requireNonNull(data, "data cannot be null");
    }


    @Override
    void upload(ObjectKey objectKey) throws StorageBackendException {
        objectUploader.upload(objectKey, data);
    }
}
