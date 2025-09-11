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

import com.google.cloud.BaseServiceException;
import com.google.cloud.ReadChannel;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.groupcdg.pitest.annotations.CoverageIgnore;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
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

@CoverageIgnore  // tested on integration level
public class GcsStorage implements StorageBackend {
    /**
     * The file extent max size is 16 MiB. Most files are few megabytes but usually over 2 MiB.
     * Good value here is to allow most files to be fetched in single chunk. Worst case is to load
     * the file in two chunks.
     */
    private static final int READER_8MB_CHUNK_SIZE = 1024 * 1024 * 8;

    private Storage storage;
    private String bucketName;

    @Override
    public void configure(final Map<String, ?> configs) {
        final GcsStorageConfig config = new GcsStorageConfig(configs);
        this.bucketName = config.bucketName();

        final HttpTransportOptions.Builder httpTransportOptionsBuilder = HttpTransportOptions.newBuilder();

        final StorageOptions.Builder builder = StorageOptions.newBuilder()
            .setCredentials(config.credentials())
            .setTransportOptions(new MetricCollector().httpTransportOptions(httpTransportOptionsBuilder));
        if (config.endpointUrl() != null) {
            builder.setHost(config.endpointUrl());
        }
        storage = builder.build().getService();
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
            throw new StorageBackendException("Failed to upload " + key, e);
        }
    }

    @Override
    public void delete(final ObjectKey key) throws StorageBackendException {
        try {
            storage.delete(this.bucketName, key.value());
        } catch (final BaseServiceException e) {
            throw new StorageBackendException("Failed to delete " + key, e);
        }
    }

    @Override
    public void delete(final Set<ObjectKey> keys) throws StorageBackendException {
        try {
            final Set<BlobId> ids = keys.stream()
                    .map(k -> BlobId.of(this.bucketName,k.value()))
                    .collect(Collectors.toSet());

            storage.delete(ids);
        } catch (final BaseServiceException e) {
            throw new StorageBackendException("Failed to delete " + keys, e);
        }
    }

    @Override
    public InputStream fetch(ObjectKey key, ByteRange range) throws StorageBackendException {
        try {
            if (range != null && range.empty()) {
                return InputStream.nullInputStream();
            }

            final Blob blob = getBlob(key);

            if (range != null && range.offset() >= blob.getSize()) {
                throw new InvalidRangeException("Failed to fetch " + key + ": Invalid range " + range + " for blob size " + blob.getSize());
            }

            final ReadChannel reader = blob.reader();
            reader.setChunkSize(READER_8MB_CHUNK_SIZE);
            if (range != null) {
                reader.limit(range.endOffset() + 1);
                reader.seek(range.offset());
            }
            return Channels.newInputStream(reader);
        } catch (final IOException e) {
            throw new StorageBackendException("Failed to fetch " + key, e);
        } catch (final BaseServiceException e) {
            if (e.getCode() == 404) {
                // https://cloud.google.com/storage/docs/json_api/v1/status-codes#404_Not_Found
                throw new KeyNotFoundException(this, key, e);
            } else if (e.getCode() == 416) {
                // https://cloud.google.com/storage/docs/json_api/v1/status-codes#416_Requested_Range_Not_Satisfiable
                throw new InvalidRangeException("Failed to fetch " + key + ": Invalid range " + range, e);
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

    @Override
    public String toString() {
        return "GCSStorage{"
            + "bucketName='" + bucketName + '\''
            + '}';
    }
}
