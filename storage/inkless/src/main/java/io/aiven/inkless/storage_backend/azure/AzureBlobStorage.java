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

package io.aiven.inkless.storage_backend.azure;

import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ParallelTransferOptions;
import com.azure.storage.blob.options.BlockBlobOutputStreamOptions;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.azure.storage.blob.specialized.SpecializedBlobClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.groupcdg.pitest.annotations.CoverageIgnore;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.storage_backend.common.InvalidRangeException;
import io.aiven.inkless.storage_backend.common.KeyNotFoundException;
import io.aiven.inkless.storage_backend.common.StorageBackend;
import io.aiven.inkless.storage_backend.common.StorageBackendException;
import reactor.core.Exceptions;

@CoverageIgnore // tested on integration level
public class AzureBlobStorage implements StorageBackend {
    private MetricCollector metricCollector;

    private AzureBlobStorageConfig config;
    private BlobContainerClient blobContainerClient;

    @Override
    public void configure(final Map<String, ?> configs) {
        this.config = new AzureBlobStorageConfig(configs);

        final BlobServiceClientBuilder blobServiceClientBuilder = new BlobServiceClientBuilder();
        if (config.connectionString() != null) {
            blobServiceClientBuilder.connectionString(config.connectionString());
        } else {
            blobServiceClientBuilder.endpoint(endpointUrl());

            if (config.accountKey() != null) {
                blobServiceClientBuilder.credential(
                    new StorageSharedKeyCredential(config.accountName(), config.accountKey()));
            } else if (config.sasToken() != null) {
                blobServiceClientBuilder.sasToken(config.sasToken());
            } else {
                blobServiceClientBuilder.credential(
                    new DefaultAzureCredentialBuilder().build());
            }
        }


        metricCollector = MetricCollector.get(config);

        blobContainerClient = blobServiceClientBuilder
            .addPolicy(metricCollector.policy())
            .buildClient()
            .getBlobContainerClient(config.containerName());
    }

    private String endpointUrl() {
        if (config.endpointUrl() != null) {
            return config.endpointUrl();
        } else {
            return "https://" + config.accountName() + ".blob.core.windows.net";
        }
    }

    @Override
    public void upload(final ObjectKey key, InputStream inputStream, long length) throws StorageBackendException {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(inputStream, "inputStream cannot be null");
        if (length <= 0) {
            throw new IllegalArgumentException("length must be positive");
        }
        final var specializedBlobClientBuilder = new SpecializedBlobClientBuilder();
        if (config.connectionString() != null) {
            specializedBlobClientBuilder.connectionString(config.connectionString());
        } else {
            specializedBlobClientBuilder.endpoint(endpointUrl());

            if (config.accountKey() != null) {
                specializedBlobClientBuilder.credential(
                    new StorageSharedKeyCredential(config.accountName(), config.accountKey()));
            } else if (config.sasToken() != null) {
                specializedBlobClientBuilder.sasToken(config.sasToken());
            } else {
                specializedBlobClientBuilder.credential(
                    new DefaultAzureCredentialBuilder().build());
            }
        }

        final BlockBlobClient blockBlobClient = specializedBlobClientBuilder
            .addPolicy(metricCollector.policy())
            .containerName(config.containerName())
            .blobName(key.value())
            .buildBlockBlobClient();

        final long blockSizeLong = config.uploadBlockSize();
        final ParallelTransferOptions parallelTransferOptions = new ParallelTransferOptions()
            .setBlockSizeLong(blockSizeLong);
        // Setting this is important, because otherwise if the size is below 256 MiB,
        // block upload won't be used and up to 256 MiB may be cached in memory.
        parallelTransferOptions.setMaxSingleUploadSizeLong(blockSizeLong);
        final BlockBlobOutputStreamOptions options = new BlockBlobOutputStreamOptions()
            .setParallelTransferOptions(parallelTransferOptions);
        // Be aware that metrics instrumentation is based on PutBlob (single upload), PutBlock (upload part),
        // and PutBlockList (complete upload) used by this call.
        // If upload changes, change metrics instrumentation accordingly.
        try (OutputStream os = new BufferedOutputStream(
            blockBlobClient.getBlobOutputStream(options), config.uploadBlockSize())) {
            long transferred = inputStream.transferTo(os);
            if (transferred != length) {
                throw new StorageBackendException(
                        "Object " + key + " created with incorrect length " + transferred + " instead of " + length);
            }
        } catch (final IOException e) {
            throw new StorageBackendException("Failed to upload " + key, e);
        } catch (final RuntimeException e) {
            throw unwrapReactorExceptions(e, "Failed to upload " + key);
        }
    }

    @Override
    public ReadableByteChannel fetch(final ObjectKey key, final ByteRange range) throws StorageBackendException, IOException {
        try {
            if (range!= null && range.empty()) {
                return Channels.newChannel(InputStream.nullInputStream());
            }
            final ReadableByteChannel channel;
            if (range != null) {
                channel = Channels.newChannel(blobContainerClient.getBlobClient(key.value()).openInputStream(
                        new BlobRange(range.offset(), range.size()), null));
            } else {
                channel = Channels.newChannel(blobContainerClient.getBlobClient(key.value()).openInputStream());
            }
            return channel;
        } catch (final BlobStorageException e) {
            if (e.getStatusCode() == 404) {
                throw new KeyNotFoundException(this, key, e);
            } else if (e.getStatusCode() == 416) {
                throw new InvalidRangeException("Failed to fetch " + key + ": Invalid range " + range, e);
            } else {
                throw new StorageBackendException("Failed to fetch " + key, e);
            }
        } catch (final RuntimeException e) {
            throw unwrapReactorExceptions(e, "Failed to fetch " + key);
        }
    }

    @Override
    public void delete(final ObjectKey key) throws StorageBackendException {
        try {
            blobContainerClient.getBlobClient(key.value()).deleteIfExists();
        } catch (final BlobStorageException e) {
            throw new StorageBackendException("Failed to delete " + key, e);
        } catch (final RuntimeException e) {
            throw unwrapReactorExceptions(e, "Failed to delete " + key);
        }
    }

    @Override
    public void delete(final Set<ObjectKey> keys) throws StorageBackendException {
        try {
            for (ObjectKey key : keys) {
                blobContainerClient.getBlobClient(key.value()).deleteIfExists();
            }
        } catch (final BlobStorageException e) {
            throw new StorageBackendException("Failed to delete " + keys, e);
        } catch (final RuntimeException e) {
            throw unwrapReactorExceptions(e, "Failed to delete " + keys);
        }
    }

    private StorageBackendException unwrapReactorExceptions(final RuntimeException e, final String message) {
        final Throwable unwrapped = Exceptions.unwrap(e);
        if (unwrapped != e) {
            return new StorageBackendException(message, unwrapped);
        } else {
            throw e;
        }
    }

    @Override
    public String toString() {
        return "AzureStorage{"
            + "containerName='" + config.containerName() + '\''
            + '}';
    }
}
