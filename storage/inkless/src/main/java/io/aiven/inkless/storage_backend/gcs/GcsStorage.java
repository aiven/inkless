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

import com.google.api.client.http.apache.v2.ApacheHttpTransport;
import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.BaseServiceException;
import com.google.cloud.ReadChannel;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.groupcdg.pitest.annotations.CoverageIgnore;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.storage_backend.common.InvalidRangeException;
import io.aiven.inkless.storage_backend.common.KeyNotFoundException;
import io.aiven.inkless.storage_backend.common.StorageBackend;
import io.aiven.inkless.storage_backend.common.StorageBackendException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.threeten.bp.Duration;

@CoverageIgnore  // tested on integration level
public class GcsStorage implements StorageBackend {
    public static final int MAX_DELETE_KEYS_LIMIT = 1000;
    private static final int OPTIMAL_CHUNK_SIZE = 256 * 1024; // 256 KB
    private static final int DEFAULT_MAX_CONNECTIONS = 100;
    private static final int DEFAULT_MAX_CONNECTIONS_PER_ROUTE = 20;
    private static final int CONNECTION_TIMEOUT_MS = 30_000;
    private static final int SOCKET_TIMEOUT_MS = 30_000;
    private static final int CONNECTION_TTL_MS = 60_000;

    private Storage storage;
    private String bucketName;
    private PoolingHttpClientConnectionManager connectionManager;

    @Override
    public void configure(final Map<String, ?> configs) {
        final GcsStorageConfig config = new GcsStorageConfig(configs);
        this.bucketName = config.bucketName();

        // Initialize connection manager with optimal pool settings
        connectionManager = new PoolingHttpClientConnectionManager();
        connectionManager.setMaxTotal(DEFAULT_MAX_CONNECTIONS);
        connectionManager.setDefaultMaxPerRoute(DEFAULT_MAX_CONNECTIONS_PER_ROUTE);
        connectionManager.setValidateAfterInactivity(CONNECTION_TTL_MS / 2);

        final HttpTransportOptions.Builder httpTransportOptionsBuilder = HttpTransportOptions.newBuilder()
            .setConnectTimeout(CONNECTION_TIMEOUT_MS)
            .setReadTimeout(SOCKET_TIMEOUT_MS)
            .setHttpTransportFactory(() -> {
                RequestConfig requestConfig = RequestConfig.custom()
                    .setConnectTimeout(CONNECTION_TIMEOUT_MS)
                    .setConnectionRequestTimeout(CONNECTION_TIMEOUT_MS / 2)
                    .setSocketTimeout(SOCKET_TIMEOUT_MS)
                    .build();

                HttpClientBuilder clientBuilder = HttpClientBuilder.create()
                    .setConnectionManager(connectionManager)
                    .setConnectionManagerShared(true)  // Don't close connection manager when client is closed
                    .setDefaultRequestConfig(requestConfig)
                    .setConnectionTimeToLive(CONNECTION_TTL_MS, TimeUnit.MILLISECONDS)
                    .setKeepAliveStrategy((response, context) -> CONNECTION_TTL_MS);

                return new ApacheHttpTransport(clientBuilder.build());
            });

        final RetrySettings retrySettings = RetrySettings.newBuilder()
            .setInitialRetryDelay(Duration.ofMillis(100))
            .setRetryDelayMultiplier(1.5)
            .setMaxRetryDelay(Duration.ofSeconds(3))
            .setInitialRpcTimeout(Duration.ofSeconds(5))
            .setRpcTimeoutMultiplier(1.2)
            .setMaxRpcTimeout(Duration.ofSeconds(15))
            .setTotalTimeout(Duration.ofSeconds(30))
            .setMaxAttempts(3)
            .build();

        final StorageOptions.Builder builder = StorageOptions.newBuilder()
            .setCredentials(config.credentials())
            .setRetrySettings(retrySettings)
            .setTransportOptions(new MetricCollector().httpTransportOptions(httpTransportOptionsBuilder))
            // Optimize headers to reduce JSON processing overhead
            .setHeaderProvider(() -> Map.of(
                "Accept-Encoding", "gzip,deflate",
                "Connection", "keep-alive"
            ));
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
        if (keys.isEmpty()) {
            return;
        }

        try {
            final List<ObjectKey> keyList = new ArrayList<>(keys);

            // Process in batches to avoid overwhelming the API
            for (int i = 0; i < keyList.size(); i += MAX_DELETE_KEYS_LIMIT) {
                int endIndex = Math.min(i + MAX_DELETE_KEYS_LIMIT, keyList.size());
                List<ObjectKey> batch = keyList.subList(i, endIndex);

                final List<BlobId> blobIds = batch.stream()
                    .map(k -> BlobId.of(this.bucketName, k.value()))
                    .collect(Collectors.toList());

                storage.delete(blobIds);
            }
        } catch (final BaseServiceException e) {
            throw new StorageBackendException("Failed to delete " + keys.size() + " keys", e);
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
            return createOptimizedInputStream(blob, range);
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

    /**
     * Creates an optimized InputStream that avoids the heavy BufferedInputStream.read
     * calls seen in the profiler by using direct ByteBuffer operations.
     */
    private InputStream createOptimizedInputStream(Blob blob, ByteRange range) throws IOException {
        final ReadChannel reader = blob.reader();

        try {
            // Configure channel for optimal chunk size
            reader.setChunkSize(OPTIMAL_CHUNK_SIZE);

            if (range != null) {
                reader.limit(range.endOffset() + 1);
                reader.seek(range.offset());
            }

            // For small ranges, read entirely into memory to avoid multiple channel reads
            long contentLength = range != null ? range.size() : blob.getSize();

            if (contentLength <= 4 * 1024 * 1024) { // 4MB threshold
                return readEntirelyToMemory(reader, (int) contentLength);
            } else {
                // For large content, use optimized channel wrapper
                return new OptimizedChannelInputStream(reader);
            }

        } catch (Exception e) {
            reader.close();
            throw e;
        }
    }

    /**
     * Read small content entirely to memory to avoid multiple channel operations
     */
    private InputStream readEntirelyToMemory(ReadChannel reader, int contentLength) throws IOException {
        try (ReadChannel channel = reader) {
            ByteBuffer buffer = ByteBuffer.allocate(contentLength);
            while (buffer.hasRemaining()) {
                int bytesRead = channel.read(buffer);
                if (bytesRead == -1) {
                    if (buffer.position() == 0) {
                        return InputStream.nullInputStream(); // Empty content
                    }
                    break; // End of channel reached
                }
            }
            return new ByteArrayInputStream(buffer.array(), 0, buffer.position());
        }
    }

    /**
     * Optimized InputStream wrapper that reduces the BufferedInputStream overhead
     * seen in the profiler by using larger read buffers and direct ByteBuffer operations.
     */
    private static class OptimizedChannelInputStream extends InputStream {
        private final ReadChannel channel;
        private final ByteBuffer buffer;
        private boolean closed = false;

        OptimizedChannelInputStream(ReadChannel channel) {
            this.channel = channel;
            // Use direct buffer to avoid array copying overhead
            this.buffer = ByteBuffer.allocateDirect(OPTIMAL_CHUNK_SIZE);
            this.buffer.flip(); // Start in "empty" state
        }

        @Override
        public int read() throws IOException {
            if (closed) {
                throw new IOException("Stream closed");
            }

            if (!buffer.hasRemaining()) {
                buffer.clear();
                int bytesRead = channel.read(buffer);
                if (bytesRead == -1) {
                    return -1;
                }
                buffer.flip();
            }

            return buffer.get() & 0xFF;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (closed) {
                throw new IOException("Stream closed");
            }

            if (b == null) {
                throw new NullPointerException();
            }
            if (off < 0 || len < 0 || off + len > b.length) {
                throw new IndexOutOfBoundsException();
            }
            if (len == 0) {
                return 0;
            }

            if (!buffer.hasRemaining()) {
                buffer.clear();
                int bytesRead = channel.read(buffer);
                if (bytesRead == -1) {
                    return -1;
                }
                buffer.flip();
            }

            int bytesToRead = Math.min(len, buffer.remaining());
            buffer.get(b, off, bytesToRead);
            return bytesToRead;
        }

        @Override
        public void close() {
            if (!closed) {
                closed = true;
                channel.close();
            }
        }

        @Override
        public int available() {
            return closed ? 0 : buffer.remaining();
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

    /**
     * Closes resources used by this storage backend
     */
    public void close() {
        if (connectionManager != null) {
            connectionManager.close();
            connectionManager = null;
        }
    }
}
