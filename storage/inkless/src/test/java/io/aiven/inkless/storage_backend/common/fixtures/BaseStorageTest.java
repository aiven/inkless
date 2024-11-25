// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.storage_backend.common.fixtures;

import org.assertj.core.util.Throwables;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.storage_backend.common.InvalidRangeException;
import io.aiven.inkless.storage_backend.common.KeyNotFoundException;
import io.aiven.inkless.storage_backend.common.StorageBackend;
import io.aiven.inkless.storage_backend.common.StorageBackendException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class BaseStorageTest {

    protected static final ObjectKey TOPIC_PARTITION_SEGMENT_KEY = new TestObjectKey("key");

    protected abstract StorageBackend storage();

    @Test
    void testUploadFetchDelete() throws IOException, StorageBackendException {
        final StorageBackend storage = storage();
        final byte[] data = "some file".getBytes();

        storage.upload(TOPIC_PARTITION_SEGMENT_KEY, data);

        try (final InputStream fetch = storage.fetch(TOPIC_PARTITION_SEGMENT_KEY, new ByteRange(0, data.length))) {
            final String r = new String(fetch.readAllBytes());
            assertThat(r).isEqualTo("some file");
        }

        final ByteRange range = new ByteRange(1, data.length - 2);
        try (final InputStream fetch = storage.fetch(TOPIC_PARTITION_SEGMENT_KEY, range)) {
            final String r = new String(fetch.readAllBytes());
            assertThat(r).isEqualTo("ome fil");
        }

        storage.delete(TOPIC_PARTITION_SEGMENT_KEY);

        assertThatThrownBy(() -> storage.fetch(TOPIC_PARTITION_SEGMENT_KEY, new ByteRange(0, data.length)))
            .isInstanceOf(KeyNotFoundException.class)
            .hasMessage("Key key does not exists in storage " + storage);
    }

    @Test
    void testUploadANewFile() throws StorageBackendException {
        final StorageBackend storage = storage();
        final byte[] content = "content".getBytes();
        storage.upload(TOPIC_PARTITION_SEGMENT_KEY, content);
        assertThat(storage.fetch(TOPIC_PARTITION_SEGMENT_KEY, new ByteRange(0, content.length)))
            .hasBinaryContent(content);
    }

    @Test
    void testRetryUploadKeepLatestVersion() throws StorageBackendException {
        final StorageBackend storage = storage();
        final byte[] content1 = "content1".getBytes();
        final byte[] content2 = "content2".getBytes();
        storage.upload(TOPIC_PARTITION_SEGMENT_KEY, content1);
        storage.upload(TOPIC_PARTITION_SEGMENT_KEY, content2);

        assertThat(storage.fetch(TOPIC_PARTITION_SEGMENT_KEY, ByteRange.maxRange()))
            .hasBinaryContent(content2);
    }

    @Test
    void testFetchFailWhenNonExistingKey() {
        final StorageBackend storage = storage();
        assertThatThrownBy(() -> storage.fetch(new TestObjectKey("non-existing"), ByteRange.maxRange()))
            .isInstanceOf(KeyNotFoundException.class)
            .hasMessage("Key non-existing does not exists in storage " + storage);
    }

    @Test
    void testFetchWithOffsetRange() throws IOException, StorageBackendException {
        final StorageBackend storage = storage();
        final String content = "AABBBBAA";
        storage.upload(TOPIC_PARTITION_SEGMENT_KEY, content.getBytes());

        final int from = 2;
        final int to = 5;
        // Replacing end position as substring is end exclusive, and expected response is end inclusive
        final String range = content.substring(from, to + 1);

        try (final InputStream fetch = storage.fetch(TOPIC_PARTITION_SEGMENT_KEY, new ByteRange(2, 4))) {
            assertThat(fetch).hasContent(range);
        }
    }

    @Test
    void testFetchSingleByte() throws IOException, StorageBackendException {
        final StorageBackend storage = storage();
        final String content = "ABC";
        storage.upload(TOPIC_PARTITION_SEGMENT_KEY, content.getBytes());

        try (final InputStream fetch = storage.fetch(TOPIC_PARTITION_SEGMENT_KEY, new ByteRange(2, 1))) {
            assertThat(fetch).hasContent("C");
        }
    }

    @Test
    void testFetchWithOffsetRangeLargerThanFileSize() throws IOException, StorageBackendException {
        final StorageBackend storage = storage();
        final String content = "ABC";
        storage.upload(TOPIC_PARTITION_SEGMENT_KEY, content.getBytes());

        try (final InputStream fetch = storage.fetch(TOPIC_PARTITION_SEGMENT_KEY, new ByteRange(2, 10))) {
            assertThat(fetch).hasContent("C");
        }
    }

    @Test
    protected void testFetchWithRangeOutsideFileSize() throws StorageBackendException {
        final StorageBackend storage = storage();
        final String content = "ABC";
        storage.upload(TOPIC_PARTITION_SEGMENT_KEY, content.getBytes());

        assertThatThrownBy(() -> storage.fetch(TOPIC_PARTITION_SEGMENT_KEY, new ByteRange(3, 10)))
            .isInstanceOf(InvalidRangeException.class);
        assertThatThrownBy(() -> storage.fetch(TOPIC_PARTITION_SEGMENT_KEY, new ByteRange(4, 10)))
            .isInstanceOf(InvalidRangeException.class);
    }

    @Test
    void testFetchNonExistingKey() {
        final StorageBackend storage = storage();
        assertThatThrownBy(() -> storage.fetch(new TestObjectKey("non-existing"), ByteRange.maxRange()))
            .isInstanceOf(KeyNotFoundException.class)
            .hasMessage("Key non-existing does not exists in storage " + storage);
        assertThatThrownBy(() -> storage.fetch(new TestObjectKey("non-existing"), new ByteRange(0, 1)))
            .isInstanceOf(KeyNotFoundException.class)
            .hasMessage("Key non-existing does not exists in storage " + storage);
    }

    @Test
    void testFetchNonExistingKeyMasking() {
        final StorageBackend storage = storage();

        final ObjectKey key = new ObjectKey() {
            @Override
            public String value() {
                return "real-key";
            }

            @Override
            public String toString() {
                return "masked-key";
            }
        };

        assertThatThrownBy(() -> storage.fetch(key, ByteRange.maxRange()))
            .extracting(Throwables::getStackTrace)
            .asString()
            .contains("masked-key")
            .doesNotContain("real-key");
    }

    @Test
    protected void testDelete() throws StorageBackendException {
        final StorageBackend storage = storage();
        storage.upload(TOPIC_PARTITION_SEGMENT_KEY, "test".getBytes());
        storage.delete(TOPIC_PARTITION_SEGMENT_KEY);

        // Test deletion idempotence.
        assertThatNoException().isThrownBy(() -> storage.delete(TOPIC_PARTITION_SEGMENT_KEY));

        assertThatThrownBy(() -> storage.fetch(TOPIC_PARTITION_SEGMENT_KEY, ByteRange.maxRange()))
            .isInstanceOf(KeyNotFoundException.class)
            .hasMessage("Key key does not exists in storage " + storage);
    }

    @Test
    protected void testDeletes() throws StorageBackendException {
        final StorageBackend storage = storage();
        final Set<ObjectKey> keys = IntStream.range(0, 10)
            .mapToObj(i -> new TestObjectKey(TOPIC_PARTITION_SEGMENT_KEY.value() + i))
            .collect(Collectors.toSet());
        for (final var key : keys) {
            storage.upload(key, "test".getBytes());
        }
        storage.delete(keys);

        // Test deletion idempotence.
        assertThatNoException().isThrownBy(() -> storage.delete(keys));

        for (final var key : keys) {
            assertThatThrownBy(() -> storage.fetch(key, ByteRange.maxRange()))
                .isInstanceOf(KeyNotFoundException.class)
                .hasMessage("Key " + key.value() + " does not exists in storage " + storage);
        }
    }
}
