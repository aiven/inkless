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
package io.aiven.inkless.storage_backend.common.fixtures;

import org.assertj.core.util.Throwables;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
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

        storage.upload(TOPIC_PARTITION_SEGMENT_KEY, new ByteArrayInputStream(data), data.length);

        final ByteBuffer fetch1 = storage.readToByteBuffer(
                storage.fetch(TOPIC_PARTITION_SEGMENT_KEY, new ByteRange(0, data.length)));
        final String r1 = new String(fetch1.array());
        assertThat(r1).isEqualTo("some file");

        final ByteRange range = new ByteRange(1, data.length - 2);
        final ByteBuffer fetch2 = storage.readToByteBuffer(
                storage.fetch(TOPIC_PARTITION_SEGMENT_KEY, range));
        final String r2 = new String(fetch2.array());
        assertThat(r2).isEqualTo("ome fil");

        storage.delete(TOPIC_PARTITION_SEGMENT_KEY);

        assertThatThrownBy(() -> storage.fetch(TOPIC_PARTITION_SEGMENT_KEY, new ByteRange(0, data.length)))
            .isInstanceOf(KeyNotFoundException.class)
            .hasMessage("Key key does not exists in storage " + storage);
    }

    @Test
    void uploadNulls() {
        final StorageBackend storage = storage();
        byte[] data = new byte[0];
        assertThatThrownBy(() -> storage.upload(null, new ByteArrayInputStream(data), data.length))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("key cannot be null");
        assertThatThrownBy(() -> storage.upload(TOPIC_PARTITION_SEGMENT_KEY, null, data.length))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("inputStream cannot be null");
        assertThatThrownBy(() -> storage.upload(TOPIC_PARTITION_SEGMENT_KEY, new ByteArrayInputStream(data), 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("length must be positive");
        assertThatThrownBy(() -> storage.upload(TOPIC_PARTITION_SEGMENT_KEY, new ByteArrayInputStream(data), -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("length must be positive");
    }

    @Test
    void testUploadANewFile() throws StorageBackendException, IOException {
        final StorageBackend storage = storage();
        final byte[] content = "content".getBytes();
        storage.upload(TOPIC_PARTITION_SEGMENT_KEY, new ByteArrayInputStream(content), content.length);
        final ByteBuffer fetch = storage.readToByteBuffer(storage.fetch(TOPIC_PARTITION_SEGMENT_KEY, new ByteRange(0, content.length)));
        assertThat(fetch.array()).isEqualTo(content);
    }

    @Test
    protected void testUploadUndersizedStream() {
        final StorageBackend storage = storage();
        final byte[] content = "content".getBytes();
        final long expectedLength = content.length + 1;

        assertThatThrownBy(() -> storage.upload(TOPIC_PARTITION_SEGMENT_KEY, new ByteArrayInputStream(content), expectedLength))
                .isInstanceOf(StorageBackendException.class)
                .hasMessage("Object key created with incorrect length " + content.length + " instead of " + expectedLength);
    }

    @Test
    protected void testUploadOversizeStream() {
        final StorageBackend storage = storage();
        final byte[] content = "content".getBytes();
        final long expectedLength = content.length - 1;

        assertThatThrownBy(() -> storage.upload(TOPIC_PARTITION_SEGMENT_KEY, new ByteArrayInputStream(content), expectedLength))
                .isInstanceOf(StorageBackendException.class)
                .hasMessage("Object key created with incorrect length " + content.length + " instead of " + expectedLength);
    }

    @Test
    void testRetryUploadKeepLatestVersion() throws StorageBackendException, IOException {
        final StorageBackend storage = storage();
        final byte[] content1 = "content1".getBytes();
        final byte[] content2 = "content2".getBytes();
        storage.upload(TOPIC_PARTITION_SEGMENT_KEY, new ByteArrayInputStream(content1), content1.length);
        storage.upload(TOPIC_PARTITION_SEGMENT_KEY, new ByteArrayInputStream(content2), content2.length);

        final ByteBuffer fetch = storage.readToByteBuffer(storage.fetch(TOPIC_PARTITION_SEGMENT_KEY, new ByteRange(0, content2.length)));
        assertThat(fetch.array()).isEqualTo(content2);
    }

    @Test
    void testFetchFailWhenNonExistingKey() {
        final StorageBackend storage = storage();
        assertThatThrownBy(() -> storage.fetch(new TestObjectKey("non-existing"), ByteRange.maxRange()))
            .isInstanceOf(KeyNotFoundException.class)
            .hasMessage("Key non-existing does not exists in storage " + storage);
    }

    @Test
    void testFetchWithoutRange() throws IOException, StorageBackendException {
        final StorageBackend storage = storage();
        final String content = "AABBBBAA";
        byte[] data = content.getBytes();
        storage.upload(TOPIC_PARTITION_SEGMENT_KEY, new ByteArrayInputStream(data), data.length);

        final ByteBuffer fetch = storage.readToByteBuffer(
                storage.fetch(TOPIC_PARTITION_SEGMENT_KEY, null));
        assertThat(new String(fetch.array())).isEqualTo(content);
    }

    @Test
    void testFetchWithOffsetRange() throws IOException, StorageBackendException {
        final StorageBackend storage = storage();
        final String content = "AABBBBAA";
        byte[] data = content.getBytes();
        storage.upload(TOPIC_PARTITION_SEGMENT_KEY, new ByteArrayInputStream(data), data.length);

        final int from = 2;
        final int to = 5;
        // Replacing end position as substring is end exclusive, and expected response is end inclusive
        final String range = content.substring(from, to + 1);

        final ByteBuffer fetch = storage.readToByteBuffer(
                storage.fetch(TOPIC_PARTITION_SEGMENT_KEY,  new ByteRange(2, 4)));
        assertThat(new String(fetch.array())).isEqualTo(range);
    }

    @Test
    void testFetchSingleByte() throws IOException, StorageBackendException {
        final StorageBackend storage = storage();
        final String content = "ABC";
        byte[] data = content.getBytes();
        storage.upload(TOPIC_PARTITION_SEGMENT_KEY, new ByteArrayInputStream(data), data.length);

        final ByteBuffer fetch = storage.readToByteBuffer(
                storage.fetch(TOPIC_PARTITION_SEGMENT_KEY, new ByteRange(2, 1)));
        assertThat(new String(fetch.array())).isEqualTo("C");
    }

    @Test
    void testFetchWithOffsetRangeLargerThanFileSize() throws IOException, StorageBackendException {
        final StorageBackend storage = storage();
        final String content = "ABC";
        byte[] data = content.getBytes();
        storage.upload(TOPIC_PARTITION_SEGMENT_KEY, new ByteArrayInputStream(data), data.length);

        final ByteBuffer fetch = storage.readToByteBuffer(
                storage.fetch(TOPIC_PARTITION_SEGMENT_KEY, new ByteRange(2, 10)));
        assertThat(new String(fetch.array())).isEqualTo("C");
    }

    @Test
    protected void testFetchWithRangeOutsideFileSize() throws StorageBackendException {
        final StorageBackend storage = storage();
        final String content = "ABC";
        byte[] data = content.getBytes();
        storage.upload(TOPIC_PARTITION_SEGMENT_KEY, new ByteArrayInputStream(data), data.length);

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
        byte[] data = "test".getBytes();
        storage.upload(TOPIC_PARTITION_SEGMENT_KEY, new ByteArrayInputStream(data), data.length);
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
            byte[] data = "test".getBytes();
            storage.upload(key, new ByteArrayInputStream(data), data.length);
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
