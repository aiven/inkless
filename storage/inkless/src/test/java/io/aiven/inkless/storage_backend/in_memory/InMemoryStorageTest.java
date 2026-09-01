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
package io.aiven.inkless.storage_backend.in_memory;

import org.apache.kafka.common.metrics.Metrics;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.util.Set;

import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.PlainObjectKey;
import io.aiven.inkless.storage_backend.common.InvalidRangeException;
import io.aiven.inkless.storage_backend.common.StorageBackend;
import io.aiven.inkless.storage_backend.common.StorageBackendException;
import io.aiven.inkless.storage_backend.common.fixtures.BaseStorageTest;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InMemoryStorageTest extends BaseStorageTest {
    static final PlainObjectKey OBJECT_KEY = PlainObjectKey.create("a", "b");

    @Override
    protected StorageBackend storage() {
        return new InMemoryStorage(new Metrics());
    }

    @Test
    @Override
    protected void testFetchWithoutRange() throws Exception {
        try (StorageBackend storage = storage()) {
            final byte[] data = "AABBBBAA".getBytes();
            storage.upload(TOPIC_PARTITION_SEGMENT_KEY, new ByteArrayInputStream(data), data.length);

            assertThatThrownBy(() -> storage.fetch(TOPIC_PARTITION_SEGMENT_KEY, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("range cannot be null");
        }
    }

    @Test
    void fetchNulls() {
        final Metrics metrics = new Metrics();
        final InMemoryStorage storage = new InMemoryStorage(metrics);
        assertThatThrownBy(() -> storage.fetch(null, new ByteRange(0, 10)))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("key cannot be null");
    }

    @Test
    void deleteNulls() {
        final Metrics metrics = new Metrics();
        final InMemoryStorage storage = new InMemoryStorage(metrics);
        assertThatThrownBy(() -> storage.delete((ObjectKey) null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("key cannot be null");
        assertThatThrownBy(() -> storage.delete((Set<ObjectKey>) null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("keys cannot be null");
    }

    /**
     * The shared fixture asserts the exception type; this pins the message the backend produces.
     */
    @Test
    void fetchOutsideOfSize() throws StorageBackendException {
        final Metrics metrics = new Metrics();
        final InMemoryStorage storage = new InMemoryStorage(metrics);
        final byte[] data = new byte[]{0, 1, 2, 3, 4, 5, 6, 7};
        storage.upload(OBJECT_KEY, new ByteArrayInputStream(data), data.length);

        assertThatThrownBy(() -> storage.fetch(OBJECT_KEY, new ByteRange(8, 1)))
            .isInstanceOf(InvalidRangeException.class)
            .hasMessage("Failed to fetch a/b: Invalid range ByteRange[offset=8, size=1] for blob size 8");
    }
}
