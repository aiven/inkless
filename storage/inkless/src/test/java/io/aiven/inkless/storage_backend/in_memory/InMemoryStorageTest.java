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

import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.Set;

import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.PlainObjectKey;
import io.aiven.inkless.storage_backend.common.InvalidRangeException;
import io.aiven.inkless.storage_backend.common.KeyNotFoundException;
import io.aiven.inkless.storage_backend.common.StorageBackendException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InMemoryStorageTest {
    static final PlainObjectKey OBJECT_KEY = PlainObjectKey.create("a", "b");

    @Test
    void uploadNulls() {
        final InMemoryStorage storage = new InMemoryStorage();
        assertThatThrownBy(() -> storage.upload(null, new byte[0]))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("key cannot be null");
        assertThatThrownBy(() -> storage.upload(OBJECT_KEY, (byte[]) null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("data cannot be null");
    }

    @Test
    void fetchNulls() {
        final InMemoryStorage storage = new InMemoryStorage();
        assertThatThrownBy(() -> storage.fetch(null, new ByteRange(0, 10)))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("key cannot be null");
        assertThatThrownBy(() -> storage.fetch(OBJECT_KEY, null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("range cannot be null");
    }

    @Test
    void deleteNulls() {
        final InMemoryStorage storage = new InMemoryStorage();
        assertThatThrownBy(() -> storage.delete((ObjectKey) null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("key cannot be null");
        assertThatThrownBy(() -> storage.delete((Set<ObjectKey>) null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("keys cannot be null");
    }

    @Test
    void fetchNonExistent() {
        final InMemoryStorage storage = new InMemoryStorage();
        assertThatThrownBy(() -> storage.fetch(OBJECT_KEY, ByteRange.maxRange()))
            .isInstanceOf(KeyNotFoundException.class);
    }

    @Test
    void uploadAndFetch() throws StorageBackendException {
        final InMemoryStorage storage = new InMemoryStorage();
        final byte[] data = new byte[10];
        storage.upload(OBJECT_KEY, data);

        final InputStream fetch = storage.fetch(OBJECT_KEY, ByteRange.maxRange());

        assertThat(fetch).hasBinaryContent(data);
    }

    @Test
    void fetchRanged() throws StorageBackendException {
        final InMemoryStorage storage = new InMemoryStorage();
        final byte[] data = new byte[]{0, 1, 2, 3, 4, 5, 6, 7};
        storage.upload(OBJECT_KEY, data);

        final InputStream fetch1 = storage.fetch(OBJECT_KEY, new ByteRange(1, 2));
        assertThat(fetch1).hasBinaryContent(new byte[]{1, 2});

        final InputStream fetch2 = storage.fetch(OBJECT_KEY, new ByteRange(1, 100));
        assertThat(fetch2).hasBinaryContent(new byte[]{1, 2, 3, 4, 5, 6, 7});
    }

    @Test
    void fetchOutsideOfSize() throws StorageBackendException {
        final InMemoryStorage storage = new InMemoryStorage();
        final byte[] data = new byte[]{0, 1, 2, 3, 4, 5, 6, 7};
        storage.upload(OBJECT_KEY, data);

        assertThatThrownBy(() -> storage.fetch(OBJECT_KEY, new ByteRange(8, 1)))
            .isInstanceOf(InvalidRangeException.class)
            .hasMessage("Range start offset 8 is outside of data size 8");
    }

    @Test
    void delete() throws StorageBackendException {
        final InMemoryStorage storage = new InMemoryStorage();
        final byte[] data = new byte[]{0, 1, 2, 3, 4, 5, 6, 7};
        storage.upload(OBJECT_KEY, data);

        final InputStream fetch = storage.fetch(OBJECT_KEY, new ByteRange(1, 2));
        assertThat(fetch).hasBinaryContent(new byte[]{1, 2});

        storage.delete(OBJECT_KEY);

        assertThatThrownBy(() -> storage.fetch(OBJECT_KEY, ByteRange.maxRange()))
            .isInstanceOf(KeyNotFoundException.class);
    }

    @Test
    void deleteMany() throws StorageBackendException {
        final InMemoryStorage storage = new InMemoryStorage();
        final byte[] data = new byte[]{0, 1, 2, 3, 4, 5, 6, 7};
        storage.upload(OBJECT_KEY, data);

        final InputStream fetch = storage.fetch(OBJECT_KEY, new ByteRange(1, 2));
        assertThat(fetch).hasBinaryContent(new byte[]{1, 2});

        storage.delete(Set.of(OBJECT_KEY, PlainObjectKey.create("un", "related")));

        assertThatThrownBy(() -> storage.fetch(OBJECT_KEY, ByteRange.maxRange()))
            .isInstanceOf(KeyNotFoundException.class);
    }
}
