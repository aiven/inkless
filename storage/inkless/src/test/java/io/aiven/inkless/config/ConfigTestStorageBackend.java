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
package io.aiven.inkless.config;

import org.apache.kafka.common.metrics.Metrics;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.ReadableByteChannel;
import java.util.Map;
import java.util.Set;

import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.storage_backend.common.StorageBackend;
import io.aiven.inkless.storage_backend.common.StorageBackendException;

public class ConfigTestStorageBackend extends StorageBackend {
    public Map<String, ?> passedConfig = null;

    public ConfigTestStorageBackend() {
        this(new Metrics());
    }

    protected ConfigTestStorageBackend(final Metrics metrics) {
        super(metrics);
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        passedConfig = configs;
    }

    @Override
    public void delete(ObjectKey key) throws StorageBackendException {
    }

    @Override
    public void delete(Set<ObjectKey> keys) throws StorageBackendException {
    }

    @Override
    public ReadableByteChannel fetch(ObjectKey key, ByteRange range) throws StorageBackendException, IOException {
        return null;
    }

    @Override
    public void upload(ObjectKey key, InputStream data, long length) throws StorageBackendException {
    }

    @Override
    public void close() throws IOException {
    }
}
