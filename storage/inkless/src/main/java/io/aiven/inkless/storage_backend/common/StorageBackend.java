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
package io.aiven.inkless.storage_backend.common;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.metrics.Metrics;

import java.io.Closeable;

public abstract class StorageBackend implements Configurable, ObjectUploader, ObjectFetcher, ObjectDeleter, Closeable {
    protected final Metrics metrics;

    // Metrics is created and passed from outside to allow sharing a single Metrics instance
    protected StorageBackend(Metrics metrics) {
        this.metrics = metrics;
    }
}
