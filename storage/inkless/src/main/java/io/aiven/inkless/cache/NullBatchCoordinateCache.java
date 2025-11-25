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
package io.aiven.inkless.cache;

import org.apache.kafka.common.TopicIdPartition;

import java.io.IOException;

/**
 * A Batch Coordinate cache implementation that always misses.
 */
public class NullBatchCoordinateCache implements BatchCoordinateCache {

    @Override
    public LogFragment get(TopicIdPartition topicIdPartition, long offset) {
        return null;
    }

    @Override
    public void put(TopicIdPartition topicIdPartition, CacheBatchCoordinate cacheBatchCoordinate) {}

    @Override
    public void close() throws IOException {}
}
