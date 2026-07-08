/*
 * Inkless
 * Copyright (C) 2024 - 2026 Aiven OY
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
 * A {@link CrossTierLogStartCache} that always misses; used when the cache is disabled.
 */
public class NullCrossTierLogStartCache implements CrossTierLogStartCache {

    @Override
    public Long get(final TopicIdPartition topicIdPartition) {
        return null;
    }

    @Override
    public void put(final TopicIdPartition topicIdPartition, final long offset) {}

    @Override
    public void close() throws IOException {}
}
