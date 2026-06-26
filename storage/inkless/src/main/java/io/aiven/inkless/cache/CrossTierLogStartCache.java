/*
 * Inkless
 * Copyright (C) 2025 Aiven OY
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

import java.io.Closeable;

/**
 * A small, TTL-bounded cache of the cross-tier (remote) log start offset per partition, used to
 * answer {@code ListOffsets(EARLIEST)} for consolidating diskless topics without hitting the
 * control plane on every request.
 *
 * <p>The cached value is only ever advanced ({@link #put} is monotonic), matching the forward-only
 * nature of remote retention, so a stale entry can only ever be too low (the safe direction).
 */
public interface CrossTierLogStartCache extends Closeable {

    /**
     * @return the cached cross-tier log start offset for the partition, or {@code null} on a miss
     *         (absent or expired).
     */
    Long get(TopicIdPartition topicIdPartition);

    /**
     * Records a cross-tier log start offset for the partition. Only advances the stored value.
     */
    void put(TopicIdPartition topicIdPartition, long offset);
}
