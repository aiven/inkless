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
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.MockTime;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CaffeineCrossTierLogStartCacheTest {

    private static final TopicIdPartition TIDP_0 = new TopicIdPartition(Uuid.randomUuid(), 0, "topic");
    private static final TopicIdPartition TIDP_1 = new TopicIdPartition(Uuid.randomUuid(), 1, "topic");

    @Test
    void rejectsNonPositiveTtl() {
        assertThatThrownBy(() -> new CaffeineCrossTierLogStartCache(Duration.ZERO))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CaffeineCrossTierLogStartCache(Duration.ofMillis(-1)))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void missReturnsNull() throws IOException {
        try (final CaffeineCrossTierLogStartCache cache = new CaffeineCrossTierLogStartCache(Duration.ofSeconds(10))) {
            assertThat(cache.get(TIDP_0)).isNull();
        }
    }

    @Test
    void putThenGet() throws IOException {
        try (final CaffeineCrossTierLogStartCache cache = new CaffeineCrossTierLogStartCache(Duration.ofSeconds(10))) {
            cache.put(TIDP_0, 42L);
            assertThat(cache.get(TIDP_0)).isEqualTo(42L);
            // Independent keys do not interfere.
            assertThat(cache.get(TIDP_1)).isNull();
        }
    }

    @Test
    void putIsMonotonic() throws IOException {
        try (final CaffeineCrossTierLogStartCache cache = new CaffeineCrossTierLogStartCache(Duration.ofSeconds(10))) {
            cache.put(TIDP_0, 50L);
            // A lower value never replaces a higher one.
            cache.put(TIDP_0, 30L);
            assertThat(cache.get(TIDP_0)).isEqualTo(50L);
            // A higher value advances the entry.
            cache.put(TIDP_0, 70L);
            assertThat(cache.get(TIDP_0)).isEqualTo(70L);
        }
    }

    @Test
    void negativeOffsetIsIgnored() throws IOException {
        try (final CaffeineCrossTierLogStartCache cache = new CaffeineCrossTierLogStartCache(Duration.ofSeconds(10))) {
            cache.put(TIDP_0, -1L);
            assertThat(cache.get(TIDP_0)).isNull();
        }
    }

    @Test
    void entriesExpireAfterTtl() throws IOException {
        final MockTime time = new MockTime();
        try (final CaffeineCrossTierLogStartCache cache =
                 new CaffeineCrossTierLogStartCache(Duration.ofSeconds(10), time::nanoseconds)) {
            cache.put(TIDP_0, 42L);
            assertThat(cache.get(TIDP_0)).isEqualTo(42L);

            // Still valid just before the TTL elapses.
            time.sleep(TimeUnit.SECONDS.toMillis(9));
            assertThat(cache.get(TIDP_0)).isEqualTo(42L);

            // Expired once the TTL (counted from the last write) elapses.
            time.sleep(TimeUnit.SECONDS.toMillis(2));
            assertThat(cache.get(TIDP_0)).isNull();
        }
    }
}
