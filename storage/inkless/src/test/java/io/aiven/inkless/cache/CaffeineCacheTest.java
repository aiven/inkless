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

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import io.aiven.inkless.generated.CacheKey;
import io.aiven.inkless.generated.FileExtent;

import static org.assertj.core.api.Assertions.assertThat;

class CaffeineCacheTest {

    @Test
    void countBasedEvictionWhenBytesDisabled() throws Exception {
        // maxCacheBytes = 0 disables byte-based eviction, uses count-based (maxCacheSize = 2)
        try (CaffeineCache cache = new CaffeineCache(2, 0, 3600, 180)) {
            cache.put(key("a"), extent(10));
            cache.put(key("b"), extent(10));
            cache.cleanUp();
            assertThat(cache.size()).isEqualTo(2);

            cache.put(key("c"), extent(10));
            cache.cleanUp();
            assertThat(cache.size()).isLessThanOrEqualTo(2);
        }
    }

    @Test
    void bytesBasedEvictionWhenBytesConfigured() throws Exception {
        // maxCacheBytes = 100 bytes; each entry is 50 bytes => max ~2 entries
        try (CaffeineCache cache = new CaffeineCache(1000, 100, 3600, 180)) {
            cache.put(key("a"), extent(50));
            cache.put(key("b"), extent(50));
            cache.put(key("c"), extent(50));
            cache.cleanUp();

            // With 100 bytes max and 50 bytes per entry, at most 2 entries should remain
            assertThat(cache.size()).isLessThanOrEqualTo(2);
        }
    }

    @Test
    void negativeMaxIdleDisablesExpireAfterAccess() throws Exception {
        // -1 means "disabled" per InklessConfig docs: no expireAfterAccess policy should be set.
        try (CaffeineCache cache = new CaffeineCache(2, 0, 3600, -1)) {
            assertThat(cache.policy().expireAfterAccess()).isEmpty();
        }
    }

    @Test
    void positiveMaxIdleSetsExpireAfterAccess() throws Exception {
        try (CaffeineCache cache = new CaffeineCache(2, 0, 3600, 180)) {
            assertThat(cache.policy().expireAfterAccess())
                .hasValueSatisfying(p -> assertThat(p.getExpiresAfter()).isEqualTo(Duration.ofSeconds(180)));
        }
    }

    @Test
    void bytesBasedEvictionIgnoresCountLimit() throws Exception {
        // maxCacheSize = 1 but bytes takes precedence; 200 bytes fits 4x50-byte entries
        try (CaffeineCache cache = new CaffeineCache(1, 200, 3600, 180)) {
            cache.put(key("a"), extent(50));
            cache.put(key("b"), extent(50));
            cache.put(key("c"), extent(50));
            cache.cleanUp();

            // Count limit (1) is ignored because bytes limit is configured
            assertThat(cache.size()).isGreaterThan(1);
        }
    }

    /**
     * The hot path classifies a fetch as a cache hit from what {@code computeIfAbsent} reports, so the
     * miss signal must be visible to the caller as soon as the call returns. Signalling it from the load
     * body would not be: the load runs on the load executor and may not have started yet, or may already
     * have completed the future.
     */
    @Test
    void loadStartedCallbackRunsOnTheCallingThreadBeforeReturning() throws Exception {
        try (CaffeineCache cache = new CaffeineCache(10, 0, 3600, 180)) {
            final Queue<Runnable> pending = new ConcurrentLinkedQueue<>();
            final AtomicBoolean loadStarted = new AtomicBoolean(false);
            final List<Thread> callbackThread = new ArrayList<>();

            final var future = cache.computeIfAbsent(
                key("a"),
                k -> extent(10),
                pending::add,
                () -> {
                    callbackThread.add(Thread.currentThread());
                    loadStarted.set(true);
                }
            );

            // The load has not run at all yet, so only a callback invoked by computeIfAbsent itself can
            // have set the flag.
            assertThat(loadStarted).isTrue();
            assertThat(callbackThread).containsExactly(Thread.currentThread());
            assertThat(future).isNotDone();
            assertThat(pending).hasSize(1);

            pending.poll().run();
            assertThat(future.get()).isEqualTo(extent(10));
        }
    }

    /**
     * Coalescing onto an in-flight load must not signal a load start: the caller did not start a load, so
     * the hot path can tell it apart from an already-cached value by the future not being done.
     */
    @Test
    void loadStartedCallbackDoesNotRunForACoalescedOrCachedLookup() throws Exception {
        try (CaffeineCache cache = new CaffeineCache(10, 0, 3600, 180)) {
            final Queue<Runnable> pending = new ConcurrentLinkedQueue<>();
            final var first = cache.computeIfAbsent(key("a"), k -> extent(10), pending::add, () -> { });

            final AtomicBoolean coalescedLoadStarted = new AtomicBoolean(false);
            final var coalesced = cache.computeIfAbsent(
                key("a"), k -> extent(10), pending::add, () -> coalescedLoadStarted.set(true));

            assertThat(coalescedLoadStarted).isFalse();
            assertThat(coalesced).isSameAs(first).isNotDone();
            assertThat(pending).hasSize(1);

            pending.poll().run();
            assertThat(first.get()).isEqualTo(extent(10));

            final AtomicBoolean cachedLoadStarted = new AtomicBoolean(false);
            final var cached = cache.computeIfAbsent(
                key("a"), k -> extent(10), pending::add, () -> cachedLoadStarted.set(true));

            assertThat(cachedLoadStarted).isFalse();
            assertThat(cached).isDone();
            assertThat(pending).isEmpty();
        }
    }

    private static CacheKey key(String id) {
        return new CacheKey()
            .setObject(id)
            .setRange(new CacheKey.ByteRange().setOffset(0).setLength(1));
    }

    private static FileExtent extent(int dataSize) {
        return new FileExtent().setData(new byte[dataSize]);
    }
}
