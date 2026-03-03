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
package io.aiven.inkless.produce;

import org.apache.kafka.storage.internals.log.LogConfig;

import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import io.aiven.inkless.control_plane.MetadataView;

/**
 * Cache for {@link LogConfig} objects to avoid expensive re-creation on every produce request.
 *
 * <p>The cache is invalidated when topic configurations change. Since topic config changes are rare
 * (typically only during topic creation or admin operations), this cache provides significant CPU
 * savings by avoiding repeated parsing and validation in {@link LogConfig#fromProps}.
 *
 * <p>The cache uses a simple invalidation strategy based on comparing the hash of the topic's
 * override properties. When the overrides change, the cached entry is evicted and recreated.
 *
 * <p>Thread-safe: this class can be used from multiple threads concurrently.
 */
public class LogConfigCache {

    private final MetadataView metadataView;
    private final Supplier<LogConfig> defaultConfigSupplier;
    private final ConcurrentHashMap<String, CachedEntry> cache = new ConcurrentHashMap<>();

    // Cache the default config originals to avoid repeated calls
    private volatile CachedDefaults cachedDefaults;

    public LogConfigCache(final MetadataView metadataView, final Supplier<LogConfig> defaultConfigSupplier) {
        this.metadataView = Objects.requireNonNull(metadataView, "metadataView cannot be null");
        this.defaultConfigSupplier = Objects.requireNonNull(defaultConfigSupplier, "defaultConfigSupplier cannot be null");
    }

    /**
     * Get a LogConfig for the given topic, using cached version if available and valid.
     *
     * @param topic the topic name
     * @return the LogConfig for the topic
     */
    public LogConfig get(final String topic) {
        final Properties currentOverrides = metadataView.getTopicConfig(topic);
        // Handle null overrides (no custom config for topic)
        final Properties effectiveOverrides = currentOverrides != null ? currentOverrides : new Properties();
        final int currentOverridesHash = computePropertiesHash(effectiveOverrides);

        final CachedEntry existing = cache.get(topic);
        if (existing != null && existing.overridesHash == currentOverridesHash) {
            return existing.logConfig;
        }

        // Cache miss or stale entry - create new LogConfig
        final Map<String, Object> defaults = getDefaultOriginals();
        final LogConfig newConfig = LogConfig.fromProps(defaults, effectiveOverrides);

        cache.put(topic, new CachedEntry(newConfig, currentOverridesHash));
        return newConfig;
    }

    /**
     * Get LogConfigs for multiple topics.
     *
     * @param topics the set of topic names
     * @return a map of topic name to LogConfig
     */
    public Map<String, LogConfig> getAll(final Set<String> topics) {
        final Map<String, LogConfig> result = new ConcurrentHashMap<>();
        for (final String topic : topics) {
            result.put(topic, get(topic));
        }
        return result;
    }

    /**
     * Explicitly invalidate the cache entry for a topic.
     * This can be called when a topic config change is detected.
     *
     * @param topic the topic to invalidate
     */
    public void invalidate(final String topic) {
        cache.remove(topic);
    }

    /**
     * Clear all cached entries.
     */
    public void invalidateAll() {
        cache.clear();
        cachedDefaults = null;
    }

    /**
     * Get the current cache size (for metrics/monitoring).
     *
     * @return the number of cached entries
     */
    public int size() {
        return cache.size();
    }

    private Map<String, Object> getDefaultOriginals() {
        // Double-checked locking pattern for caching default originals
        CachedDefaults current = cachedDefaults;
        if (current != null) {
            // Check if the default config supplier returns the same LogConfig instance
            // If the supplier returns a new instance, we need to refresh
            final LogConfig currentDefault = defaultConfigSupplier.get();
            if (current.defaultConfigIdentity == System.identityHashCode(currentDefault)) {
                return current.originals;
            }
        }

        synchronized (this) {
            current = cachedDefaults;
            final LogConfig currentDefault = defaultConfigSupplier.get();
            if (current != null && current.defaultConfigIdentity == System.identityHashCode(currentDefault)) {
                return current.originals;
            }

            final Map<String, Object> originals = currentDefault.originals();
            cachedDefaults = new CachedDefaults(originals, System.identityHashCode(currentDefault));
            return originals;
        }
    }

    /**
     * Compute a hash of Properties that is stable across JVM runs.
     * We use the hash of the entries to detect changes.
     */
    private static int computePropertiesHash(final Properties props) {
        if (props == null || props.isEmpty()) {
            return 0;
        }
        // Use entrySet hashCode which is stable
        return props.entrySet().hashCode();
    }

    private static final class CachedEntry {
        final LogConfig logConfig;
        final int overridesHash;

        CachedEntry(final LogConfig logConfig, final int overridesHash) {
            this.logConfig = logConfig;
            this.overridesHash = overridesHash;
        }
    }

    private static final class CachedDefaults {
        final Map<String, Object> originals;
        final int defaultConfigIdentity;

        CachedDefaults(final Map<String, Object> originals, final int defaultConfigIdentity) {
            this.originals = originals;
            this.defaultConfigIdentity = defaultConfigIdentity;
        }
    }
}
