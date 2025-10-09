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

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.ExponentialBackoff;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;

import org.infinispan.Cache;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.stats.Stats;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import io.aiven.inkless.generated.CacheKey;
import io.aiven.inkless.generated.FileExtent;

public class InfinispanCache implements ObjectCache {
    public static final String DIR_NAME = "inkless-cache";

    // Length of time the object is "leased" to the caller if not already present in the map
    private static final int CACHE_WRITE_LOCK_TIMEOUT_MS = 10000;
    private static final int CACHE_WRITE_BACKOFF_EXP_BASE = 2;
    private static final double CACHE_WRITE_BACKOFF_JITTER = 0.2;
    private final ExponentialBackoff backoff;
    private final Time time;
    private final DefaultCacheManager cacheManager;
    private final Cache<CacheKey, FileExtent> cache;
    private final long maxCacheSize;

    private InfinispanCacheMetrics metrics;

    public static InfinispanCache build(
        Time time,
        String clusterId,
        String rack,
        long maxCacheSize,
        Path basePath,
        boolean isPersistent,
        long lifespanSeconds,
        int maxIdleSeconds
    ) {
        final InfinispanCache infinispanCache = new InfinispanCache(time, clusterId, rack, maxCacheSize, basePath, isPersistent, lifespanSeconds, maxIdleSeconds);
        infinispanCache.initializeMetrics();
        return infinispanCache;
    }

    public InfinispanCache(
        Time time,
        String clusterId,
        String rack,
        long maxCacheSize,
        Path basePath,
        boolean isPersistent,
        long lifespanSeconds,
        int maxIdleSeconds
    ) {
        this.time = time;
        this.maxCacheSize = maxCacheSize;
        GlobalConfigurationBuilder globalConfig = GlobalConfigurationBuilder.defaultClusteredBuilder();
        final String clusterName = clusterName(clusterId, rack);
        globalConfig.transport()
            .clusterName(clusterName)
            .addProperty("configurationFile", "jgroups-udp.xml"); // Set bind port to 0
        globalConfig.serialization()
            .addContextInitializers()
            .marshaller(new KafkaMarshaller())
            .allowList().addClasses(CacheKey.class, FileExtent.class);
        this.cacheManager = new DefaultCacheManager(globalConfig.build());
        ConfigurationBuilder config = new ConfigurationBuilder();
        config.statistics().enable();
        config.clustering()
            .cacheMode(CacheMode.DIST_SYNC)
            .hash().numOwners(1);
        config.memory()
            .storage(StorageType.HEAP)
            .maxCount(maxCacheSize)
            .whenFull(EvictionStrategy.REMOVE);
        // There is no explicit way to define how much space the cache can use on disk,
        // there are only two proxies: lifespan (fixed time to keep an entry)
        // and maxIdle (how long to keep it without being accessed).
        // Lifespan is the only that can guarantee that the cache will not grow indefinitely.
        // To estimate the maximum disk usage use maximum buffer size and number of uploading threads.
        // e.g. 6MB buffer size * 10 threads = 60MB maximum disk usage per sec.
        // 5 minutes lifespan = 60MB * 300 seconds = 18GB maximum disk usage.
        // Lifespan is enforced, but maxIdle can be disabled (it is by default).
        config.expiration()
            // maximum time an entry can live in the cache (fixed)
            .lifespan(lifespanSeconds, TimeUnit.SECONDS)
            // maximum time an entry is idle (not accessed) before it is expired
            // entries expire based on either lifespan or maxIdle, whichever happens first
            // when disabled and only lifespan is used
            .maxIdle(maxIdleSeconds, TimeUnit.SECONDS)
            // how often the cache checks for expired entries
            .wakeUpInterval(5, TimeUnit.SECONDS);
        if (isPersistent) {
            // Prepare the cache directory within a known location. Index and data directories are created within this directory.
            final Path cacheBasePath = cachePersistenceDir(basePath);
            config.persistence()
                .passivation(true)
                .addSoftIndexFileStore()
                .shared(false)
                .purgeOnStartup(true)
                .dataLocation(cacheBasePath.resolve("data").toAbsolutePath().toString())
                .indexLocation(cacheBasePath.resolve("index").toAbsolutePath().toString());
        }
        this.cache = cacheManager.administration()
            .withFlags(CacheContainerAdmin.AdminFlag.VOLATILE)
            .getOrCreateCache("fileExtents", config.build());
        this.backoff = new ExponentialBackoff(1, CACHE_WRITE_BACKOFF_EXP_BASE, CACHE_WRITE_BACKOFF_EXP_BASE, CACHE_WRITE_BACKOFF_JITTER);
    }

    private void initializeMetrics() {
        this.metrics = new InfinispanCacheMetrics(this);
    }

    static String clusterName(String clusterId, String rack) {
        // To avoid cross-rack traffic, include rack in the cluster name
        // Clusters with different names don't share data or storage
        return "inkless-" + clusterId + (rack != null ? "-" + rack : "" );
    }

    @Override
    public FileExtent get(CacheKey key) {
        Timer timer = time.timer(CACHE_WRITE_LOCK_TIMEOUT_MS);
        int attempt = 0;
        do {
            FileExtent fileExtent = cache.putIfAbsent(key, new FileExtent(), CACHE_WRITE_LOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            if (fileExtent == null) {
                // It was not in the map, and so we just "locked" it with an empty result.
                // Proceed to perform the write operation
                break;
            } else if (fileExtent.data().length == 0) {
                // The entry in the map was an empty "lock" instance, so someone else is currently populating the mapping.
                // Poll the cache for the updated entry until it appears, or we run out of time
                time.sleep(backoff.backoff(attempt));
                timer.update();
                attempt++;
            } else {
                // The entry in the map was real, return it for use.
                return fileExtent;
            }
        } while (timer.notExpired());
        return null;
    }

    @Override
    public void put(CacheKey key, FileExtent value) {
        cache.put(key, value);
    }

    @Override
    public boolean remove(CacheKey key) {
        return cache.remove(key) != null;
    }

    @Override
    public long size() {
        return cache.size();
    }

    public long maxCacheSize() {
        return maxCacheSize;
    }

    @Override
    public void close() throws IOException {
        cache.clear();
        cacheManager.close();
        if (metrics != null) metrics.close();
    }

    public Stats metrics() {
        return cache.getAdvancedCache().getStats();
    }

    static Path cachePersistenceDir(Path baseDir) {
        final Path p = baseDir.resolve(DIR_NAME);
        if (!Files.exists(p)) {
            try {
                return Files.createDirectories(p);
            } catch (IOException e) {
                throw new ConfigException("Failed to create cache directories", e);
            }
        } else if (!Files.isDirectory(p)) {
            throw new ConfigException("Cache persistence directory is not a directory: " + p);
        } else if (!Files.isWritable(p)) {
            throw new ConfigException("Cache persistence directory is not writable: " + p);
        }
        return p;
    }

}
