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

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import io.aiven.inkless.generated.CacheKey;
import io.aiven.inkless.generated.FileExtent;

public class InfinispanCache implements ObjectCache {

    // Length of time the object is "leased" to the caller if not already present in the map
    private static final int CACHE_WRITE_LOCK_TIMEOUT_MS = 10000;
    private static final int CACHE_WRITE_BACKOFF_EXP_BASE = 2;
    private static final double CACHE_WRITE_BACKOFF_JITTER = 0.2;
    private final ExponentialBackoff backoff;
    private final Time time;
    private final DefaultCacheManager cacheManager;
    private final Cache<CacheKey, FileExtent> cache;

    public InfinispanCache(Time time, String clusterId, String rack, long cacheSize) {
        this.time = time;
        GlobalConfigurationBuilder globalConfig = GlobalConfigurationBuilder.defaultClusteredBuilder();
        globalConfig.transport()
            .clusterName(clusterName(clusterId, rack))
            .addProperty("configurationFile", "jgroups-udp.xml"); // Set bind port to 0
        globalConfig.serialization()
                .addContextInitializers()
                .marshaller(new KafkaMarshaller())
                .allowList().addClasses(CacheKey.class, FileExtent.class);
        cacheManager = new DefaultCacheManager(globalConfig.build());
        ConfigurationBuilder config = new ConfigurationBuilder();
        config.statistics().enable();
        config.clustering()
            .cacheMode(CacheMode.DIST_SYNC)
            .memory()
            .storage(StorageType.HEAP)
            .maxCount(cacheSize)
            .whenFull(EvictionStrategy.REMOVE);
        cache = cacheManager.administration()
                .withFlags(CacheContainerAdmin.AdminFlag.VOLATILE)
                .getOrCreateCache("fileExtents", config.build());
        backoff = new ExponentialBackoff(1, CACHE_WRITE_BACKOFF_EXP_BASE, CACHE_WRITE_BACKOFF_EXP_BASE, CACHE_WRITE_BACKOFF_JITTER);

        new InfinispanCacheMetrics(this, cacheSize);
    }

    private static String clusterName(String clusterId, String rack) {
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
        cache.put(key, value, 1L, TimeUnit.MINUTES);
    }

    @Override
    public boolean remove(CacheKey key) {
        return cache.remove(key) != null;
    }

    @Override
    public long size() {
        return cache.size();
    }

    @Override
    public void close() throws IOException {
        cache.clear();
        cacheManager.close();
    }

    public Stats metrics() {
        return cache.getAdvancedCache().getStats();
    }
}
