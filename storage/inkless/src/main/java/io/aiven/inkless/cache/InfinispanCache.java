// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.cache;

import org.infinispan.Cache;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;

import java.util.concurrent.TimeUnit;

import io.aiven.inkless.generated.CacheKey;
import io.aiven.inkless.generated.FileExtent;

public class InfinispanCache implements ObjectCache, AutoCloseable {

    private final DefaultCacheManager cacheManager;
    private final Cache<CacheKey, FileExtent> cache;

    public InfinispanCache() {
        GlobalConfigurationBuilder globalConfig = GlobalConfigurationBuilder.defaultClusteredBuilder();
        globalConfig.serialization()
                .addContextInitializers()
                .marshaller(new KafkaMarshaller())
                .allowList().addClasses(CacheKey.class, FileExtent.class);
        cacheManager = new DefaultCacheManager(globalConfig.build());
        ConfigurationBuilder config = new ConfigurationBuilder();
        config.clustering().cacheMode(CacheMode.DIST_SYNC);
        cache = cacheManager.administration()
                .withFlags(CacheContainerAdmin.AdminFlag.VOLATILE)
                .getOrCreateCache("fileExtents", config.build());
    }

    @Override
    public FileExtent get(CacheKey key) {
        return cache.get(key);
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
    public void close() throws Exception {
        cacheManager.close();
    }
}
