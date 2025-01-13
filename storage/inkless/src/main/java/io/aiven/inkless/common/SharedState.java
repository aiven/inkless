// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.common;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import java.util.function.Supplier;

import io.aiven.inkless.cache.FixedBlockAlignment;
import io.aiven.inkless.cache.InfinispanCache;
import io.aiven.inkless.cache.KeyAlignmentStrategy;
import io.aiven.inkless.cache.ObjectCache;
import io.aiven.inkless.config.InklessConfig;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.MetadataView;
import io.aiven.inkless.storage_backend.common.StorageBackend;

public record SharedState(
        Time time,
        int brokerId,
        InklessConfig config,
        MetadataView metadata,
        ControlPlane controlPlane,
        StorageBackend storage,
        ObjectKeyCreator objectKeyCreator,
        KeyAlignmentStrategy keyAlignmentStrategy,
        ObjectCache cache,
        BrokerTopicStats brokerTopicStats,
        Supplier<LogConfig> defaultTopicConfigs
) {

    public static SharedState initialize(
        Time time,
        int brokerId,
        InklessConfig config,
        MetadataView metadata,
        ControlPlane controlPlane,
        BrokerTopicStats brokerTopicStats,
        Supplier<LogConfig> defaultTopicConfigs
    ) {
        return new SharedState(
            time,
            brokerId,
            config,
            metadata,
            controlPlane,
            config.storage(),
            ObjectKey.create(config.objectKeyPrefix(), config.objectKeyLogPrefixMasked()),
            new FixedBlockAlignment(config.fetchCacheBlockBytes()),
            new InfinispanCache(time),
            brokerTopicStats,
            defaultTopicConfigs
        );
    }
}
