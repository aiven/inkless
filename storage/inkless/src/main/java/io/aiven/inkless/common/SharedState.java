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
package io.aiven.inkless.common;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.function.Supplier;

import io.aiven.inkless.cache.CaffeineCache;
import io.aiven.inkless.cache.FixedBlockAlignment;
import io.aiven.inkless.cache.KeyAlignmentStrategy;
import io.aiven.inkless.cache.ObjectCache;
import io.aiven.inkless.config.InklessConfig;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.MetadataView;

public record SharedState(
    Time time,
    int brokerId,
    InklessConfig config,
    MetadataView metadata,
    ControlPlane controlPlane,
    ObjectKeyCreator objectKeyCreator,
    KeyAlignmentStrategy keyAlignmentStrategy,
    ObjectCache cache,
    BrokerTopicStats brokerTopicStats,
    Supplier<LogConfig> defaultTopicConfigs
) implements Closeable {

    public static SharedState initialize(
        Time time,
        String clusterId,
        String rack,
        int brokerId,
        InklessConfig config,
        MetadataView metadata,
        ControlPlane controlPlane,
        BrokerTopicStats brokerTopicStats,
        Path logDir,
        Supplier<LogConfig> defaultTopicConfigs
    ) {
        return new SharedState(
            time,
            brokerId,
            config,
            metadata,
            controlPlane,
            ObjectKey.creator(config.objectKeyPrefix(), config.objectKeyLogPrefixMasked()),
            new FixedBlockAlignment(config.fetchCacheBlockBytes()),
            new CaffeineCache(
                config.cacheMaxCount(),
                config.cacheExpirationLifespanSec(),
                config.cacheExpirationMaxIdleSec()
            ),
            /*
            InfinispanCache.build(
                time,
                clusterId,
                rack,
                config.cacheMaxCount(),
                logDir,
                config.isCachePersistenceEnabled(),
                config.cacheExpirationLifespanSec(),
                config.cacheExpirationMaxIdleSec()
            ),
             */
            brokerTopicStats,
            defaultTopicConfigs
        );
    }

    @Override
    public void close() throws IOException {
        try {
            cache.close();
            controlPlane.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
