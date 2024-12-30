// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.common;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

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
        BrokerTopicStats brokerTopicStats
) {

    public static SharedState initialize(
        Time time,
        int brokerId,
        InklessConfig config,
        MetadataView metadata,
        BrokerTopicStats brokerTopicStats
    ) {
        return new SharedState(
            time,
            brokerId,
            config,
            metadata,
            ControlPlane.create(config, time, metadata),
            config.storage(),
            PlainObjectKey.creator(config.objectKeyPrefix()),
            brokerTopicStats
        );
    }
}
