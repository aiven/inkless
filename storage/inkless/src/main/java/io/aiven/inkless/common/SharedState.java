// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.common;

import org.apache.kafka.common.utils.Time;

import io.aiven.inkless.config.InklessConfig;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.MetadataView;
import io.aiven.inkless.storage_backend.common.StorageBackend;

public record SharedState(
        Time time,
        InklessConfig config,
        MetadataView metadata,
        ControlPlane controlPlane,
        StorageBackend storage
) {

    public static SharedState initialize(Time time, InklessConfig config, MetadataView metadata) {
        return new SharedState(time, config, metadata, ControlPlane.create(config, metadata), config.storage());
    }

}
