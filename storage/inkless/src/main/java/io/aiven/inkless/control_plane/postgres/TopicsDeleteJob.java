// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;

import com.zaxxer.hikari.HikariDataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.function.Consumer;

import io.aiven.inkless.control_plane.MetadataView;

class TopicsDeleteJob implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(TopicsDeleteJob.class);

    private final Time time;
    private final MetadataView metadataView;
    private final HikariDataSource hikariDataSource;
    private final Set<Uuid> deletedTopicIds;
    private final Consumer<Long> durationCallback;

    TopicsDeleteJob(final Time time,
                    final MetadataView metadataView,
                    final HikariDataSource hikariDataSource,
                    final Set<Uuid> deletedTopicIds,
                    final Consumer<Long> durationCallback) {
        this.time = time;
        this.metadataView = metadataView;
        this.hikariDataSource = hikariDataSource;
        this.deletedTopicIds = deletedTopicIds;
        this.durationCallback = durationCallback;
    }

    @Override
    public void run() {
        if (!deletedTopicIds.isEmpty()) {
            LOGGER.error("Topic deletion is not implemented yet. Trying to delete: {}", deletedTopicIds);
        }
    }
}
