// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;

import com.zaxxer.hikari.HikariDataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

class TopicsDeleteJob implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(TopicsDeleteJob.class);

    private final Time time;
    private final HikariDataSource hikariDataSource;
    private final Set<Uuid> deletedTopicIds;

    TopicsDeleteJob(final Time time,
                    final HikariDataSource hikariDataSource,
                    final Set<Uuid> deletedTopicIds) {
        this.time = time;
        this.hikariDataSource = hikariDataSource;
        this.deletedTopicIds = deletedTopicIds;
    }

    @Override
    public void run() {
        if (!deletedTopicIds.isEmpty()) {
            LOGGER.error("Topic deletion is not implemented yet. Trying to delete: {}", deletedTopicIds);
        }
    }
}
