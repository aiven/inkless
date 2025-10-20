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
package io.aiven.inkless.control_plane.topic;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.kafka.common.utils.Time;

import io.aiven.inkless.control_plane.CreateTopicAndPartitionsRequest;
import io.aiven.inkless.control_plane.postgres.JobUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TopicsAndPartitionsCreateJob {
    private static final Logger LOGGER = LoggerFactory.getLogger(TopicsAndPartitionsCreateJob.class);

    private final Time time;
    private final Connection connection;
    private final PreparedStatement preparedStatement;

    TopicsAndPartitionsCreateJob(final Time time,
                                 final Connection connection) throws SQLException {
        this.time = Objects.requireNonNull(time, "time cannot be null");
        this.connection = Objects.requireNonNull(connection, "connection cannot be null");

        this.preparedStatement = connection.prepareStatement(
            "INSERT INTO logs (topic_id, partition, topic_name, log_start_offset, high_watermark, byte_size) " +
                "VALUES (?, ?, ?, ?, ?, ?)"
        );
    }

    public void run(final Set<CreateTopicAndPartitionsRequest> requests, final Consumer<Long> durationCallback) {
        Objects.requireNonNull(requests, "requests cannot be null");
        Objects.requireNonNull(durationCallback, "durationCallback cannot be null");
        if (requests.isEmpty()) {
            return;
        }
        JobUtils.run(() -> this.runOnce(requests), time, durationCallback);
    }

    private void runOnce(final Set<CreateTopicAndPartitionsRequest> requests) {
        try {
            preparedStatement.clearParameters();
            for (final var request : requests) {
                for (int partition = 0; partition < request.numPartitions(); partition++) {
                    preparedStatement.setString(1, request.topicId().toString());
                    preparedStatement.setInt(2, partition);
                    preparedStatement.setString(3, request.topicName());
                    preparedStatement.setInt(4, 0);
                    preparedStatement.setInt(5, 0);
                    preparedStatement.setInt(6, 0);
                    preparedStatement.execute();
                }
            }
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
