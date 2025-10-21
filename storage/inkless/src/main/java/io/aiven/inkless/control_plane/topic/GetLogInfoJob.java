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

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import io.aiven.inkless.control_plane.GetLogInfoRequest;
import io.aiven.inkless.control_plane.GetLogInfoResponse;
import io.aiven.inkless.control_plane.postgres.JobUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class GetLogInfoJob implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(TopicsAndPartitionsCreateJob.class);

    private final Time time;
    private final Connection connection;
    private final PreparedStatement preparedStatement;

    GetLogInfoJob(final Time time,
                  final Connection connection) throws SQLException {
        this.time = Objects.requireNonNull(time, "time cannot be null");
        this.connection = Objects.requireNonNull(connection, "connection cannot be null");

        this.preparedStatement = connection.prepareStatement(
            "SELECT log_start_offset, high_watermark, byte_size " +
                "FROM logs " +
                "WHERE topic_id = ? AND partition = ?"
        );
    }

    public List<GetLogInfoResponse> call(final List<GetLogInfoRequest> requests,
                                         final Consumer<Long> durationCallback) {
        Objects.requireNonNull(requests, "requests cannot be null");
        Objects.requireNonNull(durationCallback, "durationCallback cannot be null");
        if (requests.isEmpty()) {
            return List.of();
        }
        return JobUtils.run(() -> runOnce(requests), time, durationCallback);
    }

    private List<GetLogInfoResponse> runOnce(final List<GetLogInfoRequest> requests) {
        final List<GetLogInfoResponse> results = new ArrayList<>();

        try {
            preparedStatement.clearParameters();
            for (final GetLogInfoRequest request : requests) {
                preparedStatement.setString(1, request.topicId().toString());
                preparedStatement.setInt(2, request.partition());
                try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                    if (resultSet.next()) {
                        results.add(new GetLogInfoResponse(
                            Errors.NONE,
                            resultSet.getLong("log_start_offset"),
                            resultSet.getLong("high_watermark"),
                            resultSet.getLong("byte_size")
                        ));
                    } else {
                        results.add(GetLogInfoResponse.unknownTopicOrPartition());
                    }
                    if (resultSet.next()) {
                        throw new RuntimeException();
                    }
                }
            }
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }

        return results;
    }

    @Override
    public void close() throws IOException {
        Utils.closeQuietly(preparedStatement, "preparedStatement");
    }
}
