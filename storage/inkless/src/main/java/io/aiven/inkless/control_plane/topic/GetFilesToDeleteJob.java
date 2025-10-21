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
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.control_plane.FileState;
import io.aiven.inkless.control_plane.FileToDelete;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class GetFilesToDeleteJob implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(GetFilesToDeleteJob.class);

    private final Time time;
    private final Connection connection;
    private final PreparedStatement preparedStatement;

    GetFilesToDeleteJob(final Time time, final Connection connection) throws SQLException {
        this.time = Objects.requireNonNull(time, "time cannot be null");
        this.connection = Objects.requireNonNull(connection, "connection cannot be null");

        this.preparedStatement = connection.prepareStatement(
            "SELECT object_key, marked_for_deletion_at " +
                "FROM files " +
                "WHERE state = ?"
        );
    }


    public List<FileToDelete> call(final Consumer<Long> durationCallback) {
        Objects.requireNonNull(durationCallback, "durationCallback cannot be null");

        return TimeUtils.measureDurationMsSupplier(time, this::runOnce, durationCallback);
    }

    private List<FileToDelete> runOnce() {
        try {
            this.preparedStatement.clearParameters();
            this.preparedStatement.setString(1, FileState.DELETING.toString());
            final List<FileToDelete> result = new ArrayList<>();
            try (final ResultSet resultSet = this.preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    result.add(new FileToDelete(
                        resultSet.getString("object_key"),
                        Instant.ofEpochMilli(resultSet.getLong("marked_for_deletion_at"))
                    ));
                }
            }
            return result;
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        Utils.closeQuietly(preparedStatement, "preparedStatement");
    }
}
