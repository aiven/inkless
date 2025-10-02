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
package kafka.cluster;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.storage.internals.log.UnifiedLog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DisklessPartition implements Closeable {
    private final Logger LOGGER = LoggerFactory.getLogger(DisklessPartition.class);

    private final ReentrantLock lock = new ReentrantLock();
    private final TopicIdPartition topicIdPartition;
    private final UnifiedLog log;
    private final Connection dbConnection;

    DisklessPartition(final TopicIdPartition topicIdPartition, final UnifiedLog log) throws SQLException {
        this.topicIdPartition = Objects.requireNonNull(topicIdPartition, "topicIdPartition cannot be null");
        this.log = Objects.requireNonNull(log, "log cannot be null");

        final var dbUrl = "jdbc:sqlite:" + log.dir().toPath().resolve("state.db");
        this.dbConnection = java.sql.DriverManager.getConnection(dbUrl);

        // TODO use
    }

    // TODO delete

    @Override
    public void close() {
        Utils.closeQuietly(
            dbConnection,
            String.format("SQLite connection in %s", topicIdPartition.topicPartition()),
            LOGGER
        );
    }
}
