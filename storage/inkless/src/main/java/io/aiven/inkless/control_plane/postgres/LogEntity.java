/*
 * Inkless
 * Copyright (C) 2025 Aiven OY
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
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

import io.aiven.inkless.common.UuidUtil;

record LogEntity(Uuid topicId,
                 int partition,
                 String topicName,
                 long logStartOffset,
                 long highWatermark) {
    static LogEntity fromResultSet(final ResultSet resultSet) throws SQLException {
        return new LogEntity(
            UuidUtil.fromJava(resultSet.getObject("topic_id", UUID.class)),
            resultSet.getInt("partition"),
            resultSet.getString("topic_name"),
            resultSet.getLong("log_start_offset"),
            resultSet.getLong("high_watermark")
        );
    }

    TopicIdPartition topicIdPartition() {
        return new TopicIdPartition(this.topicId(), this.partition(), this.topicName());
    }
}
