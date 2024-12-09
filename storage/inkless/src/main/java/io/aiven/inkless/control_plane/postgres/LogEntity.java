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
