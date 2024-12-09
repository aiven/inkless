// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.TopicIdPartition;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import io.aiven.inkless.common.UuidUtil;

class LogSelectQuery {
    private static final MessageFormat SELECT_LOGS_QUERY_TEMPLATE = new MessageFormat("""
        SELECT topic_id, partition, topic_name, log_start_offset, high_watermark
        FROM logs
        WHERE {0}
        {1}
        """);

    static List<LogEntity> execute(
        final Connection connection,
        final Collection<TopicIdPartition> topicIdAndPartitions,
        final boolean forUpdate
    ) throws SQLException {
        Objects.requireNonNull(connection, "connection cannot be null");
        Objects.requireNonNull(topicIdAndPartitions, "topicIdAndPartitions cannot be null");
        if (topicIdAndPartitions.isEmpty()) {
            throw new IllegalArgumentException("topicIdAndPartitions cannot be empty");
        }

        final String wherePlaceholders = topicIdAndPartitions.stream()
            .map(r -> "(topic_id = ? AND partition = ?)")
            .collect(Collectors.joining(" OR "));

        final String forUpdateStr = forUpdate ? "FOR UPDATE" : "";
        final String query = SELECT_LOGS_QUERY_TEMPLATE.format(new String[]{wherePlaceholders, forUpdateStr});

        final List<LogEntity> result = new ArrayList<>();
        try (final PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            int placeholderI = 1;
            for (final TopicIdPartition request : topicIdAndPartitions) {
                preparedStatement.setObject(placeholderI++, UuidUtil.toJava(request.topicId()));
                preparedStatement.setInt(placeholderI++, request.partition());
            }

            try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    result.add(LogEntity.fromResultSet(resultSet));
                }
            }
        }
        return result;
    }
}
