// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import io.aiven.inkless.common.UuidUtil;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class LogSelectQueryTest {
    @Mock
    Connection connection;
    @Mock
    PreparedStatement preparedStatement;
    @Mock
    ResultSet resultSet;

    @Captor
    ArgumentCaptor<String> queryCaptor;

    static final TopicIdPartition T0P0 = new TopicIdPartition(Uuid.ZERO_UUID, 0, "t0");
    static final TopicIdPartition T0P1 = new TopicIdPartition(Uuid.ZERO_UUID, 1, "t0");
    static final TopicIdPartition T1P0 = new TopicIdPartition(Uuid.ONE_UUID, 0, "t1");
    static final TopicIdPartition T1P1 = new TopicIdPartition(Uuid.ONE_UUID, 1, "t1");

    @Test
    void testNulls() {
        assertThatThrownBy(() -> LogSelectQuery.execute(null, List.of(T0P0), false))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("connection cannot be null");
        assertThatThrownBy(() -> LogSelectQuery.execute(connection, null, false))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("topicIdAndPartitions cannot be null");
    }

    @Test
    void testEmpty() {
        assertThatThrownBy(() -> LogSelectQuery.execute(connection, List.of(), false))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("topicIdAndPartitions cannot be empty");
    }

    @ParameterizedTest
    @MethodSource("testSingleTopicPartitionProvider")
    void testSingleTopicPartition(final boolean forUpdate, final String expectedQuery) throws SQLException {
        final long logStartOffset = 1L;
        final long highWatermark = 2L;

        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);

        when(resultSet.next()).thenReturn(true, false);

        when(resultSet.getObject(eq("topic_id"), eq(UUID.class)))
            .thenReturn(UuidUtil.toJava(T0P1.topicId()));
        when(resultSet.getInt(eq("partition")))
            .thenReturn(T0P1.partition());
        when(resultSet.getString(eq("topic_name")))
            .thenReturn(T0P1.topic());
        when(resultSet.getLong(eq("log_start_offset")))
            .thenReturn(logStartOffset);
        when(resultSet.getLong(eq("high_watermark")))
            .thenReturn(highWatermark);

        final List<LogEntity> result = LogSelectQuery.execute(connection, List.of(T0P1), forUpdate);
        assertThat(result).containsExactlyInAnyOrder(
            new LogEntity(T0P1.topicId(), T0P1.partition(), T0P1.topic(), logStartOffset, highWatermark));

        verify(connection).prepareStatement(queryCaptor.capture());
        assertThat(queryCaptor.getValue().trim()).isEqualTo(expectedQuery.trim());

        verify(preparedStatement).setObject(eq(1), eq(UuidUtil.toJava(T0P1.topicId())));
        verify(preparedStatement).setInt(eq(2), eq(T0P1.partition()));
    }

    static Stream<Arguments> testSingleTopicPartitionProvider() {
        final String queryNotForUpdate = """
        SELECT topic_id, partition, topic_name, log_start_offset, high_watermark
        FROM logs
        WHERE (topic_id = ? AND partition = ?)
        """;
        final String queryForUpdate = """
        SELECT topic_id, partition, topic_name, log_start_offset, high_watermark
        FROM logs
        WHERE (topic_id = ? AND partition = ?)
        FOR UPDATE
        """;
        return Stream.of(
            Arguments.of(false, queryNotForUpdate),
            Arguments.of(true, queryForUpdate)
        );
    }

    @ParameterizedTest
    @MethodSource("testMultipleTopicPartitionsProvider")
    void testMultipleTopicPartitions(final boolean forUpdate, final String expectedQuery) throws SQLException {
        final long logStartOffset1 = 1L;
        final long highWatermark1 = 2L;
        final long logStartOffset2 = 10L;
        final long highWatermark2 = 20L;

        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);

        when(resultSet.next()).thenReturn(true, true, false);

        when(resultSet.getObject(eq("topic_id"), eq(UUID.class)))
            .thenReturn(UuidUtil.toJava(T0P1.topicId()), UuidUtil.toJava(T1P0.topicId()));
        when(resultSet.getInt(eq("partition")))
            .thenReturn(T0P1.partition(), T1P0.partition());
        when(resultSet.getString(eq("topic_name")))
            .thenReturn(T0P1.topic(), T1P0.topic());
        when(resultSet.getLong(eq("log_start_offset")))
            .thenReturn(logStartOffset1, logStartOffset2);
        when(resultSet.getLong(eq("high_watermark")))
            .thenReturn(highWatermark1, highWatermark2);

        final List<LogEntity> result = LogSelectQuery.execute(connection, List.of(T0P1, T1P0), forUpdate);
        assertThat(result).containsExactlyInAnyOrder(
            new LogEntity(T0P1.topicId(), T0P1.partition(), T0P1.topic(), logStartOffset1, highWatermark1),
            new LogEntity(T1P0.topicId(), T1P0.partition(), T1P0.topic(), logStartOffset2, highWatermark2)
        );

        verify(connection).prepareStatement(queryCaptor.capture());
        assertThat(queryCaptor.getValue().trim()).isEqualTo(expectedQuery.trim());

        verify(preparedStatement).setObject(eq(1), eq(UuidUtil.toJava(T0P1.topicId())));
        verify(preparedStatement).setInt(eq(2), eq(T0P1.partition()));
        verify(preparedStatement).setObject(eq(3), eq(UuidUtil.toJava(T1P0.topicId())));
        verify(preparedStatement).setInt(eq(4), eq(T1P0.partition()));
    }

    static Stream<Arguments> testMultipleTopicPartitionsProvider() {
        final String queryNotForUpdate = """
        SELECT topic_id, partition, topic_name, log_start_offset, high_watermark
        FROM logs
        WHERE (topic_id = ? AND partition = ?) OR (topic_id = ? AND partition = ?)
        """;
        final String queryForUpdate = """
        SELECT topic_id, partition, topic_name, log_start_offset, high_watermark
        FROM logs
        WHERE (topic_id = ? AND partition = ?) OR (topic_id = ? AND partition = ?)
        FOR UPDATE
        """;
        return Stream.of(
            Arguments.of(false, queryNotForUpdate),
            Arguments.of(true, queryForUpdate)
        );
    }
}
