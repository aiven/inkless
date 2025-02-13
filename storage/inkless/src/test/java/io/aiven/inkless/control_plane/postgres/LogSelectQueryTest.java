// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.util.List;
import java.util.function.Consumer;

import io.aiven.inkless.test_utils.InklessPostgreSQLContainer;
import io.aiven.inkless.test_utils.PostgreSQLTestContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.jooq.generated.Tables.LOGS;
import static org.mockito.Mockito.mock;

@Testcontainers
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class LogSelectQueryTest {
    @Container
    static final InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();
    
    @Mock
    Consumer<Long> durationCallback;

    static final TopicIdPartition T0P0 = new TopicIdPartition(Uuid.ZERO_UUID, 0, "t0");
    static final TopicIdPartition T0P1 = new TopicIdPartition(Uuid.ZERO_UUID, 1, "t0");
    static final TopicIdPartition T1P0 = new TopicIdPartition(Uuid.ONE_UUID, 0, "t1");

    @BeforeEach
    void setUp(final TestInfo testInfo) {
        pgContainer.createDatabase(testInfo);
        pgContainer.migrate();
    }

    @AfterEach
    void tearDown() {
        pgContainer.tearDown();
    }

    @Test
    void testNulls() {
        final Time time = new MockTime();
        assertThatThrownBy(() -> LogSelectQuery.execute(time, null, List.of(T0P0), false, durationMs -> {
        }))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("jooqCtx cannot be null");
        assertThatThrownBy(() -> LogSelectQuery.execute(time, mock(DSLContext.class), null, false, durationMs -> {
        }))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("topicIdAndPartitions cannot be null");
    }

    @Test
    void testEmpty() {
        final Time time = new MockTime();
        assertThatThrownBy(() -> LogSelectQuery.execute(time, mock(DSLContext.class), List.of(), false, durationMs -> {
        }))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("topicIdAndPartitions cannot be empty");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testSingleTopicPartition(final boolean forUpdate) throws Exception {
        final Time time = new MockTime();
        final long logStartOffset = 1L;
        final long highWatermark = 2L;

        try (final Connection connection = pgContainer.getDataSource().getConnection()) {
            final DSLContext ctx = DSL.using(connection, SQLDialect.POSTGRES);
            ctx.insertInto(LOGS,
                LOGS.TOPIC_ID, LOGS.PARTITION, LOGS.TOPIC_NAME, LOGS.LOG_START_OFFSET, LOGS.HIGH_WATERMARK
            ).values(
                T0P1.topicId(), T0P1.partition(), T0P1.topic(), logStartOffset, highWatermark
            ).execute();
            connection.commit();
        }

        final List<LogEntity> result = LogSelectQuery.execute(
            time, pgContainer.getJooqCtx().dsl(), List.of(T0P1), forUpdate, durationCallback);
        assertThat(result).containsExactlyInAnyOrder(
            new LogEntity(T0P1.topicId(), T0P1.partition(), T0P1.topic(), logStartOffset, highWatermark));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testMultipleTopicPartitions(final boolean forUpdate) throws Exception {
        final Time time = new MockTime();
        final long logStartOffset1 = 1L;
        final long highWatermark1 = 2L;
        final long logStartOffset2 = 10L;
        final long highWatermark2 = 20L;

        try (final Connection connection = pgContainer.getDataSource().getConnection()) {
            final DSLContext ctx = DSL.using(connection, SQLDialect.POSTGRES);
            ctx.insertInto(LOGS,
                LOGS.TOPIC_ID, LOGS.PARTITION, LOGS.TOPIC_NAME, LOGS.LOG_START_OFFSET, LOGS.HIGH_WATERMARK
            ).values(
                T0P1.topicId(), T0P1.partition(), T0P1.topic(), logStartOffset1, highWatermark1
            ).values(
                T1P0.topicId(), T1P0.partition(), T1P0.topic(), logStartOffset2, highWatermark2
            ).execute();
            connection.commit();
        }

        final List<LogEntity> result = LogSelectQuery.execute(time, pgContainer.getJooqCtx().dsl(), List.of(T0P1, T1P0), forUpdate, durationCallback);
        assertThat(result).containsExactlyInAnyOrder(
            new LogEntity(T0P1.topicId(), T0P1.partition(), T0P1.topic(), logStartOffset1, highWatermark1),
            new LogEntity(T1P0.topicId(), T1P0.partition(), T1P0.topic(), logStartOffset2, highWatermark2)
        );
    }
}
