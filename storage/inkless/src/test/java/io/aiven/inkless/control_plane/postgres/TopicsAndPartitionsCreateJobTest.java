// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.generated.tables.records.LogsRecord;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;

import io.aiven.inkless.control_plane.CreateTopicAndPartitionsRequest;
import io.aiven.inkless.test_utils.SharedPostgreSQLTest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.jooq.generated.Tables.LOGS;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class TopicsAndPartitionsCreateJobTest extends SharedPostgreSQLTest {
    static final String TOPIC_1 = "topic1";
    static final String TOPIC_2 = "topic2";
    static final Uuid TOPIC_ID1 = new Uuid(10, 12);
    static final Uuid TOPIC_ID2 = new Uuid(555, 333);

    @Test
    void empty() {
        final TopicsAndPartitionsCreateJob job = new TopicsAndPartitionsCreateJob(Time.SYSTEM, jooqCtx, Set.of(), durationMs -> {});
        job.run();
        assertThat(DBUtils.getAllLogs(hikariDataSource)).isEmpty();
    }

    @Test
    void topicsWithoutPartition() {
        final Set<CreateTopicAndPartitionsRequest> createTopicAndPartitionsRequests = Set.of(
            new CreateTopicAndPartitionsRequest(TOPIC_ID1, TOPIC_1, 0),
            new CreateTopicAndPartitionsRequest(TOPIC_ID2, TOPIC_2, 0)
        );
        final TopicsAndPartitionsCreateJob job = new TopicsAndPartitionsCreateJob(Time.SYSTEM, jooqCtx, createTopicAndPartitionsRequests, durationMs -> {});
        job.run();
        assertThat(DBUtils.getAllLogs(hikariDataSource)).isEmpty();
    }

    @Test
    void createTopicsAndPartition() {
        final Set<CreateTopicAndPartitionsRequest> createTopicAndPartitionsRequests = Set.of(
            new CreateTopicAndPartitionsRequest(TOPIC_ID1, TOPIC_1, 2),
            new CreateTopicAndPartitionsRequest(TOPIC_ID2, TOPIC_2, 1)
        );
        final TopicsAndPartitionsCreateJob job1 = new TopicsAndPartitionsCreateJob(Time.SYSTEM, jooqCtx, createTopicAndPartitionsRequests, durationMs -> {});
        job1.run();
        assertThat(DBUtils.getAllLogs(hikariDataSource)).containsExactlyInAnyOrder(
            new LogsRecord(TOPIC_ID1, 0, TOPIC_1, 0L, 0L),
            new LogsRecord(TOPIC_ID1, 1, TOPIC_1, 0L, 0L),
            new LogsRecord(TOPIC_ID2, 0, TOPIC_2, 0L, 0L)
        );

        // Repetition doesn't affect anything.
        final TopicsAndPartitionsCreateJob job2 = new TopicsAndPartitionsCreateJob(Time.SYSTEM, jooqCtx, createTopicAndPartitionsRequests, durationMs -> {});
        job2.run();
        assertThat(DBUtils.getAllLogs(hikariDataSource)).containsExactlyInAnyOrder(
                new LogsRecord(TOPIC_ID1, 0, TOPIC_1, 0L, 0L),
                new LogsRecord(TOPIC_ID1, 1, TOPIC_1, 0L, 0L),
                new LogsRecord(TOPIC_ID2, 0, TOPIC_2, 0L, 0L)
        );
    }

    @Test
    void createPartitionAfterTopic() {
        final Set<CreateTopicAndPartitionsRequest> createTopicAndPartitionsRequests1 = Set.of(
            new CreateTopicAndPartitionsRequest(TOPIC_ID1, TOPIC_1, 2),
            new CreateTopicAndPartitionsRequest(TOPIC_ID2, TOPIC_2, 1)
        );
        final TopicsAndPartitionsCreateJob job1 = new TopicsAndPartitionsCreateJob(Time.SYSTEM, jooqCtx, createTopicAndPartitionsRequests1, durationMs -> {});
        job1.run();

        final Set<CreateTopicAndPartitionsRequest> createTopicAndPartitionsRequests2 = Set.of(
            new CreateTopicAndPartitionsRequest(TOPIC_ID1, TOPIC_1, 2),
            new CreateTopicAndPartitionsRequest(TOPIC_ID2, TOPIC_2, 2)
        );
        final TopicsAndPartitionsCreateJob job2 = new TopicsAndPartitionsCreateJob(Time.SYSTEM, jooqCtx, createTopicAndPartitionsRequests2, durationMs -> {});
        job2.run();

        assertThat(DBUtils.getAllLogs(hikariDataSource)).containsExactlyInAnyOrder(
            new LogsRecord(TOPIC_ID1, 0, TOPIC_1, 0L, 0L),
            new LogsRecord(TOPIC_ID1, 1, TOPIC_1, 0L, 0L),
            new LogsRecord(TOPIC_ID2, 0, TOPIC_2, 0L, 0L),
            new LogsRecord(TOPIC_ID2, 1, TOPIC_2, 0L, 0L)
        );
    }

    @Test
    void existingRecordsNotAffected() throws SQLException {
        try (final Connection connection = hikariDataSource.getConnection()) {
            final DSLContext ctx = DSL.using(connection, SQLDialect.POSTGRES);
            ctx.insertInto(LOGS,
                LOGS.TOPIC_ID, LOGS.PARTITION, LOGS.TOPIC_NAME, LOGS.LOG_START_OFFSET, LOGS.HIGH_WATERMARK
            ).values(
                TOPIC_ID1, 0, TOPIC_1, 101L, 201L
            ).values(
                TOPIC_ID2, 0, TOPIC_2, 102L, 202L
            ).execute();
            connection.commit();
        }

        final Set<CreateTopicAndPartitionsRequest> createTopicAndPartitionsRequests = Set.of(
            new CreateTopicAndPartitionsRequest(TOPIC_ID1, TOPIC_1, 2),
            new CreateTopicAndPartitionsRequest(TOPIC_ID2, TOPIC_2, 1)
        );
        final TopicsAndPartitionsCreateJob job1 = new TopicsAndPartitionsCreateJob(Time.SYSTEM, jooqCtx, createTopicAndPartitionsRequests, durationMs -> {});
        job1.run();

        assertThat(DBUtils.getAllLogs(hikariDataSource)).containsExactlyInAnyOrder(
                new LogsRecord(TOPIC_ID1, 0, TOPIC_1, 101L, 201L),  // unaffected
                new LogsRecord(TOPIC_ID1, 1, TOPIC_1, 0L, 0L),
                new LogsRecord(TOPIC_ID2, 0, TOPIC_2, 102L, 202L)  // unaffected
        );
    }
}
