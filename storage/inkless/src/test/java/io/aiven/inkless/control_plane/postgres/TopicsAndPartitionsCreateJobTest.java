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
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.generated.tables.records.LogsRecord;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;

import io.aiven.inkless.control_plane.CreateTopicAndPartitionsRequest;
import io.aiven.inkless.test_utils.InklessPostgreSQLContainer;
import io.aiven.inkless.test_utils.PostgreSQLTestContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.jooq.generated.Tables.LOGS;

@Testcontainers
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class TopicsAndPartitionsCreateJobTest {
    @Container
    static final InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();
    
    static final String TOPIC_1 = "topic1";
    static final String TOPIC_2 = "topic2";
    static final Uuid TOPIC_ID1 = new Uuid(10, 12);
    static final Uuid TOPIC_ID2 = new Uuid(555, 333);

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
    void empty() {
        final TopicsAndPartitionsCreateJob job = new TopicsAndPartitionsCreateJob(Time.SYSTEM, pgContainer.getJooqCtx(), Set.of(), durationMs -> {});
        job.run();
        assertThat(DBUtils.getAllLogs(pgContainer.getDataSource())).isEmpty();
    }

    @Test
    void topicsWithoutPartition() {
        final Set<CreateTopicAndPartitionsRequest> createTopicAndPartitionsRequests = Set.of(
            new CreateTopicAndPartitionsRequest(TOPIC_ID1, TOPIC_1, 0),
            new CreateTopicAndPartitionsRequest(TOPIC_ID2, TOPIC_2, 0)
        );
        final TopicsAndPartitionsCreateJob job = new TopicsAndPartitionsCreateJob(Time.SYSTEM, pgContainer.getJooqCtx(), createTopicAndPartitionsRequests, durationMs -> {});
        job.run();
        assertThat(DBUtils.getAllLogs(pgContainer.getDataSource())).isEmpty();
    }

    @Test
    void createTopicsAndPartition() {
        final Set<CreateTopicAndPartitionsRequest> createTopicAndPartitionsRequests = Set.of(
            new CreateTopicAndPartitionsRequest(TOPIC_ID1, TOPIC_1, 2),
            new CreateTopicAndPartitionsRequest(TOPIC_ID2, TOPIC_2, 1)
        );
        final TopicsAndPartitionsCreateJob job1 = new TopicsAndPartitionsCreateJob(Time.SYSTEM, pgContainer.getJooqCtx(), createTopicAndPartitionsRequests, durationMs -> {});
        job1.run();
        assertThat(DBUtils.getAllLogs(pgContainer.getDataSource())).containsExactlyInAnyOrder(
            new LogsRecord(TOPIC_ID1, 0, TOPIC_1, 0L, 0L, 0L, 0L, null, 0),
            new LogsRecord(TOPIC_ID1, 1, TOPIC_1, 0L, 0L, 0L, 0L, null, 0),
            new LogsRecord(TOPIC_ID2, 0, TOPIC_2, 0L, 0L, 0L, 0L, null, 0)
        );

        // Repetition doesn't affect anything.
        final TopicsAndPartitionsCreateJob job2 = new TopicsAndPartitionsCreateJob(Time.SYSTEM, pgContainer.getJooqCtx(), createTopicAndPartitionsRequests, durationMs -> {});
        job2.run();
        assertThat(DBUtils.getAllLogs(pgContainer.getDataSource())).containsExactlyInAnyOrder(
                new LogsRecord(TOPIC_ID1, 0, TOPIC_1, 0L, 0L, 0L, 0L, null, 0),
                new LogsRecord(TOPIC_ID1, 1, TOPIC_1, 0L, 0L, 0L, 0L, null, 0),
                new LogsRecord(TOPIC_ID2, 0, TOPIC_2, 0L, 0L, 0L, 0L, null, 0)
        );
    }

    @Test
    void createPartitionAfterTopic() {
        final Set<CreateTopicAndPartitionsRequest> createTopicAndPartitionsRequests1 = Set.of(
            new CreateTopicAndPartitionsRequest(TOPIC_ID1, TOPIC_1, 2),
            new CreateTopicAndPartitionsRequest(TOPIC_ID2, TOPIC_2, 1)
        );
        final TopicsAndPartitionsCreateJob job1 = new TopicsAndPartitionsCreateJob(Time.SYSTEM, pgContainer.getJooqCtx(), createTopicAndPartitionsRequests1, durationMs -> {});
        job1.run();

        final Set<CreateTopicAndPartitionsRequest> createTopicAndPartitionsRequests2 = Set.of(
            new CreateTopicAndPartitionsRequest(TOPIC_ID1, TOPIC_1, 2),
            new CreateTopicAndPartitionsRequest(TOPIC_ID2, TOPIC_2, 2)
        );
        final TopicsAndPartitionsCreateJob job2 = new TopicsAndPartitionsCreateJob(Time.SYSTEM, pgContainer.getJooqCtx(), createTopicAndPartitionsRequests2, durationMs -> {});
        job2.run();

        assertThat(DBUtils.getAllLogs(pgContainer.getDataSource())).containsExactlyInAnyOrder(
            new LogsRecord(TOPIC_ID1, 0, TOPIC_1, 0L, 0L, 0L, 0L, null, 0),
            new LogsRecord(TOPIC_ID1, 1, TOPIC_1, 0L, 0L, 0L, 0L, null, 0),
            new LogsRecord(TOPIC_ID2, 0, TOPIC_2, 0L, 0L, 0L, 0L, null, 0),
            new LogsRecord(TOPIC_ID2, 1, TOPIC_2, 0L, 0L, 0L, 0L, null, 0)
        );
    }

    @Test
    void existingRecordsNotAffected() throws SQLException {
        try (final Connection connection = pgContainer.getDataSource().getConnection()) {
            final DSLContext ctx = DSL.using(connection, SQLDialect.POSTGRES);
            ctx.insertInto(LOGS,
                LOGS.TOPIC_ID, LOGS.PARTITION, LOGS.TOPIC_NAME, LOGS.LOG_START_OFFSET, LOGS.HIGH_WATERMARK, LOGS.BYTE_SIZE
            ).values(
                TOPIC_ID1, 0, TOPIC_1, 101L, 201L, 999L
            ).values(
                TOPIC_ID2, 0, TOPIC_2, 102L, 202L, 1999L
            ).execute();
            connection.commit();
        }

        final Set<CreateTopicAndPartitionsRequest> createTopicAndPartitionsRequests = Set.of(
            new CreateTopicAndPartitionsRequest(TOPIC_ID1, TOPIC_1, 2),
            new CreateTopicAndPartitionsRequest(TOPIC_ID2, TOPIC_2, 1)
        );
        final TopicsAndPartitionsCreateJob job1 = new TopicsAndPartitionsCreateJob(Time.SYSTEM, pgContainer.getJooqCtx(), createTopicAndPartitionsRequests, durationMs -> {});
        job1.run();

        assertThat(DBUtils.getAllLogs(pgContainer.getDataSource())).containsExactlyInAnyOrder(
                new LogsRecord(TOPIC_ID1, 0, TOPIC_1, 101L, 201L, 999L, 0L, null, 0),  // unaffected
                new LogsRecord(TOPIC_ID1, 1, TOPIC_1, 0L, 0L, 0L, 0L, null, 0),
                new LogsRecord(TOPIC_ID2, 0, TOPIC_2, 102L, 202L, 1999L, 0L, null, 0)  // unaffected
        );
    }
}
