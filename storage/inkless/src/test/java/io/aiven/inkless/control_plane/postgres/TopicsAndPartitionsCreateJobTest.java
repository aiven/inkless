// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Set;

import io.aiven.inkless.common.UuidUtil;
import io.aiven.inkless.control_plane.CreateTopicAndPartitionsRequest;
import io.aiven.inkless.test_utils.SharedPostgreSQLTest;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class TopicsAndPartitionsCreateJobTest extends SharedPostgreSQLTest {
    static final String TOPIC_1 = "topic1";
    static final String TOPIC_2 = "topic2";
    static final Uuid TOPIC_ID1 = new Uuid(10, 12);
    static final Uuid TOPIC_ID2 = new Uuid(555, 333);

    @Test
    void empty() {
        final TopicsAndPartitionsCreateJob job = new TopicsAndPartitionsCreateJob(Time.SYSTEM, hikariDataSource, Set.of(), durationMs -> {});
        job.run();
        assertThat(DBUtils.getAllLogs(hikariDataSource)).isEmpty();
    }

    @Test
    void topicsWithoutPartition() {
        final Set<CreateTopicAndPartitionsRequest> createTopicAndPartitionsRequests = Set.of(
            new CreateTopicAndPartitionsRequest(TOPIC_ID1, TOPIC_1, 0),
            new CreateTopicAndPartitionsRequest(TOPIC_ID2, TOPIC_2, 0)
        );
        final TopicsAndPartitionsCreateJob job = new TopicsAndPartitionsCreateJob(Time.SYSTEM, hikariDataSource, createTopicAndPartitionsRequests, durationMs -> {});
        job.run();
        assertThat(DBUtils.getAllLogs(hikariDataSource)).isEmpty();
    }

    @Test
    void createTopicsAndPartition() {
        final Set<CreateTopicAndPartitionsRequest> createTopicAndPartitionsRequests = Set.of(
            new CreateTopicAndPartitionsRequest(TOPIC_ID1, TOPIC_1, 2),
            new CreateTopicAndPartitionsRequest(TOPIC_ID2, TOPIC_2, 1)
        );
        final TopicsAndPartitionsCreateJob job1 = new TopicsAndPartitionsCreateJob(Time.SYSTEM, hikariDataSource, createTopicAndPartitionsRequests, durationMs -> {});
        job1.run();
        assertThat(DBUtils.getAllLogs(hikariDataSource)).containsExactlyInAnyOrder(
            new DBUtils.Log(TOPIC_ID1, 0, TOPIC_1, 0, 0),
            new DBUtils.Log(TOPIC_ID1, 1, TOPIC_1, 0, 0),
            new DBUtils.Log(TOPIC_ID2, 0, TOPIC_2, 0, 0)
        );

        // Repetition doesn't affect anything.
        final TopicsAndPartitionsCreateJob job2 = new TopicsAndPartitionsCreateJob(Time.SYSTEM, hikariDataSource, createTopicAndPartitionsRequests, durationMs -> {});
        job2.run();
        assertThat(DBUtils.getAllLogs(hikariDataSource)).containsExactlyInAnyOrder(
                new DBUtils.Log(TOPIC_ID1, 0, TOPIC_1, 0, 0),
                new DBUtils.Log(TOPIC_ID1, 1, TOPIC_1, 0, 0),
                new DBUtils.Log(TOPIC_ID2, 0, TOPIC_2, 0, 0)
        );
    }

    @Test
    void createPartitionAfterTopic() {
        final Set<CreateTopicAndPartitionsRequest> createTopicAndPartitionsRequests1 = Set.of(
            new CreateTopicAndPartitionsRequest(TOPIC_ID1, TOPIC_1, 2),
            new CreateTopicAndPartitionsRequest(TOPIC_ID2, TOPIC_2, 1)
        );
        final TopicsAndPartitionsCreateJob job1 = new TopicsAndPartitionsCreateJob(Time.SYSTEM, hikariDataSource, createTopicAndPartitionsRequests1, durationMs -> {});
        job1.run();

        final Set<CreateTopicAndPartitionsRequest> createTopicAndPartitionsRequests2 = Set.of(
            new CreateTopicAndPartitionsRequest(TOPIC_ID1, TOPIC_1, 2),
            new CreateTopicAndPartitionsRequest(TOPIC_ID2, TOPIC_2, 2)
        );
        final TopicsAndPartitionsCreateJob job2 = new TopicsAndPartitionsCreateJob(Time.SYSTEM, hikariDataSource, createTopicAndPartitionsRequests2, durationMs -> {});
        job2.run();

        assertThat(DBUtils.getAllLogs(hikariDataSource)).containsExactlyInAnyOrder(
            new DBUtils.Log(TOPIC_ID1, 0, TOPIC_1, 0, 0),
            new DBUtils.Log(TOPIC_ID1, 1, TOPIC_1, 0, 0),
            new DBUtils.Log(TOPIC_ID2, 0, TOPIC_2, 0, 0),
            new DBUtils.Log(TOPIC_ID2, 1, TOPIC_2, 0, 0)
        );
    }

    @Test
    void existingRecordsNotAffected() throws SQLException {
        final String insert = """
            INSERT INTO logs (topic_id, partition, topic_name, log_start_offset, high_watermark)
            VALUES (?, ?, ?, ?, ?);
            """;
        try (final Connection connection = hikariDataSource.getConnection();
             final PreparedStatement statement = connection.prepareStatement(insert)) {
            statement.setObject(1, UuidUtil.toJava(TOPIC_ID1));
            statement.setInt(2, 0);
            statement.setString(3, TOPIC_1);
            statement.setLong(4, 101);
            statement.setLong(5, 201);
            statement.addBatch();

            statement.setObject(1, UuidUtil.toJava(TOPIC_ID2));
            statement.setInt(2, 0);
            statement.setString(3, TOPIC_2);
            statement.setLong(4, 102);
            statement.setLong(5, 202);
            statement.addBatch();

            statement.executeBatch();
            connection.commit();
        }

        final Set<CreateTopicAndPartitionsRequest> createTopicAndPartitionsRequests = Set.of(
            new CreateTopicAndPartitionsRequest(TOPIC_ID1, TOPIC_1, 2),
            new CreateTopicAndPartitionsRequest(TOPIC_ID2, TOPIC_2, 1)
        );
        final TopicsAndPartitionsCreateJob job1 = new TopicsAndPartitionsCreateJob(Time.SYSTEM, hikariDataSource, createTopicAndPartitionsRequests, durationMs -> {});
        job1.run();

        assertThat(DBUtils.getAllLogs(hikariDataSource)).containsExactlyInAnyOrder(
                new DBUtils.Log(TOPIC_ID1, 0, TOPIC_1, 101, 201),  // unaffected
                new DBUtils.Log(TOPIC_ID1, 1, TOPIC_1, 0, 0),
                new DBUtils.Log(TOPIC_ID2, 0, TOPIC_2, 102, 202)  // unaffected
        );
    }
}
