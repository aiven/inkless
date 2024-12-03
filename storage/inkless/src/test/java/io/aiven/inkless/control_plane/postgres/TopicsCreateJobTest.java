// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import io.aiven.inkless.common.UuidUtil;
import io.aiven.inkless.test_utils.SharedPostgreSQLTest;

import static org.assertj.core.api.Assertions.assertThat;

class TopicsCreateJobTest extends SharedPostgreSQLTest {
    static final String TOPIC_1 = "topic1";
    static final String TOPIC_2 = "topic2";
    static final Uuid TOPIC_ID1 = new Uuid(10, 12);
    static final Uuid TOPIC_ID2 = new Uuid(555, 333);

    @Test
    void empty() {
        final TopicsCreateJob job = new TopicsCreateJob(Time.SYSTEM, hikariDataSource, Map.of());
        job.run();
        assertThat(DBUtils.getAllLogs(hikariDataSource)).isEmpty();
    }

    @Test
    void createTopicsAndPartition() {
        final MetadataDelta delta = new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build();
        delta.replay(new TopicRecord().setName(TOPIC_1).setTopicId(TOPIC_ID1));
        delta.replay(new PartitionRecord().setTopicId(TOPIC_ID1).setPartitionId(0));
        delta.replay(new PartitionRecord().setTopicId(TOPIC_ID1).setPartitionId(1));
        delta.replay(new TopicRecord().setName(TOPIC_2).setTopicId(TOPIC_ID2));
        delta.replay(new PartitionRecord().setTopicId(TOPIC_ID2).setPartitionId(0));

        final TopicsCreateJob job1 = new TopicsCreateJob(Time.SYSTEM, hikariDataSource, delta.topicsDelta().changedTopics());
        job1.run();
        assertThat(DBUtils.getAllLogs(hikariDataSource)).containsExactlyInAnyOrder(
            new DBUtils.Log(TOPIC_ID1, 0, TOPIC_1, 0, 0),
            new DBUtils.Log(TOPIC_ID1, 1, TOPIC_1, 0, 0),
            new DBUtils.Log(TOPIC_ID2, 0, TOPIC_2, 0, 0)
        );

        // Repetition doesn't affect anything.
        final TopicsCreateJob job2 = new TopicsCreateJob(Time.SYSTEM, hikariDataSource, delta.topicsDelta().changedTopics());
        job2.run();
        assertThat(DBUtils.getAllLogs(hikariDataSource)).containsExactlyInAnyOrder(
                new DBUtils.Log(TOPIC_ID1, 0, TOPIC_1, 0, 0),
                new DBUtils.Log(TOPIC_ID1, 1, TOPIC_1, 0, 0),
                new DBUtils.Log(TOPIC_ID2, 0, TOPIC_2, 0, 0)
        );
    }

    @Test
    void createPartitionAfterTopic() {
        final MetadataDelta delta1 = new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build();
        delta1.replay(new TopicRecord().setName(TOPIC_1).setTopicId(TOPIC_ID1));
        delta1.replay(new PartitionRecord().setTopicId(TOPIC_ID1).setPartitionId(0));
        delta1.replay(new PartitionRecord().setTopicId(TOPIC_ID1).setPartitionId(1));
        delta1.replay(new TopicRecord().setName(TOPIC_2).setTopicId(TOPIC_ID2));
        delta1.replay(new PartitionRecord().setTopicId(TOPIC_ID2).setPartitionId(0));
        final MetadataImage image1 = delta1.apply(MetadataProvenance.EMPTY);

        final TopicsCreateJob job1 = new TopicsCreateJob(Time.SYSTEM, hikariDataSource, delta1.topicsDelta().changedTopics());
        job1.run();

        final MetadataDelta delta2 = new MetadataDelta.Builder().setImage(image1).build();
        delta2.replay(new PartitionRecord().setTopicId(TOPIC_ID2).setPartitionId(1));
        final TopicsCreateJob job2 = new TopicsCreateJob(Time.SYSTEM, hikariDataSource, delta2.topicsDelta().changedTopics());
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

        final MetadataDelta delta = new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build();
        delta.replay(new TopicRecord().setName(TOPIC_1).setTopicId(TOPIC_ID1));
        delta.replay(new PartitionRecord().setTopicId(TOPIC_ID1).setPartitionId(0));
        delta.replay(new PartitionRecord().setTopicId(TOPIC_ID1).setPartitionId(1));
        delta.replay(new TopicRecord().setName(TOPIC_2).setTopicId(TOPIC_ID2));
        delta.replay(new PartitionRecord().setTopicId(TOPIC_ID2).setPartitionId(0));

        final TopicsCreateJob job1 = new TopicsCreateJob(Time.SYSTEM, hikariDataSource, delta.topicsDelta().changedTopics());
        job1.run();

        assertThat(DBUtils.getAllLogs(hikariDataSource)).containsExactlyInAnyOrder(
                new DBUtils.Log(TOPIC_ID1, 0, TOPIC_1, 101, 201),  // unaffected
                new DBUtils.Log(TOPIC_ID1, 1, TOPIC_1, 0, 0),
                new DBUtils.Log(TOPIC_ID2, 0, TOPIC_2, 102, 202)  // unaffected
        );
    }
}
