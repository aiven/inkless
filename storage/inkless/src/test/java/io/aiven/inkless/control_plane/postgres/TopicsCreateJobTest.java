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
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import io.aiven.inkless.common.UuidUtil;
import io.aiven.inkless.control_plane.MetadataView;
import io.aiven.inkless.test_utils.SharedPostgreSQLTest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class TopicsCreateJobTest extends SharedPostgreSQLTest {
    static final String TOPIC_1 = "topic1";
    static final String TOPIC_2 = "topic2";
    static final Uuid TOPIC_ID1 = new Uuid(10, 12);
    static final Uuid TOPIC_ID2 = new Uuid(555, 333);

    @Mock
    MetadataView metadataView;

    @Test
    void empty() {
        final TopicsCreateJob job = new TopicsCreateJob(Time.SYSTEM, metadataView, hikariDataSource, Map.of(), durationMs -> {});
        job.run();
        assertThat(DBUtils.getAllLogs(hikariDataSource)).isEmpty();
    }

    @Test
    void topicsWithoutPartition() {
        when(metadataView.isInklessTopic(anyString())).thenReturn(true);

        final MetadataDelta delta = new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build();
        delta.replay(new TopicRecord().setName(TOPIC_1).setTopicId(TOPIC_ID1));
        delta.replay(new TopicRecord().setName(TOPIC_2).setTopicId(TOPIC_ID2));

        final TopicsCreateJob job = new TopicsCreateJob(Time.SYSTEM, metadataView, hikariDataSource, delta.topicsDelta().changedTopics(), durationMs -> {});
        job.run();
        assertThat(DBUtils.getAllLogs(hikariDataSource)).isEmpty();
    }

    @Test
    void createTopicsAndPartition() {
        when(metadataView.isInklessTopic(anyString())).thenReturn(true);

        final MetadataDelta delta = new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build();
        delta.replay(new TopicRecord().setName(TOPIC_1).setTopicId(TOPIC_ID1));
        delta.replay(new PartitionRecord().setTopicId(TOPIC_ID1).setPartitionId(0));
        delta.replay(new PartitionRecord().setTopicId(TOPIC_ID1).setPartitionId(1));
        delta.replay(new TopicRecord().setName(TOPIC_2).setTopicId(TOPIC_ID2));
        delta.replay(new PartitionRecord().setTopicId(TOPIC_ID2).setPartitionId(0));

        final TopicsCreateJob job1 = new TopicsCreateJob(Time.SYSTEM, metadataView, hikariDataSource, delta.topicsDelta().changedTopics(), durationMs -> {});
        job1.run();
        assertThat(DBUtils.getAllLogs(hikariDataSource)).containsExactlyInAnyOrder(
            new DBUtils.Log(TOPIC_ID1, 0, TOPIC_1, 0, 0),
            new DBUtils.Log(TOPIC_ID1, 1, TOPIC_1, 0, 0),
            new DBUtils.Log(TOPIC_ID2, 0, TOPIC_2, 0, 0)
        );

        // Repetition doesn't affect anything.
        final TopicsCreateJob job2 = new TopicsCreateJob(Time.SYSTEM, metadataView, hikariDataSource, delta.topicsDelta().changedTopics(), durationMs -> {});
        job2.run();
        assertThat(DBUtils.getAllLogs(hikariDataSource)).containsExactlyInAnyOrder(
                new DBUtils.Log(TOPIC_ID1, 0, TOPIC_1, 0, 0),
                new DBUtils.Log(TOPIC_ID1, 1, TOPIC_1, 0, 0),
                new DBUtils.Log(TOPIC_ID2, 0, TOPIC_2, 0, 0)
        );
    }

    @Test
    void createPartitionAfterTopic() {
        when(metadataView.isInklessTopic(anyString())).thenReturn(true);

        final MetadataDelta delta1 = new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build();
        delta1.replay(new TopicRecord().setName(TOPIC_1).setTopicId(TOPIC_ID1));
        delta1.replay(new PartitionRecord().setTopicId(TOPIC_ID1).setPartitionId(0));
        delta1.replay(new PartitionRecord().setTopicId(TOPIC_ID1).setPartitionId(1));
        delta1.replay(new TopicRecord().setName(TOPIC_2).setTopicId(TOPIC_ID2));
        delta1.replay(new PartitionRecord().setTopicId(TOPIC_ID2).setPartitionId(0));
        final MetadataImage image1 = delta1.apply(MetadataProvenance.EMPTY);

        final TopicsCreateJob job1 = new TopicsCreateJob(Time.SYSTEM, metadataView, hikariDataSource, delta1.topicsDelta().changedTopics(), durationMs -> {});
        job1.run();

        final MetadataDelta delta2 = new MetadataDelta.Builder().setImage(image1).build();
        delta2.replay(new PartitionRecord().setTopicId(TOPIC_ID2).setPartitionId(1));
        final TopicsCreateJob job2 = new TopicsCreateJob(Time.SYSTEM, metadataView, hikariDataSource, delta2.topicsDelta().changedTopics(), durationMs -> {});
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
        when(metadataView.isInklessTopic(anyString())).thenReturn(true);

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

        final TopicsCreateJob job1 = new TopicsCreateJob(Time.SYSTEM, metadataView, hikariDataSource, delta.topicsDelta().changedTopics(), durationMs -> {});
        job1.run();

        assertThat(DBUtils.getAllLogs(hikariDataSource)).containsExactlyInAnyOrder(
                new DBUtils.Log(TOPIC_ID1, 0, TOPIC_1, 101, 201),  // unaffected
                new DBUtils.Log(TOPIC_ID1, 1, TOPIC_1, 0, 0),
                new DBUtils.Log(TOPIC_ID2, 0, TOPIC_2, 102, 202)  // unaffected
        );
    }

    @Test
    void ignoreNonInklessTopics() {
        when(metadataView.isInklessTopic(eq(TOPIC_1))).thenReturn(false);
        when(metadataView.isInklessTopic(eq(TOPIC_2))).thenReturn(true);

        final MetadataDelta delta = new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build();
        delta.replay(new TopicRecord().setName(TOPIC_1).setTopicId(TOPIC_ID1));
        delta.replay(new PartitionRecord().setTopicId(TOPIC_ID1).setPartitionId(0));
        delta.replay(new PartitionRecord().setTopicId(TOPIC_ID1).setPartitionId(1));
        delta.replay(new TopicRecord().setName(TOPIC_2).setTopicId(TOPIC_ID2));
        delta.replay(new PartitionRecord().setTopicId(TOPIC_ID2).setPartitionId(0));

        final TopicsCreateJob job1 = new TopicsCreateJob(Time.SYSTEM, metadataView, hikariDataSource, delta.topicsDelta().changedTopics(), durationMs -> {});
        job1.run();
        assertThat(DBUtils.getAllLogs(hikariDataSource)).containsExactlyInAnyOrder(
            new DBUtils.Log(TOPIC_ID2, 0, TOPIC_2, 0, 0)
        );
    }
}
