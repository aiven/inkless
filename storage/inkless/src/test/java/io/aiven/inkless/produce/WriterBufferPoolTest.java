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
package io.aiven.inkless.produce;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.common.RequestLocal;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import io.aiven.inkless.produce.buffer.BufferPool;
import io.aiven.inkless.produce.buffer.ElasticBufferPool;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for BufferPool integration in Writer.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class WriterBufferPoolTest {

    private static final Uuid TOPIC_ID = new Uuid(1000, 1000);
    private static final String TOPIC = "test-topic";
    private static final TopicIdPartition T0P0 = new TopicIdPartition(TOPIC_ID, 0, TOPIC);

    private static final Map<String, LogConfig> TOPIC_CONFIGS = Map.of(
        TOPIC, new LogConfig(Map.of(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, TimestampType.CREATE_TIME.name))
    );
    private static final RequestLocal REQUEST_LOCAL = RequestLocal.noCaching();

    @Mock
    Time time;
    @Mock
    ScheduledExecutorService commitTickScheduler;
    @Mock
    FileCommitter fileCommitter;
    @Mock
    WriterMetrics writerMetrics;

    private BufferPool pool;
    private BrokerTopicStats brokerTopicStats;
    private Writer writer;
    private WriterTestUtils.RecordCreator recordCreator;

    @BeforeEach
    void setUp() {
        pool = new ElasticBufferPool(2);
        brokerTopicStats = new BrokerTopicStats();
        recordCreator = new WriterTestUtils.RecordCreator();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (writer != null) {
            writer.close();
        }
        if (pool != null) {
            pool.close();
        }
    }

    @Test
    void writerAcceptsNullBufferPool() {
        writer = new Writer(
            time, Duration.ofMillis(1), 8 * 1024, commitTickScheduler,
            fileCommitter, writerMetrics, brokerTopicStats, null, 0);

        // Should not throw
        assertThat(writer).isNotNull();
    }

    @Test
    void writerAcceptsBufferPool() {
        writer = new Writer(
            time, Duration.ofMillis(1), 8 * 1024, commitTickScheduler,
            fileCommitter, writerMetrics, brokerTopicStats, pool, 0);

        assertThat(writer).isNotNull();
    }

    @Test
    void writeWithPoolCreatesActiveFile() {
        writer = new Writer(
            time, Duration.ofMillis(1), 8 * 1024, commitTickScheduler,
            fileCommitter, writerMetrics, brokerTopicStats, pool, 0);

        // Write should succeed with buffer pool
        final Map<TopicIdPartition, MemoryRecords> writeRequest = Map.of(
            T0P0, recordCreator.create(T0P0.topicPartition(), 1)
        );
        var future = writer.write(writeRequest, TOPIC_CONFIGS, REQUEST_LOCAL);

        assertThat(future).isNotNull();
        assertThat(future).isNotCompleted();
    }

    @Test
    void writeWithoutPoolCreatesActiveFile() {
        writer = new Writer(
            time, Duration.ofMillis(1), 8 * 1024, commitTickScheduler,
            fileCommitter, writerMetrics, brokerTopicStats, null, 0);

        // Write should succeed without buffer pool (using heap allocation)
        final Map<TopicIdPartition, MemoryRecords> writeRequest = Map.of(
            T0P0, recordCreator.create(T0P0.topicPartition(), 1)
        );
        var future = writer.write(writeRequest, TOPIC_CONFIGS, REQUEST_LOCAL);

        assertThat(future).isNotNull();
        assertThat(future).isNotCompleted();
    }
}
