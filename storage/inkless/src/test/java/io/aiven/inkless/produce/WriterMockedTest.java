// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class WriterMockedTest {
    static final String TOPIC_0 = "topic0";
    static final String TOPIC_1 = "topic1";
    static final Uuid TOPIC_ID_0 = new Uuid(0, 1);
    static final Uuid TOPIC_ID_1 = new Uuid(0, 2);
    static final TopicIdPartition T0P0 = new TopicIdPartition(TOPIC_ID_0, 0, TOPIC_0);
    static final TopicIdPartition T0P1 = new TopicIdPartition(TOPIC_ID_0, 1, TOPIC_0);
    static final TopicIdPartition T1P0 = new TopicIdPartition(TOPIC_ID_1, 0, TOPIC_1);
    static final TopicIdPartition T1P1 = new TopicIdPartition(TOPIC_ID_1, 1, TOPIC_1);

    static final Map<String, LogConfig> TOPIC_CONFIGS = Map.of(
        TOPIC_0, logConfig(Map.of(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, TimestampType.CREATE_TIME.name)),
        TOPIC_1, logConfig(Map.of(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, TimestampType.LOG_APPEND_TIME.name))
    );

    static LogConfig logConfig(Map<String, ?> config) {
        return new LogConfig(config);
    }

    @Mock
    Time time;
    @Mock
    ScheduledExecutorService commitTickScheduler;
    @Mock
    FileCommitter fileCommitter;
    @Mock
    WriterMetrics writerMetrics;

    BrokerTopicStats brokerTopicStats;

    @Captor
    ArgumentCaptor<ClosedFile> closedFileCaptor;

    WriterTestUtils.RecordCreator recordCreator;

    @BeforeEach
    void setup() {
        recordCreator = new WriterTestUtils.RecordCreator();
        brokerTopicStats = new BrokerTopicStats();
    }

    @Test
    void tickWithEmptyFile() throws InterruptedException {
        final Writer writer = new Writer(
            time, Duration.ofMillis(1), 8 * 1024, commitTickScheduler, fileCommitter, writerMetrics, brokerTopicStats);

        writer.tick();

        // Tick must be ignored in as the active file is empty.
        verify(fileCommitter, never()).commit(any());
    }

    @Test
    void tickIsScheduledWhenFileIsWrittenTo() {
        final Writer writer = new Writer(
            time, Duration.ofMillis(1), 8 * 1024, commitTickScheduler, fileCommitter, writerMetrics, brokerTopicStats);

        final Map<TopicIdPartition, MemoryRecords> writeRequest = Map.of(
            T0P0, recordCreator.create(T0P0.topicPartition(), 100)
        );
        writer.write(writeRequest, TOPIC_CONFIGS);

        verify(commitTickScheduler).schedule(any(Runnable.class), eq(1L), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    void committingDueToOverfillWithFirstRequest() throws InterruptedException {
        when(time.nanoseconds()).thenReturn(10_000_000L);

        final Writer writer = new Writer(
            time, Duration.ofMillis(1), 15908, commitTickScheduler, fileCommitter, writerMetrics, brokerTopicStats);

        final Map<TopicIdPartition, MemoryRecords> writeRequest = Map.of(
            T0P0, recordCreator.create(T0P0.topicPartition(), 100),
            T0P1, recordCreator.create(T0P1.topicPartition(), 100),
            T1P0, recordCreator.create(T1P0.topicPartition(), 100),
            T1P1, recordCreator.create(T1P1.topicPartition(), 100)
        );
        assertThat(writer.write(writeRequest, TOPIC_CONFIGS)).isNotCompleted();

        // As we wrote too much, commit must be triggered.
        verify(fileCommitter).commit(closedFileCaptor.capture());
        assertThat(closedFileCaptor.getValue().start()).isEqualTo(Instant.ofEpochMilli(10));
        assertThat(closedFileCaptor.getValue().allFuturesByRequest()).hasSize(1);
    }

    @Test
    void committingDueToOverfillWithMultipleRequests() throws InterruptedException {
        final Writer writer = new Writer(
            time, Duration.ofMillis(1), 8 * 1024, commitTickScheduler, fileCommitter, writerMetrics, brokerTopicStats);

        final Map<TopicIdPartition, MemoryRecords> writeRequest0 = Map.of(
            T0P0, recordCreator.create(T0P0.topicPartition(), 1),
            T0P1, recordCreator.create(T0P1.topicPartition(), 1),
            T1P0, recordCreator.create(T1P0.topicPartition(), 1),
            T1P1, recordCreator.create(T1P1.topicPartition(), 1)
        );
        final Map<TopicIdPartition, MemoryRecords> writeRequest1 = Map.of(
            T0P0, recordCreator.create(T0P0.topicPartition(), 100),
            T0P1, recordCreator.create(T0P1.topicPartition(), 100),
            T1P0, recordCreator.create(T1P0.topicPartition(), 100),
            T1P1, recordCreator.create(T1P1.topicPartition(), 100)
        );
        assertThat(writer.write(writeRequest0, TOPIC_CONFIGS)).isNotCompleted();
        assertThat(writer.write(writeRequest1, TOPIC_CONFIGS)).isNotCompleted();

        // As we wrote too much, commit must be triggered.
        verify(fileCommitter).commit(closedFileCaptor.capture());
        assertThat(closedFileCaptor.getValue().allFuturesByRequest()).hasSize(2);
    }

    @Test
    void committingOnTick() throws InterruptedException {
        final Writer writer = new Writer(
            time, Duration.ofMillis(1), 8 * 1024, commitTickScheduler, fileCommitter, writerMetrics, brokerTopicStats);

        final Map<TopicIdPartition, MemoryRecords> writeRequest = Map.of(
            T0P0, recordCreator.create(T0P0.topicPartition(), 1),
            T0P1, recordCreator.create(T0P1.topicPartition(), 1),
            T1P0, recordCreator.create(T1P0.topicPartition(), 1),
            T1P1, recordCreator.create(T1P1.topicPartition(), 1)
        );
        assertThat(writer.write(writeRequest, TOPIC_CONFIGS)).isNotCompleted();

        writer.tick();

        verify(fileCommitter).commit(closedFileCaptor.capture());
        assertThat(closedFileCaptor.getValue().allFuturesByRequest()).hasSize(1);
    }

    @Test
    void committingDueToClose() throws InterruptedException, IOException {
        final Writer writer = new Writer(
            time, Duration.ofMillis(1), 8 * 1024, commitTickScheduler, fileCommitter, writerMetrics, brokerTopicStats);

        final Map<TopicIdPartition, MemoryRecords> writeRequest = Map.of(
            T0P0, recordCreator.create(T0P0.topicPartition(), 1),
            T0P1, recordCreator.create(T0P1.topicPartition(), 1),
            T1P0, recordCreator.create(T1P0.topicPartition(), 1),
            T1P1, recordCreator.create(T1P1.topicPartition(), 1)
        );
        assertThat(writer.write(writeRequest, TOPIC_CONFIGS)).isNotCompleted();

        writer.close();

        verify(fileCommitter).commit(closedFileCaptor.capture());
        assertThat(closedFileCaptor.getValue().allFuturesByRequest()).hasSize(1);
    }

    @Test
    void writeAfterRotation() throws InterruptedException {
        final Writer writer = new Writer(
            time, Duration.ofMillis(1), 8 * 1024, commitTickScheduler, fileCommitter, writerMetrics, brokerTopicStats);

        final Map<TopicIdPartition, MemoryRecords> writeRequest = Map.of(
            T0P0, recordCreator.create(T0P0.topicPartition(), 100),
            T0P1, recordCreator.create(T0P1.topicPartition(), 100),
            T1P0, recordCreator.create(T1P0.topicPartition(), 100),
            T1P1, recordCreator.create(T1P1.topicPartition(), 100)
        );
        assertThat(writer.write(writeRequest, TOPIC_CONFIGS)).isNotCompleted();

        reset(fileCommitter);

        assertThat(writer.write(writeRequest, TOPIC_CONFIGS)).isNotCompleted();

        verify(fileCommitter).commit(closedFileCaptor.capture());
        assertThat(closedFileCaptor.getValue().allFuturesByRequest()).hasSize(1);
    }

    @Test
    void close() throws IOException {
        final Writer writer = new Writer(
            time, Duration.ofMillis(1), 8 * 1024, commitTickScheduler, fileCommitter, writerMetrics, brokerTopicStats);
        reset(commitTickScheduler);

        writer.close();

        verify(commitTickScheduler).shutdownNow();
        verify(fileCommitter).close();
    }

    @Test
    void closeAfterClose() throws IOException {
        final Writer writer = new Writer(
            time, Duration.ofMillis(1), 8 * 1024, commitTickScheduler, fileCommitter, writerMetrics, brokerTopicStats);
        writer.close();

        reset(commitTickScheduler);
        reset(fileCommitter);

        writer.close();

        verifyNoInteractions(commitTickScheduler);
        verifyNoInteractions(fileCommitter);
    }

    @Test
    void tickAfterClose() throws IOException {
        final Writer writer = new Writer(
            time, Duration.ofMillis(1), 8 * 1024, commitTickScheduler, fileCommitter, writerMetrics, brokerTopicStats);
        writer.close();

        reset(commitTickScheduler);
        reset(fileCommitter);

        writer.tick();

        verifyNoInteractions(commitTickScheduler);
        verifyNoInteractions(fileCommitter);
    }

    @Test
    void writeAfterClose() throws IOException {
        final Writer writer = new Writer(
            time, Duration.ofMillis(1), 8 * 1024, commitTickScheduler, fileCommitter, writerMetrics, brokerTopicStats);
        writer.close();
        reset(commitTickScheduler);
        reset(fileCommitter);

        final var writeResult = writer.write(Map.of(T0P0, recordCreator.create(T0P0.topicPartition(), 10)), TOPIC_CONFIGS);

        assertThat(writeResult).isCompletedExceptionally();
        assertThatThrownBy(writeResult::get)
            .isInstanceOf(ExecutionException.class)
            .hasRootCauseInstanceOf(RuntimeException.class)
            .hasRootCauseMessage("Writer already closed");

        verifyNoInteractions(commitTickScheduler);
        verifyNoInteractions(fileCommitter);
    }

    @Test
    void commitInterrupted() throws InterruptedException, IOException {
        final Writer writer = new Writer(
            time, Duration.ofMillis(1), 8 * 1024, commitTickScheduler, fileCommitter, writerMetrics, brokerTopicStats);

        final InterruptedException interruptedException = new InterruptedException();
        doThrow(interruptedException).when(fileCommitter).commit(any());

        final Map<TopicIdPartition, MemoryRecords> writeRequest = Map.of(
            T0P0, recordCreator.create(T0P0.topicPartition(), 100),
            T0P1, recordCreator.create(T0P1.topicPartition(), 100),
            T1P0, recordCreator.create(T1P0.topicPartition(), 100),
            T1P1, recordCreator.create(T1P1.topicPartition(), 100)
        );

        assertThatThrownBy(() -> writer.write(writeRequest, TOPIC_CONFIGS))
            .hasRootCause(interruptedException);

        // Shutdown happens.
        verify(commitTickScheduler).shutdownNow();
        verify(fileCommitter).close();
    }

    @Test
    void constructorInvalidArguments() {
        assertThatThrownBy(() -> new Writer(
            null, Duration.ofMillis(1), 8 * 1024, commitTickScheduler, fileCommitter, writerMetrics, brokerTopicStats))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("time cannot be null");
        assertThatThrownBy(() -> new Writer(
            time, null, 8 * 1024, commitTickScheduler, fileCommitter, writerMetrics, brokerTopicStats))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("commitInterval cannot be null");
        assertThatThrownBy(() -> new Writer(
            time, Duration.ofMillis(1), 0, commitTickScheduler, fileCommitter, writerMetrics, brokerTopicStats))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("maxBufferSize must be positive");
        assertThatThrownBy(() ->
            new Writer(time, Duration.ofMillis(1), 8 * 1024, null, fileCommitter, writerMetrics, brokerTopicStats))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("commitTickScheduler cannot be null");
        assertThatThrownBy(() -> new Writer(time, Duration.ofMillis(1), 8 * 1024, commitTickScheduler, fileCommitter, null, brokerTopicStats))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("writerMetrics cannot be null");
        assertThatThrownBy(() -> new Writer(time, Duration.ofMillis(1), 8 * 1024, commitTickScheduler, null, writerMetrics, brokerTopicStats))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("fileCommitter cannot be null");
    }

    @Test
    void writeNull() {
        final Writer writer = new Writer(time, Duration.ofMillis(1), 8 * 1024, commitTickScheduler, fileCommitter, writerMetrics, brokerTopicStats);

        assertThatThrownBy(() -> writer.write(null, TOPIC_CONFIGS))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("entriesPerPartition cannot be null");
        assertThatThrownBy(() -> writer.write(Map.of(), null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("topicConfigs cannot be null");
    }

    @Test
    void writeEmptyRequests() {
        final Writer writer = new Writer(time, Duration.ofMillis(1), 8 * 1024, commitTickScheduler, fileCommitter, writerMetrics, brokerTopicStats);

        assertThatThrownBy(() -> writer.write(Map.of(), TOPIC_CONFIGS))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("entriesPerPartition cannot be empty");
    }

    @Test
    void entriesTopicConfigMismatch() {
        final Writer writer = new Writer(time, Duration.ofMillis(1), 8 * 1024, commitTickScheduler, fileCommitter, writerMetrics, brokerTopicStats);

        assertThatThrownBy(() -> writer.write(Map.of(T0P0, MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(new byte[10]))), Map.of(TOPIC_1, new LogConfig(Map.of()))))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Configs are not including all the topics requested");
    }
}
