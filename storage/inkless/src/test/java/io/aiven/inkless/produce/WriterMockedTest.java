// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.ProduceResponse;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

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
    static final TopicPartition T0P0 = new TopicPartition("topic0", 0);
    static final TopicPartition T0P1 = new TopicPartition("topic0", 1);
    static final TopicPartition T1P0 = new TopicPartition("topic1", 0);
    static final TopicPartition T1P1 = new TopicPartition("topic1", 1);

    @Mock
    ScheduledExecutorService commitTickScheduler;
    @Mock
    FileCommitter fileCommitter;

    @Captor
    ArgumentCaptor<ClosedFile> closedFileCaptor;

    WriterTestUtils.RecordCreator recordCreator;

    @BeforeEach
    void setup() {
        recordCreator = new WriterTestUtils.RecordCreator();
    }

    @Test
    void tickWithEmptyFile() throws InterruptedException {
        final Writer writer = new Writer(
            Duration.ofMillis(1), 8 * 1024, commitTickScheduler, fileCommitter);

        verify(commitTickScheduler).scheduleAtFixedRate(any(), eq(1L), eq(1L), eq(TimeUnit.MILLISECONDS));

        writer.tick();

        // Tick must be ignored in as the active file is empty.
        verify(fileCommitter, never()).commit(any());
    }

    @Test
    void committingDueToOverfillWithFirstRequest() throws InterruptedException {
        final Writer writer = new Writer(
            Duration.ofMillis(1), 15908, commitTickScheduler, fileCommitter);

        final Map<TopicPartition, MemoryRecords> writeRequest = Map.of(
            T0P0, recordCreator.create(T0P0, 100),
            T0P1, recordCreator.create(T0P1, 100),
            T1P0, recordCreator.create(T1P0, 100),
            T1P1, recordCreator.create(T1P1, 100)
        );
        assertThat(writer.write(writeRequest)).isNotCompleted();

        // As we wrote too much, commit must be triggered.
        verify(fileCommitter).commit(closedFileCaptor.capture());
        assertThat(closedFileCaptor.getValue().originalRequests()).isEqualTo(Map.of(0, writeRequest));
        assertThat(closedFileCaptor.getValue().awaitingFuturesByRequest()).hasSize(1);
    }

    @Test
    void committingDueToOverfillWithMultipleRequests() throws InterruptedException {
        final Writer writer = new Writer(
            Duration.ofMillis(1), 8 * 1024, commitTickScheduler, fileCommitter);

        final Map<TopicPartition, MemoryRecords> writeRequest0 = Map.of(
            T0P0, recordCreator.create(T0P0, 1),
            T0P1, recordCreator.create(T0P1, 1),
            T1P0, recordCreator.create(T1P0, 1),
            T1P1, recordCreator.create(T1P1, 1)
        );
        final Map<TopicPartition, MemoryRecords> writeRequest1 = Map.of(
            T0P0, recordCreator.create(T0P0, 100),
            T0P1, recordCreator.create(T0P1, 100),
            T1P0, recordCreator.create(T1P0, 100),
            T1P1, recordCreator.create(T1P1, 100)
        );
        assertThat(writer.write(writeRequest0)).isNotCompleted();
        assertThat(writer.write(writeRequest1)).isNotCompleted();

        // As we wrote too much, commit must be triggered.
        verify(fileCommitter).commit(closedFileCaptor.capture());
        assertThat(closedFileCaptor.getValue().originalRequests())
            .isEqualTo(Map.of(0, writeRequest0, 1, writeRequest1));
        assertThat(closedFileCaptor.getValue().awaitingFuturesByRequest()).hasSize(2);
    }

    @Test
    void committingOnTick() throws InterruptedException {
        final Writer writer = new Writer(
            Duration.ofMillis(1), 8 * 1024, commitTickScheduler, fileCommitter);

        final Map<TopicPartition, MemoryRecords> writeRequest = Map.of(
            T0P0, recordCreator.create(T0P0, 1),
            T0P1, recordCreator.create(T0P1, 1),
            T1P0, recordCreator.create(T1P0, 1),
            T1P1, recordCreator.create(T1P1, 1)
        );
        assertThat(writer.write(writeRequest)).isNotCompleted();

        writer.tick();

        verify(fileCommitter).commit(closedFileCaptor.capture());
        assertThat(closedFileCaptor.getValue().originalRequests())
            .isEqualTo(Map.of(0, writeRequest));
        assertThat(closedFileCaptor.getValue().awaitingFuturesByRequest()).hasSize(1);
    }

    @Test
    void committingDueToClose() throws InterruptedException, IOException {
        final Writer writer = new Writer(
            Duration.ofMillis(1), 8 * 1024, commitTickScheduler, fileCommitter);

        final Map<TopicPartition, MemoryRecords> writeRequest = Map.of(
            T0P0, recordCreator.create(T0P0, 1),
            T0P1, recordCreator.create(T0P1, 1),
            T1P0, recordCreator.create(T1P0, 1),
            T1P1, recordCreator.create(T1P1, 1)
        );
        assertThat(writer.write(writeRequest)).isNotCompleted();

        writer.close();

        verify(fileCommitter).commit(closedFileCaptor.capture());
        assertThat(closedFileCaptor.getValue().originalRequests())
            .isEqualTo(Map.of(0, writeRequest));
        assertThat(closedFileCaptor.getValue().awaitingFuturesByRequest()).hasSize(1);
    }

    @Test
    void writeAfterRotation() throws InterruptedException {
        final Writer writer = new Writer(
            Duration.ofMillis(1), 8 * 1024, commitTickScheduler, fileCommitter);

        final Map<TopicPartition, MemoryRecords> writeRequest = Map.of(
            T0P0, recordCreator.create(T0P0, 100),
            T0P1, recordCreator.create(T0P1, 100),
            T1P0, recordCreator.create(T1P0, 100),
            T1P1, recordCreator.create(T1P1, 100)
        );
        assertThat(writer.write(writeRequest)).isNotCompleted();

        reset(fileCommitter);

        assertThat(writer.write(writeRequest)).isNotCompleted();

        verify(fileCommitter).commit(closedFileCaptor.capture());
        assertThat(closedFileCaptor.getValue().originalRequests()).isEqualTo(Map.of(0, writeRequest));
        assertThat(closedFileCaptor.getValue().awaitingFuturesByRequest()).hasSize(1);
    }

    @Test
    void close() throws IOException {
        final Writer writer = new Writer(
            Duration.ofMillis(1), 8 * 1024, commitTickScheduler, fileCommitter);
        reset(commitTickScheduler);

        writer.close();

        verify(commitTickScheduler).shutdownNow();
        verify(fileCommitter).close();
    }

    @Test
    void closeAfterClose() throws IOException {
        final Writer writer = new Writer(
            Duration.ofMillis(1), 8 * 1024, commitTickScheduler, fileCommitter);
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
            Duration.ofMillis(1), 8 * 1024, commitTickScheduler, fileCommitter);
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
            Duration.ofMillis(1), 8 * 1024, commitTickScheduler, fileCommitter);
        writer.close();
        reset(commitTickScheduler);
        reset(fileCommitter);

        final var writeResult = writer.write(Map.of(T0P0, recordCreator.create(T0P0, 10)));

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
            Duration.ofMillis(1), 8 * 1024, commitTickScheduler, fileCommitter);

        final InterruptedException interruptedException = new InterruptedException();
        doThrow(interruptedException).when(fileCommitter).commit(any());

        final Map<TopicPartition, MemoryRecords> writeRequest = Map.of(
            T0P0, recordCreator.create(T0P0, 100),
            T0P1, recordCreator.create(T0P1, 100),
            T1P0, recordCreator.create(T1P0, 100),
            T1P1, recordCreator.create(T1P1, 100)
        );

        assertThatThrownBy(() -> writer.write(writeRequest))
            .hasRootCause(interruptedException);

        // Shutdown happens.
        verify(commitTickScheduler).shutdownNow();
        verify(fileCommitter).close();
    }

    @Test
    void constructorInvalidArguments() {
        assertThatThrownBy(() -> new Writer(
            null, 8 * 1024, commitTickScheduler, fileCommitter))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("commitInterval cannot be null");
        assertThatThrownBy(() -> new Writer(
            Duration.ofMillis(1), 0, commitTickScheduler, fileCommitter))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("maxBufferSize must be positive");
        assertThatThrownBy(() -> new Writer(
            Duration.ofMillis(1), 8 * 1024, null, fileCommitter))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("commitTickScheduler cannot be null");
        assertThatThrownBy(() -> new Writer(
            Duration.ofMillis(1), 8 * 1024, commitTickScheduler, null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("fileCommitter cannot be null");
    }

    @Test
    void writeNull() {
        final Writer writer = new Writer(
            Duration.ofMillis(1), 8 * 1024, commitTickScheduler, fileCommitter);

        assertThatThrownBy(() -> writer.write(null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("entriesPerPartition cannot be null");
    }
}
