// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.common.utils.Time;

import io.aiven.inkless.control_plane.CommitBatchRequest;
import io.aiven.inkless.control_plane.CommitBatchResponse;
import io.aiven.inkless.storage_backend.common.StorageBackendException;

import org.junit.jupiter.api.BeforeEach;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class WriterMockedTest {
    static final TopicPartition T0P0 = new TopicPartition("topic0", 0);
    static final TopicPartition T0P1 = new TopicPartition("topic0", 1);
    static final TopicPartition T1P0 = new TopicPartition("topic1", 0);
    static final TopicPartition T1P1 = new TopicPartition("topic1", 1);

    @Mock
    Time time;
    @Mock
    CommitTickScheduler commitTickScheduler;
    @Mock
    FileCommitter fileCommitter;

    @Captor
    ArgumentCaptor<List<CommitBatchRequest>> commitBatchRequestsCaptor;

    WriterTestUtils.RecordCreator recordCreator;

    @BeforeEach
    void setup() {
        recordCreator = new WriterTestUtils.RecordCreator();
    }

    @Test
    void tickInEmpty() {
        final Writer writer = new Writer(commitTickScheduler, fileCommitter, time, Duration.ofDays(1), 8 * 1024);

        writer.tickTest();

        // Tick must be ignored in Empty.
        verify(commitTickScheduler, never()).schedule(any(), any());
        verify(fileCommitter, never()).commit(anyList(), any());
    }

    @ParameterizedTest
    @MethodSource("provideCommitFinishedCallbackArgs")
    void commitFinishedInEmpty(final List<CommitBatchResponse> commitBatchResponses, final Throwable error) {
        final Writer writer = new Writer(commitTickScheduler, fileCommitter, time, Duration.ofDays(1), 8 * 1024);

        writer.commitFinished(commitBatchResponses, error);

        // CommitFinished must be ignored in Empty.
        verify(commitTickScheduler, never()).schedule(any(), any());
        verify(fileCommitter, never()).commit(anyList(), any());
    }

    @ParameterizedTest
    @MethodSource("provideCommitFinishedCallbackArgs")
    void commitFinishedInWriting(final List<CommitBatchResponse> commitBatchResponses, final Throwable error) {
        final Writer writer = new Writer(commitTickScheduler, fileCommitter, time, Duration.ofDays(1), 8 * 1024);

        when(time.milliseconds()).thenReturn(10L);

        final var writeFuture = writer.write(
            Map.of(T0P0, recordCreator.create(T0P0, 10))
        );

        // As we enter Writing, the tick must be scheduled.
        verify(commitTickScheduler).schedule(eq(Instant.ofEpochMilli(10L)), any());

        reset(commitTickScheduler);

        writer.commitFinished(commitBatchResponses, error);

        // CommitFinished must be ignored in Writing.
        verify(commitTickScheduler, never()).schedule(any(), any());
        verify(fileCommitter, never()).commit(anyList(), any());

        assertThat(writeFuture).isNotCompleted();
    }

    @Test
    void committingDueToOverfillWithFirstRequest() {
        final Writer writer = new Writer(commitTickScheduler, fileCommitter, time, Duration.ofDays(1), 8 * 1024);

        when(time.milliseconds()).thenReturn(10L);

        final var writeFuture1 = writer.write(
            Map.of(
                T0P0, recordCreator.create(T0P0, 100),
                T0P1, recordCreator.create(T0P1, 100),
                T1P0, recordCreator.create(T1P0, 100),
                T1P1, recordCreator.create(T1P1, 100)
            )
        );

        // As we enter Writing, tick must be scheduled.
        verify(commitTickScheduler).schedule(eq(Instant.ofEpochMilli(10L)), any());
        // As we wrote too much, commit must be triggered.
        verify(fileCommitter).commit(commitBatchRequestsCaptor.capture(), any());
        assertThat(commitBatchRequestsCaptor.getValue()).hasSize(4);

        writer.commitFinished(List.of(
            new CommitBatchResponse(Errors.NONE, 1),
            new CommitBatchResponse(Errors.NONE, 2),
            new CommitBatchResponse(Errors.NONE, 3),
            new CommitBatchResponse(Errors.INVALID_TOPIC_EXCEPTION, -1)
        ), null);

        assertThat(writeFuture1).isCompletedWithValue(Map.of(
            T0P0, new PartitionResponse(Errors.NONE, 1, -1, -1),
            T0P1, new PartitionResponse(Errors.NONE, 2, -1, -1),
            T1P0, new PartitionResponse(Errors.NONE, 3, -1, -1),
            T1P1, new PartitionResponse(Errors.INVALID_TOPIC_EXCEPTION, -1, -1, -1)
        ));
    }

    @Test
    void committingDueToOverfillWithMultipleRequests() {
        final Writer writer = new Writer(commitTickScheduler, fileCommitter, time, Duration.ofDays(1), 8 * 1024);

        when(time.milliseconds()).thenReturn(10L);

        final var writeFuture1 = writer.write(
            Map.of(
                T0P0, recordCreator.create(T0P0, 1),
                T0P1, recordCreator.create(T0P1, 1),
                T1P0, recordCreator.create(T1P0, 1),
                T1P1, recordCreator.create(T1P1, 1)
            )
        );
        final var writeFuture2 = writer.write(
            Map.of(
                T0P0, recordCreator.create(T0P0, 100),
                T0P1, recordCreator.create(T0P1, 100),
                T1P0, recordCreator.create(T1P0, 100),
                T1P1, recordCreator.create(T1P1, 100)
            )
        );

        // As we enter Writing, tick must be scheduled.
        verify(commitTickScheduler).schedule(eq(Instant.ofEpochMilli(10L)), any());
        // As we wrote too much, commit must be triggered.
        verify(fileCommitter).commit(commitBatchRequestsCaptor.capture(), any());
        assertThat(commitBatchRequestsCaptor.getValue()).hasSize(8);

        writer.commitFinished(List.of(
            new CommitBatchResponse(Errors.NONE, 0),
            new CommitBatchResponse(Errors.NONE, 1),
            new CommitBatchResponse(Errors.NONE, 0),
            new CommitBatchResponse(Errors.NONE, 2),
            new CommitBatchResponse(Errors.NONE, 0),
            new CommitBatchResponse(Errors.NONE, 3),
            new CommitBatchResponse(Errors.NONE, 0),
            new CommitBatchResponse(Errors.INVALID_TOPIC_EXCEPTION, -1)
        ), null);

        assertThat(writeFuture1).isCompletedWithValue(Map.of(
            T0P0, new PartitionResponse(Errors.NONE, 0, -1, -1),
            T0P1, new PartitionResponse(Errors.NONE, 0, -1, -1),
            T1P0, new PartitionResponse(Errors.NONE, 0, -1, -1),
            T1P1, new PartitionResponse(Errors.NONE, 0, -1, -1)
        ));
        assertThat(writeFuture2).isCompletedWithValue(Map.of(
            T0P0, new PartitionResponse(Errors.NONE, 1, -1, -1),
            T0P1, new PartitionResponse(Errors.NONE, 2, -1, -1),
            T1P0, new PartitionResponse(Errors.NONE, 3, -1, -1),
            T1P1, new PartitionResponse(Errors.INVALID_TOPIC_EXCEPTION, -1, -1, -1)
        ));
    }

    @Test
    void failingCommit() {
        final Writer writer = new Writer(commitTickScheduler, fileCommitter, time, Duration.ofDays(1), 8 * 1024);

        when(time.milliseconds()).thenReturn(10L);

        final var writeFuture1 = writer.write(
            Map.of(
                T0P0, recordCreator.create(T0P0, 100),
                T0P1, recordCreator.create(T0P1, 100),
                T1P0, recordCreator.create(T1P0, 100),
                T1P1, recordCreator.create(T1P1, 100)
            )
        );

        // As we enter Writing, tick must be scheduled.
        verify(commitTickScheduler).schedule(eq(Instant.ofEpochMilli(10L)), any());
        // As we wrote too much, commit must be triggered.
        verify(fileCommitter).commit(commitBatchRequestsCaptor.capture(), any());
        assertThat(commitBatchRequestsCaptor.getValue()).hasSize(4);

        writer.commitFinished(null, new StorageBackendException("test"));

        assertThat(writeFuture1).isCompletedWithValue(Map.of(
            T0P0, new PartitionResponse(Errors.KAFKA_STORAGE_ERROR, -1, -1, -1, List.of(), "Error commiting data"),
            T0P1, new PartitionResponse(Errors.KAFKA_STORAGE_ERROR, -1, -1, -1, List.of(), "Error commiting data"),
            T1P0, new PartitionResponse(Errors.KAFKA_STORAGE_ERROR, -1, -1, -1, List.of(), "Error commiting data"),
            T1P1, new PartitionResponse(Errors.KAFKA_STORAGE_ERROR, -1, -1, -1, List.of(), "Error commiting data")
        ));
    }

    @Test
    void committingDueToTimeout() {
        final Writer writer = new Writer(commitTickScheduler, fileCommitter, time, Duration.ofDays(1), 8 * 1024);

        when(time.milliseconds()).thenReturn(10L);

        final var writeFuture1 = writer.write(
            Map.of(
                T0P0, recordCreator.create(T0P0, 10),
                T0P1, recordCreator.create(T0P1, 10),
                T1P0, recordCreator.create(T1P0, 10),
                T1P1, recordCreator.create(T1P1, 10)
            )
        );

        // As we enter Writing, tick must be scheduled.
        verify(commitTickScheduler).schedule(eq(Instant.ofEpochMilli(10L)), any());
        // As haven't written enough, commit must not be triggered.
        verify(fileCommitter, never()).commit(any(), any());

        writer.tickTest();

        verify(fileCommitter).commit(commitBatchRequestsCaptor.capture(), any());
        assertThat(commitBatchRequestsCaptor.getValue()).hasSize(4);

        writer.commitFinished(List.of(
            new CommitBatchResponse(Errors.NONE, 1),
            new CommitBatchResponse(Errors.NONE, 2),
            new CommitBatchResponse(Errors.NONE, 3),
            new CommitBatchResponse(Errors.INVALID_TOPIC_EXCEPTION, -1)
        ), null);

        assertThat(writeFuture1).isCompletedWithValue(Map.of(
            T0P0, new PartitionResponse(Errors.NONE, 1, -1, -1),
            T0P1, new PartitionResponse(Errors.NONE, 2, -1, -1),
            T1P0, new PartitionResponse(Errors.NONE, 3, -1, -1),
            T1P1, new PartitionResponse(Errors.INVALID_TOPIC_EXCEPTION, -1, -1, -1)
        ));
    }

    @Test
    void committingAndWritingOverfill() {
        final Writer writer = new Writer(commitTickScheduler, fileCommitter, time, Duration.ofDays(1), 8 * 1024);

        when(time.milliseconds()).thenReturn(10L);

        final var writeFuture1 = writer.write(
            Map.of(
                T0P0, recordCreator.create(T0P0, 100),
                T0P1, recordCreator.create(T0P1, 100),
                T1P0, recordCreator.create(T1P0, 100),
                T1P1, recordCreator.create(T1P1, 100)
            )
        );

        // As we enter Writing, tick must be scheduled.
        verify(commitTickScheduler).schedule(eq(Instant.ofEpochMilli(10L)), any());
        // As we wrote too much, commit must be triggered.
        verify(fileCommitter).commit(commitBatchRequestsCaptor.capture(), any());
        assertThat(commitBatchRequestsCaptor.getValue()).hasSize(4);

        reset(fileCommitter);

        // While we're committing, another write happens.
        final var writeFuture2 = writer.write(
            Map.of(
                T0P0, recordCreator.create(T0P0, 100),
                T0P1, recordCreator.create(T0P1, 100),
                T1P0, recordCreator.create(T1P0, 100),
                T1P1, recordCreator.create(T1P1, 100)
            )
        );

        writer.commitFinished(List.of(
            new CommitBatchResponse(Errors.NONE, 1),
            new CommitBatchResponse(Errors.NONE, 2),
            new CommitBatchResponse(Errors.NONE, 3),
            new CommitBatchResponse(Errors.INVALID_TOPIC_EXCEPTION, -1)
        ), null);

        assertThat(writeFuture1).isCompletedWithValue(Map.of(
            T0P0, new PartitionResponse(Errors.NONE, 1, -1, -1),
            T0P1, new PartitionResponse(Errors.NONE, 2, -1, -1),
            T1P0, new PartitionResponse(Errors.NONE, 3, -1, -1),
            T1P1, new PartitionResponse(Errors.INVALID_TOPIC_EXCEPTION, -1, -1, -1)
        ));
        assertThat(writeFuture2).isNotCompleted();

        // The second session is being committed as well.
        verify(fileCommitter).commit(commitBatchRequestsCaptor.capture(), any());
        assertThat(commitBatchRequestsCaptor.getValue()).hasSize(4);

        writer.commitFinished(List.of(
            new CommitBatchResponse(Errors.NONE, 10),
            new CommitBatchResponse(Errors.NONE, 20),
            new CommitBatchResponse(Errors.NONE, 30),
            new CommitBatchResponse(Errors.NONE, 40)
        ), null);

        assertThat(writeFuture2).isCompletedWithValue(Map.of(
            T0P0, new PartitionResponse(Errors.NONE, 10, -1, -1),
            T0P1, new PartitionResponse(Errors.NONE, 20, -1, -1),
            T1P0, new PartitionResponse(Errors.NONE, 30, -1, -1),
            T1P1, new PartitionResponse(Errors.NONE, 40, -1, -1)
        ));
    }

    @Test
    void committingAndWritingTime() {
        final Writer writer = new Writer(commitTickScheduler, fileCommitter, time, Duration.ofMillis(100), 8 * 1024);

        when(time.milliseconds()).thenReturn(10L);

        final var writeFuture1 = writer.write(
            Map.of(
                T0P0, recordCreator.create(T0P0, 100),
                T0P1, recordCreator.create(T0P1, 100),
                T1P0, recordCreator.create(T1P0, 100),
                T1P1, recordCreator.create(T1P1, 100)
            )
        );

        // As we enter Writing, tick must be scheduled.
        verify(commitTickScheduler).schedule(eq(Instant.ofEpochMilli(10L)), any());
        // As we wrote too much, commit must be triggered.
        verify(fileCommitter).commit(commitBatchRequestsCaptor.capture(), any());
        assertThat(commitBatchRequestsCaptor.getValue()).hasSize(4);

        reset(fileCommitter);

        // While we're committing, another write happens.
        final var writeFuture2 = writer.write(
            Map.of(
                T0P0, recordCreator.create(T0P0, 1),
                T0P1, recordCreator.create(T0P1, 1),
                T1P0, recordCreator.create(T1P0, 1),
                T1P1, recordCreator.create(T1P1, 1)
            )
        );

        when(time.milliseconds()).thenReturn(1000L);

        writer.commitFinished(List.of(
            new CommitBatchResponse(Errors.NONE, 1),
            new CommitBatchResponse(Errors.NONE, 2),
            new CommitBatchResponse(Errors.NONE, 3),
            new CommitBatchResponse(Errors.INVALID_TOPIC_EXCEPTION, -1)
        ), null);

        assertThat(writeFuture1).isCompletedWithValue(Map.of(
            T0P0, new PartitionResponse(Errors.NONE, 1, -1, -1),
            T0P1, new PartitionResponse(Errors.NONE, 2, -1, -1),
            T1P0, new PartitionResponse(Errors.NONE, 3, -1, -1),
            T1P1, new PartitionResponse(Errors.INVALID_TOPIC_EXCEPTION, -1, -1, -1)
        ));
        assertThat(writeFuture2).isNotCompleted();

        // The second session is being committed as well.
        verify(fileCommitter).commit(commitBatchRequestsCaptor.capture(), any());
        assertThat(commitBatchRequestsCaptor.getValue()).hasSize(4);

        writer.commitFinished(List.of(
            new CommitBatchResponse(Errors.NONE, 10),
            new CommitBatchResponse(Errors.NONE, 20),
            new CommitBatchResponse(Errors.NONE, 30),
            new CommitBatchResponse(Errors.NONE, 40)
        ), null);

        assertThat(writeFuture2).isCompletedWithValue(Map.of(
            T0P0, new PartitionResponse(Errors.NONE, 10, -1, -1),
            T0P1, new PartitionResponse(Errors.NONE, 20, -1, -1),
            T1P0, new PartitionResponse(Errors.NONE, 30, -1, -1),
            T1P1, new PartitionResponse(Errors.NONE, 40, -1, -1)
        ));
    }

    private static Stream<Arguments> provideCommitFinishedCallbackArgs() {
        return Stream.of(
            Arguments.of(List.of(), null),
            Arguments.of(null, new StorageBackendException("test"))
        );
    }
}
