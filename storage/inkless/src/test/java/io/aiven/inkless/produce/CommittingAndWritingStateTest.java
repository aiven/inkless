// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.utils.Time;

import io.aiven.inkless.control_plane.CommitBatchRequest;
import io.aiven.inkless.control_plane.CommitBatchResponse;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class CommittingAndWritingStateTest {
    static final int MAX_BUFFER_SIZE = 1000;

    @Mock
    Time time;
    @Mock
    CommitTickScheduler commitTickScheduler;
    @Mock
    FileCommitter fileCommitter;

    @Mock
    List<CommitBatchRequest> commitBatchRequests;
    @Mock
    List<Integer> requestIds;
    byte[] data = new byte[8];
    @Mock
    WriteSession session;

    @Test
    void nothingHappensInConstructor() {
        final CommittingAndWritingState state = new CommittingAndWritingState(
            new WriterState.StateContext(time, commitTickScheduler, fileCommitter, MAX_BUFFER_SIZE, Duration.ofDays(1)),
            session);

        verify(commitTickScheduler, never()).schedule(any(), any());
        verify(fileCommitter, never()).commit(anyList(), any());
    }

    @Test
    void addNotEnoughToTriggerCommitAfterCurrentCommitCompleted() {
        // When the active batch started.
        when(time.milliseconds()).thenReturn(10L);

        final CommittingAndWritingState state = new CommittingAndWritingState(
            new WriterState.StateContext(time, commitTickScheduler, fileCommitter, MAX_BUFFER_SIZE, Duration.ofMillis(100)),
            session);

        final WriterState.AddResult result1 =
            state.add(Map.of(new TopicPartition("topic", 0), MemoryRecords.EMPTY));

        assertThat(result1.result()).isNotNull();
        assertThat(result1.newState()).isSameAs(state);
        verify(commitTickScheduler, never()).schedule(any(), any());
        verify(fileCommitter, never()).commit(anyList(), any());

        reset(commitTickScheduler);
        reset(fileCommitter);

        final List<CommitBatchResponse> commitBatchResponses = List.of(
            new CommitBatchResponse(Errors.NONE, 100)
        );
        final WriterState.CommitFinishedResult result2 = state.commitFinished(commitBatchResponses, null);

        assertThat(result2.newState()).isInstanceOf(WritingState.class);
        verify(commitTickScheduler).schedule(eq(Instant.ofEpochMilli(10L)), same(result2.newState()));
        verify(fileCommitter, never()).commit(anyList(), any());
        verify(session).finishCommit(same(commitBatchResponses), isNull());
    }

    @Test
    void addEnoughToTriggerCommitAfterCurrentCommitCompleted() {
        final CommittingAndWritingState state = new CommittingAndWritingState(
            new WriterState.StateContext(time, commitTickScheduler, fileCommitter, 0, Duration.ofDays(1)),
            session);

        final WriterState.AddResult result1 =
            state.add(Map.of(new TopicPartition("topic", 0), MemoryRecords.EMPTY));

        assertThat(result1.result()).isNotNull();
        assertThat(result1.newState()).isSameAs(state);
        verify(commitTickScheduler, never()).schedule(any(), any());
        verify(fileCommitter, never()).commit(anyList(), any());

        reset(commitTickScheduler);
        reset(fileCommitter);

        final List<CommitBatchResponse> commitBatchResponses = List.of(
            new CommitBatchResponse(Errors.NONE, 100)
        );
        final WriterState.CommitFinishedResult result2 = state.commitFinished(commitBatchResponses, null);

        assertThat(result2.newState()).isInstanceOf(CommittingState.class);
        verify(commitTickScheduler, never()).schedule(any(), any());
        verify(fileCommitter).commit(anyList(), any());
        verify(session).finishCommit(same(commitBatchResponses), isNull());
    }

    @Test
    void sessionOldEnoughToTriggerCommitAfterCurrentCommitCompleted() {
        final long commitInterval = 100;
        final CommittingAndWritingState state = new CommittingAndWritingState(
            new WriterState.StateContext(time, commitTickScheduler, fileCommitter, Integer.MAX_VALUE, Duration.ofMillis(commitInterval)),
            session);

        final WriterState.AddResult result1 =
            state.add(Map.of(new TopicPartition("topic", 0), MemoryRecords.EMPTY));

        assertThat(result1.result()).isNotNull();
        assertThat(result1.newState()).isSameAs(state);
        verify(commitTickScheduler, never()).schedule(any(), any());
        verify(fileCommitter, never()).commit(anyList(), any());

        reset(commitTickScheduler);
        reset(fileCommitter);

        when(time.milliseconds()).thenReturn(commitInterval);
        final List<CommitBatchResponse> commitBatchResponses = List.of(
            new CommitBatchResponse(Errors.NONE, 10)
        );
        final WriterState.CommitFinishedResult result2 = state.commitFinished(commitBatchResponses, null);

        assertThat(result2.newState()).isInstanceOf(CommittingState.class);
        verify(commitTickScheduler, never()).schedule(any(), any());
        verify(fileCommitter).commit(anyList(), any());
        verify(session).finishCommit(same(commitBatchResponses), isNull());
    }

    @Test
    void commitTick() {
        final CommittingAndWritingState state = new CommittingAndWritingState(
            new WriterState.StateContext(time, commitTickScheduler, fileCommitter, MAX_BUFFER_SIZE, Duration.ofDays(1)),
            session);

        final WriterState.CommitTickResult result = state.commitTick();

        // This event must be ignored.
        assertThat(result.newState()).isSameAs(state);
        verify(commitTickScheduler, never()).schedule(any(), any());
        verify(fileCommitter, never()).commit(anyList(), any());
    }

    @Test
    void commitFinished() {
        // Tested by addNotEnoughToTriggerUploadAfterCurrentUploadCompleted
        // and addEnoughToTriggerUploadAfterCurrentUploadCompleted above.
    }
}
