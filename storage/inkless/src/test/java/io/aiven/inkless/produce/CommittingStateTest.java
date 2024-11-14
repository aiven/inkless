// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.utils.Time;

import io.aiven.inkless.control_plane.CommitBatchRequest;
import io.aiven.inkless.control_plane.CommitBatchResponse;

import org.junit.jupiter.api.BeforeEach;
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
class CommittingStateTest {
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

    @BeforeEach
    void setup() {
        when(session.closeAndPrepareForCommit()).thenReturn(new BatchBufferCloseResult(
            commitBatchRequests, requestIds, data
        ));
    }

    @Test
    void commitStartedInConstructor() {
        final CommittingState state = new CommittingState(
            new WriterState.StateContext(time, commitTickScheduler, fileCommitter, MAX_BUFFER_SIZE, Duration.ofDays(1)),
            session);

        verify(commitTickScheduler, never()).schedule(any(), any());
        verify(fileCommitter).commit(same(commitBatchRequests), eq(data));
    }

    @Test
    void add() {
        final CommittingState state = new CommittingState(
            new WriterState.StateContext(time, commitTickScheduler, fileCommitter, MAX_BUFFER_SIZE, Duration.ofDays(1)),
            session);
        reset(fileCommitter);

        final WriterState.AddResult result =
            state.add(Map.of(new TopicPartition("topic", 0), MemoryRecords.EMPTY));

        assertThat(result.result()).isNotNull();
        assertThat(result.newState()).isInstanceOf(CommittingAndWritingState.class);
        verify(commitTickScheduler, never()).schedule(any(), any());
        // Even if it would be enough to trigger commit in `WritingState`, here it does nothing as
        // committing is already happening.
        verify(fileCommitter, never()).commit(anyList(), any());
    }

    @Test
    void tick() {
        final CommittingState state = new CommittingState(
            new WriterState.StateContext(time, commitTickScheduler, fileCommitter, MAX_BUFFER_SIZE, Duration.ofDays(1)),
            session);
        reset(fileCommitter);

        final WriterState.CommitTickResult result = state.commitTick();

        // This event must be ignored.
        assertThat(result.newState()).isSameAs(state);
        verify(commitTickScheduler, never()).schedule(any(), any());
        verify(fileCommitter, never()).commit(anyList(), any());
    }

    @Test
    void commitFinished() {
        final CommittingState state = new CommittingState(
            new WriterState.StateContext(time, commitTickScheduler, fileCommitter, MAX_BUFFER_SIZE, Duration.ofDays(1)),
            session);
        reset(fileCommitter);

        final List<CommitBatchResponse> commitBatchResponses = List.of(
            new CommitBatchResponse(Errors.NONE, 100)
        );
        final WriterState.CommitFinishedResult result = state.commitFinished(commitBatchResponses, null);

        assertThat(result.newState()).isInstanceOf(EmptyState.class);
        verify(commitTickScheduler, never()).schedule(any(), any());
        verify(fileCommitter, never()).commit(anyList(), any());
        verify(session).finishCommit(same(commitBatchResponses), isNull());
    }
}
