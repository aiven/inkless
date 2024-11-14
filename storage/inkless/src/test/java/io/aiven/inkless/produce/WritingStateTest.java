// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.utils.Time;

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
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class WritingStateTest {
    static final int MAX_BUFFER_SIZE = 1000;

    @Mock
    Time time;
    @Mock
    CommitTickScheduler commitTickScheduler;
    @Mock
    FileCommitter fileCommitter;

    @Test
    void commitTickScheduledInConstructor() {
        when(time.milliseconds()).thenReturn(10L);

        final WritingState state = new WritingState(
            new WriterState.StateContext(time, commitTickScheduler, fileCommitter, MAX_BUFFER_SIZE, Duration.ofDays(1)));

        verify(commitTickScheduler).schedule(eq(Instant.ofEpochMilli(10L)), same(state));
        verify(fileCommitter, never()).commit(anyList(), any());
    }

    @Test
    void addNotEnoughToTriggerCommit() {
        final WritingState state = new WritingState(
            new WriterState.StateContext(time, commitTickScheduler, fileCommitter, MAX_BUFFER_SIZE, Duration.ofDays(1)));
        reset(commitTickScheduler);

        final WriterState.AddResult result =
            state.add(Map.of(new TopicPartition("topic", 0), MemoryRecords.EMPTY));

        assertThat(result.result()).isNotNull();
        assertThat(result.newState()).isSameAs(state);
        verify(commitTickScheduler, never()).schedule(any(), any());
        verify(fileCommitter, never()).commit(anyList(), any());
    }

    @Test
    void addEnoughToTriggerCommit() {
        final WritingState state = new WritingState(
            new WriterState.StateContext(time, commitTickScheduler, fileCommitter, 0, Duration.ofDays(1)));
        reset(commitTickScheduler);

        final WriterState.AddResult result =
            state.add(Map.of(new TopicPartition("topic", 0), MemoryRecords.EMPTY));

        assertThat(result.result()).isNotNull();
        assertThat(result.newState()).isInstanceOf(CommittingState.class);
        verify(commitTickScheduler, never()).schedule(any(), any());
        verify(fileCommitter).commit(anyList(), any());
    }

    @Test
    void commitTick() {
        final WritingState state = new WritingState(
            new WriterState.StateContext(time, commitTickScheduler, fileCommitter, MAX_BUFFER_SIZE, Duration.ofDays(1)));

        final WriterState.CommitTickResult result = state.commitTick();
        assertThat(result.newState()).isInstanceOf(CommittingState.class);
    }

    @Test
    void commitFinished() {
        final WritingState state = new WritingState(
            new WriterState.StateContext(time, commitTickScheduler, fileCommitter, MAX_BUFFER_SIZE, Duration.ofDays(1)));

        final WriterState.CommitFinishedResult result = state.commitFinished(null, null);
        assertThat(result.newState()).isSameAs(state);
    }
}
