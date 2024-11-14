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

import io.aiven.inkless.produce.WriterState.StateContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class EmptyStateTest {
    static final int MAX_BUFFER_SIZE = 1000;

    @Mock
    Time time;
    @Mock
    CommitTickScheduler commitTickScheduler;
    @Mock
    FileCommitter fileCommitter;

    @Test
    void nothingHappensInConstructor() {
        new EmptyState(new StateContext(time, commitTickScheduler, fileCommitter, MAX_BUFFER_SIZE, Duration.ofDays(1)));

        verify(commitTickScheduler, never()).schedule(any(), any());
        verify(fileCommitter, never()).commit(anyList(), any());
    }

    @Test
    void addNotEnoughToTriggerCommit() {
        final EmptyState state = new EmptyState(
            new StateContext(time, commitTickScheduler, fileCommitter, MAX_BUFFER_SIZE, Duration.ofDays(1)));

        when(time.milliseconds()).thenReturn(10L);

        final WriterState.AddResult result =
            state.add(Map.of(new TopicPartition("topic", 0), MemoryRecords.EMPTY));

        assertThat(result.result()).isNotNull();
        assertThat(result.newState()).isInstanceOf(WritingState.class);
        verify(commitTickScheduler).schedule(eq(Instant.ofEpochMilli(10L)), same(result.newState()));
        verify(fileCommitter, never()).commit(anyList(), any());
    }

    @Test
    void addEnoughToTriggerCommit() {
        final EmptyState state = new EmptyState(
            new StateContext(time, commitTickScheduler, fileCommitter, 0, Duration.ofDays(1)));

        when(time.milliseconds()).thenReturn(10L);

        final WriterState.AddResult result =
            state.add(Map.of(new TopicPartition("topic", 0), MemoryRecords.EMPTY));

        assertThat(result.result()).isNotNull();
        assertThat(result.newState()).isInstanceOf(CommittingState.class);
        // The tick is scheduled anyway, for the intermediate write state.
        verify(commitTickScheduler).schedule(eq(Instant.ofEpochMilli(10L)), isA(WritingState.class));
        verify(fileCommitter).commit(anyList(), any());
    }

    @Test
    void tick() {
        final EmptyState state = new EmptyState(
            new StateContext(time, commitTickScheduler, fileCommitter, MAX_BUFFER_SIZE, Duration.ofDays(1)));

        final WriterState.CommitTickResult result = state.commitTick();

        // This event must be ignored.
        assertThat(result.newState()).isSameAs(state);
        verify(commitTickScheduler, never()).schedule(any(), any());
        verify(fileCommitter, never()).commit(anyList(), any());
    }

    @Test
    void commitFinished() {
        final EmptyState state = new EmptyState(
            new StateContext(time, commitTickScheduler, fileCommitter, MAX_BUFFER_SIZE, Duration.ofDays(1)));

        final WriterState.CommitFinishedResult result = state.commitFinished(null, null);

        // This event must be ignored.
        assertThat(result.newState()).isSameAs(state);
        verify(commitTickScheduler, never()).schedule(any(), any());
        verify(fileCommitter, never()).commit(anyList(), any());
    }
}
