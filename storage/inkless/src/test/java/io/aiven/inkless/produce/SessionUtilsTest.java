// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class SessionUtilsTest {
    @Mock
    Time time;
    @Mock
    CommitTickScheduler commitTickScheduler;
    @Mock
    FileCommitter fileCommitter;

    @Test
    void sessionCanAddMoreData() {
        final int maxBufferSize = 100;
        final WriterState.StateContext context =
            new WriterState.StateContext(time, commitTickScheduler, fileCommitter, maxBufferSize, Duration.ofDays(1));
        final WriteSession session = new WriteSession(Instant.MIN);

        assertThat(SessionUtils.canSessionAddMoreData(session, context)).isTrue();
    }

    @Test
    void sessionCanNotAddMoreData() {
        final int maxBufferSize = 1070;
        final WriterState.StateContext context =
            new WriterState.StateContext(time, commitTickScheduler, fileCommitter, maxBufferSize, Duration.ofDays(1));
        final WriteSession session = new WriteSession(Instant.MIN);
        session.add(Map.of(
            new TopicPartition("t", 0), MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(new byte[1000]))
        ));

        assertThat(SessionUtils.canSessionAddMoreData(session, context)).isFalse();
    }

    @Test
    void sessionHasNotReachedCommitTimeout() {
        final long commitInterval = 250;
        final WriterState.StateContext context =
            new WriterState.StateContext(time, commitTickScheduler, fileCommitter, Integer.MAX_VALUE, Duration.ofMillis(commitInterval));
        final WriteSession session = new WriteSession(Instant.ofEpochMilli(0));

        when(time.milliseconds()).thenReturn(commitInterval - 1);
        assertThat(SessionUtils.hasSessionReachedCommitTimeout(session, context)).isFalse();
    }

    @Test
    void sessionHasReachedCommitTimeout() {
        final long commitInterval = 250;
        final WriterState.StateContext context =
            new WriterState.StateContext(time, commitTickScheduler, fileCommitter, Integer.MAX_VALUE, Duration.ofMillis(commitInterval));
        final WriteSession session = new WriteSession(Instant.ofEpochMilli(0));

        when(time.milliseconds()).thenReturn(commitInterval + 1);
        assertThat(SessionUtils.hasSessionReachedCommitTimeout(session, context)).isTrue();

        when(time.milliseconds()).thenReturn(commitInterval);
        assertThat(SessionUtils.hasSessionReachedCommitTimeout(session, context)).isTrue();
    }
}
