// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.utils.Time;

import io.aiven.inkless.control_plane.CommitBatchResponse;

/**
 * A state for {@link Writer}.
 *
 * <p>The possible state transitions:
 * <pre>
 *                                   +---------+
 *                                   |         |
 *      +-------+       a       +----+----+    | a
 *      | Empty |-------------->| Writing |<---+
 *      +-------+               +-+-------+
 *        ^                       |    ^
 *        |            a,t        |    |
 *      c |    +------------------+    | c
 *        |    |                       |
 *        |    |                       |
 *        |    v                 +-----+------+
 *    +---+--------+     a       | Committing |
 *    | Committing |------------>|    and     |
 *    +------------+             |  Writing   |
 *          ^                    +-----+------+
 *          |            c             |
 *          +--------------------------+
 * </pre>
 *
 * <p>Where {@code a} is the {@code Add} event, {@code t} is {@code Commit tick}, {@code c} is {@code Commit finished}.
 *
 * <p>The non-trivial transitions are the following:
 * <ol>
 *     <li>{@code Writing} -> {@code Committing} on {@code Add}: the buffer is overfilled,
 *     need to start committing regardless of the time.</li>
 *     <li>{@code Committing} -> {@code CommittingAndWriting} on {@code Add}: the commit is in progress,
 *     but the writer needs to handle coming data, so new {@link WriteSession} is created for this.</li>
 *     <li>{@code CommittingAndWriting} -> {@code Committing} on {@code Commit finished}: the current commit has finished,
 *     but the active {@link WriteSession} is filled or is old enough and needs committing as well.</li>
 *     <li>{@code CommittingAndWriting} -> {@code Writing} on {@code Commit finished}: the current commit has finished
 *     and the active {@link WriteSession} is not filled, so the writer continues just writing.</li>
 * </ol>
 */
public interface WriterState {
    /**
     * Add data.
     */
    AddResult add(Map<TopicPartition, MemoryRecords> entriesPerPartition);

    /**
     * Signal that it's time to commit.
     */
    CommitTickResult commitTick();

    /**
     * Signal that the current commit-in-progress has finished.
     */
    CommitFinishedResult commitFinished(List<CommitBatchResponse> commitBatchResponses,
                                        Throwable error);

    record StateContext(Time time,
                        CommitTickScheduler commitTickScheduler,
                        FileCommitter committer,
                        int maxBufferSize,
                        Duration commitInterval) {
        Instant now() {
            return Instant.ofEpochMilli(time.milliseconds());
        }
    }

    record AddResult(CompletableFuture<Map<TopicPartition, ProduceResponse.PartitionResponse>> result,
                     WriterState newState) {
    }

    record CommitTickResult(WriterState newState) {
    }

    record CommitFinishedResult(WriterState newState) {
    }
}
