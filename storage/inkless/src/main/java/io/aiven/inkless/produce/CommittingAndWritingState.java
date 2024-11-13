// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.ProduceResponse;

import io.aiven.inkless.control_plane.CommitBatchResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The committing-and-writing state of {@link Writer}.
 *
 * <p>There's a session being committed and at the same time another session accepts writes.
 * An {@code Add} event will be handled as in {@link WritingState},
 * but cannot trigger another commit immediately by session size.
 * {@code Commit tick} events are ignored.
 * A {@code Commit finished} event will mark the end of the commit process
 * and potentially start another commit process with the currently active session.
 */
class CommittingAndWritingState extends AbstractWriterState {
    private static final Logger LOGGER = LoggerFactory.getLogger(CommittingAndWritingState.class);

    private final WriteSession sessionBeingCommitted;
    private final WriteSession activeSession;

    CommittingAndWritingState(final StateContext config,
                              final WriteSession sessionBeingCommitted) {
        super(config);
        this.activeSession = new WriteSession(config.now());
        this.sessionBeingCommitted = sessionBeingCommitted;
    }

    @Override
    public AddResult add(final Map<TopicPartition, MemoryRecords> entriesPerPartition) {
        final CompletableFuture<Map<TopicPartition, ProduceResponse.PartitionResponse>> addResult =
            activeSession.add(entriesPerPartition);

        // Regardless if the current session can add more, we stay in the current state.
        return new AddResult(addResult, this);
    }

    @Override
    public CommitFinishedResult commitFinished(final List<CommitBatchResponse> commitBatchResponses,
                                               final Throwable error) {
        sessionBeingCommitted.finishCommit(commitBatchResponses, error);
        if (SessionUtils.canSessionAddMoreData(activeSession, context)
            && !SessionUtils.hasSessionReachedCommitTimeout(activeSession, context)) {
            LOGGER.debug("CommitFinished received, current session can accept more data, transferring into Writing");
            return new CommitFinishedResult(new WritingState(context, activeSession));
        } else {
            LOGGER.debug("CommitFinished received, current session can not accept more data, transferring into Committing");
            return new CommitFinishedResult(new CommittingState(context, activeSession));
        }
    }
}
