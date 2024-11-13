// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.ProduceResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The writing state of {@link Writer}.
 *
 * <p>There's an active write session open and there's at least one batch in it.
 * A {@code Commit tick} event will trigger the commit.
 * {@code Commit finished} events are ignored.
 */
class WritingState extends AbstractWriterState {
    private static final Logger LOGGER = LoggerFactory.getLogger(WritingState.class);

    private final WriteSession activeSession;

    WritingState(final StateContext config) {
        this(config, new WriteSession(config.now()));
    }

    WritingState(final StateContext config,
                 final WriteSession activeSession) {
        super(config);
        this.activeSession = activeSession;
        config.commitTickScheduler().schedule(activeSession.sessionStarted(), this);
    }

    @Override
    public AddResult add(final Map<TopicPartition, MemoryRecords> entriesPerPartition) {
        final CompletableFuture<Map<TopicPartition, ProduceResponse.PartitionResponse>> addResult =
            activeSession.add(entriesPerPartition);

        // If the session is full, commit immediately.
        if (SessionUtils.canSessionAddMoreData(activeSession, context)) {
            return new AddResult(addResult, this);
        } else {
            return new AddResult(addResult, new CommittingState(context, activeSession));
        }
    }

    @Override
    public CommitTickResult commitTick() {
        LOGGER.debug("CommitTick received, transferring into Committing");
        return new CommitTickResult(new CommittingState(context, activeSession));
    }
}
