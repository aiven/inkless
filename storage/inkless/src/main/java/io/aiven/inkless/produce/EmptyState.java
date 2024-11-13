// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.MemoryRecords;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The empty state of {@link Writer}.
 *
 * <p>In this state, the {@link Writer} is passively waits for new data
 * and ignores errant {@code Commit tick} or {@code Commit finished} events.
 */
class EmptyState extends AbstractWriterState {
    private static final Logger LOGGER = LoggerFactory.getLogger(WritingState.class);

    EmptyState(final StateContext config) {
        super(config);
    }

    @Override
    public AddResult add(final Map<TopicPartition, MemoryRecords> entriesPerPartition) {
        LOGGER.debug("Add received, transferring into Writing");
        return new WritingState(context).add(entriesPerPartition);
    }
}
