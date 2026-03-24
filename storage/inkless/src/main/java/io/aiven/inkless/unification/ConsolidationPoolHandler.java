package io.aiven.inkless.unification;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.server.common.OffsetAndEpoch;
import org.apache.kafka.server.util.ShutdownableThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

public class ConsolidationPoolHandler extends ShutdownableThread {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsolidationPoolHandler.class);

    public ConsolidationPoolHandler() {
        super("ConsolidationPoolHandler", true);
    }

    public void addOffsetsToPartition(TopicIdPartition topicIdPartition, OffsetAndEpoch offsetAndMetadata) {
        // TODO: provide real implementation
        LOGGER.info("Adding partition {} to the pool", topicIdPartition);
    }

    public void removePartition(TopicIdPartition topicIdPartition) {
        // TODO: provide real implementation
        LOGGER.info("Removing partition {} from the pool", topicIdPartition);
    }

    public Set<TopicIdPartition> currentPartitions() {
        // TODO: provide real implementation
        return Set.of();
    }

    @Override
    public void doWork() {
        // TODO: provide real implementation
    }
}
