// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.storage.internals.log.BatchMetadata;
import org.apache.kafka.storage.internals.log.ProducerStateEntry;

import java.util.List;

public record FindProducerStateResponse(TopicPartition topicPartition, long producerId, short producerEpoch, List<BatchMetadata> batches) {
    public static FindProducerStateResponse empty(FindProducerStateRequest request) {
        return new FindProducerStateResponse(request.topicIdPartition().topicPartition(), request.producerId(), request.producerEpoch(), List.of());
    }

    public static FindProducerStateResponse of(FindProducerStateRequest request, List<BatchMetadata> batches) {
        return new FindProducerStateResponse(request.topicIdPartition().topicPartition(), request.producerId(), request.producerEpoch(), batches);
    }

    public ProducerStateEntry entry() {
        ProducerStateEntry entry = ProducerStateEntry.empty(producerId);
        entry.maybeUpdateProducerEpoch(producerEpoch);
        for (BatchMetadata batch : batches) {
            entry.addBatch(producerEpoch, batch.lastSeq, batch.lastOffset, batch.offsetDelta, batch.timestamp);
        }
        return entry;
    }
}
