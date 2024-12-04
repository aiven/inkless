package io.aiven.inkless.produce;

import org.apache.kafka.storage.internals.log.BatchMetadata;

public record DuplicatedBatchMetadata(long originalBaseOffset, BatchMetadata batchMetadata) {
}
