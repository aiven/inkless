package io.aiven.inkless.cache;

import org.apache.kafka.common.TopicIdPartition;

import java.io.IOException;

/**
 * A Batch Coordinate cache implementation that always misses.
 */
public class NullBatchCoordinateCache implements BatchCoordinateCache {

    @Override
    public LogFragment get(TopicIdPartition topicIdPartition, long offset) {
        return null;
    }

    @Override
    public void put(TopicIdPartition topicIdPartition, CacheBatchCoordinate cacheBatchCoordinate) {}

    @Override
    public void close() throws IOException {}
}
