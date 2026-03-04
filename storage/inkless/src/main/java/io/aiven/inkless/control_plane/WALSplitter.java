package io.aiven.inkless.control_plane;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.record.MemoryRecords;

import java.util.Map;
import java.util.TreeMap;
import java.util.function.Supplier;

public interface WALSplitter extends Supplier<Map<TopicIdPartition, TreeMap<BatchInfo, MemoryRecords>>> {

    public void updateLastOffsets(Map<TopicIdPartition, Long> lastOffsets);
}
