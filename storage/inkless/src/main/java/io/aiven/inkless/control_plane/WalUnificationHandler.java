package io.aiven.inkless.control_plane;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.record.MemoryRecords;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Supplier;

public interface WalUnificationHandler
        extends Function<Set<TopicIdPartition>, Map<TopicIdPartition, TreeMap<BatchInfo, MemoryRecords>>> {

    void setRemoteLogEndOffsets(Map<TopicIdPartition, Long> lastOffsets);
    void setLastOffsets(Map<TopicIdPartition, Long> lastOffsets);
}
