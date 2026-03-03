package kafka.server

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.server.LeaderEndPoint
import org.apache.kafka.server.common.OffsetAndEpoch
import org.apache.kafka.storage.internals.log.LogAppendInfo

import java.util.Optional

class WALFetcherThread(name: String,
                       leader: LeaderEndPoint,
                       brokerConfig: KafkaConfig,
                       failedPartitions: FailedPartitions,
                       replicaMgr: ReplicaManager,
                       quota: ReplicaQuota,
                       logPrefix: String)
  extends AbstractFetcherThread(name = name,
                                clientId = name,
                                leader = leader,
                                failedPartitions,
                                fetchTierStateMachine = new TierStateMachine(leader, replicaMgr, false),
                                fetchBackOffMs = brokerConfig.replicaFetchBackoffMs,
                                isInterruptible = false,
                                replicaMgr.brokerTopicStats) {

  override protected def processPartitionData(topicPartition: TopicPartition, fetchOffset: Long, partitionLeaderEpoch: Int, partitionData: FetchData): Option[LogAppendInfo] = ???

  override protected def truncate(topicPartition: TopicPartition, truncationState: OffsetTruncationState): Unit = ???

  override protected def truncateFullyAndStartAt(topicPartition: TopicPartition, offset: Long): Unit = ???

  override protected def latestEpoch(topicPartition: TopicPartition): Optional[Integer] = ???

  override protected def logStartOffset(topicPartition: TopicPartition): Long = ???

  override protected def logEndOffset(topicPartition: TopicPartition): Long = ???

  override protected def endOffsetForEpoch(topicPartition: TopicPartition, epoch: Int): Optional[OffsetAndEpoch] = ???
}
