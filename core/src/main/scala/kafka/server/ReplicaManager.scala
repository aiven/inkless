/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.server

import com.yammer.metrics.core.Meter
import io.aiven.inkless.common.SharedState
import io.aiven.inkless.consume.{FetchHandler, FetchOffsetHandler}
import io.aiven.inkless.control_plane.{FindBatchRequest, FindBatchResponse}
import io.aiven.inkless.delete.{DeleteRecordsInterceptor, FileCleaner, RetentionEnforcer}
import io.aiven.inkless.merge.FileMerger
import io.aiven.inkless.produce.AppendHandler
import kafka.cluster.{Partition, PartitionListener}
import kafka.controller.StateChangeLogger
import kafka.log.LogManager
import kafka.server.HostedPartition.Online
import kafka.server.QuotaFactory.QuotaManagers
import kafka.server.ReplicaManager.{AtMinIsrPartitionCountMetricName, FailedIsrUpdatesPerSecMetricName, IsrExpandsPerSecMetricName, IsrShrinksPerSecMetricName, LeaderCountMetricName, OfflineReplicaCountMetricName, PartitionCountMetricName, PartitionsWithLateTransactionsCountMetricName, ProducerIdCountMetricName, ReassigningPartitionsMetricName, UnderMinIsrPartitionCountMetricName, UnderReplicatedPartitionsMetricName, createLogReadResult, isListOffsetsTimestampUnsupported}
import kafka.server.share.DelayedShareFetch
import kafka.utils._
import org.apache.kafka.common.{IsolationLevel, Node, TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.common.errors._
import org.apache.kafka.common.internals.{Plugin, Topic}
import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsPartitionResult
import org.apache.kafka.common.message.DescribeLogDirsResponseData.DescribeLogDirsTopic
import org.apache.kafka.common.message.ListOffsetsRequestData.{ListOffsetsPartition, ListOffsetsTopic}
import org.apache.kafka.common.message.ListOffsetsResponseData.{ListOffsetsPartitionResponse, ListOffsetsTopicResponse}
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderTopic
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.{EpochEndOffset, OffsetForLeaderTopicResult}
import org.apache.kafka.common.message.{DescribeLogDirsResponseData, DescribeProducersResponseData}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record._
import org.apache.kafka.common.replica.PartitionView.DefaultPartitionView
import org.apache.kafka.common.replica.ReplicaView.DefaultReplicaView
import org.apache.kafka.common.replica._
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.requests._
import org.apache.kafka.common.utils.{Exit, Time, Utils}
import org.apache.kafka.coordinator.transaction.{AddPartitionsToTxnConfig, TransactionLogConfig}
import org.apache.kafka.image.{LocalReplicaChanges, MetadataImage, TopicsDelta}
import org.apache.kafka.metadata.LeaderAndIsr
import org.apache.kafka.metadata.LeaderConstants.NO_LEADER
import org.apache.kafka.metadata.MetadataCache
import org.apache.kafka.server.common.{DirectoryEventHandler, RequestLocal, StopPartition}
import org.apache.kafka.server.log.remote.TopicPartitionLog
import org.apache.kafka.server.config.ReplicationConfigs
import org.apache.kafka.server.log.remote.storage.RemoteLogManager
import org.apache.kafka.server.metrics.KafkaMetricsGroup
import org.apache.kafka.server.network.BrokerEndPoint
import org.apache.kafka.server.purgatory.{DelayedDeleteRecords, DelayedOperationPurgatory, DelayedRemoteListOffsets, DeleteRecordsPartitionStatus, ListOffsetsPartitionStatus, TopicPartitionOperationKey}
import org.apache.kafka.server.share.fetch.{DelayedShareFetchKey, DelayedShareFetchPartitionKey}
import org.apache.kafka.server.storage.log.{FetchParams, FetchPartitionData}
import org.apache.kafka.server.transaction.AddPartitionsToTxnManager
import org.apache.kafka.server.transaction.AddPartitionsToTxnManager.TransactionSupportedOperation
import org.apache.kafka.server.util.timer.{SystemTimer, TimerTask}
import org.apache.kafka.server.util.{Scheduler, ShutdownableThread}
import org.apache.kafka.server.{ActionQueue, DelayedActionQueue, LogReadResult, common}
import org.apache.kafka.storage.internals.checkpoint.{LazyOffsetCheckpoints, OffsetCheckpointFile, OffsetCheckpoints}
import org.apache.kafka.storage.internals.log.{AppendOrigin, AsyncOffsetReadFutureHolder, FetchDataInfo, LeaderHwChange, LogAppendInfo, LogConfig, LogDirFailureChannel, LogOffsetMetadata, LogReadInfo, OffsetResultHolder, RecordValidationException, RemoteLogReadResult, RemoteStorageFetchInfo, UnifiedLog, VerificationGuard}
import org.apache.kafka.storage.log.metrics.BrokerTopicStats

import java.io.File
import java.lang.{Long => JLong}
import java.nio.file.{Files, Paths}
import java.util
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{CompletableFuture, ConcurrentHashMap, Future, RejectedExecutionException, TimeUnit}
import java.util.{Collections, Optional, OptionalInt, OptionalLong}
import java.util.function.Consumer
import java.util.stream.Collectors
import scala.collection.{Map, Seq, Set, immutable, mutable}
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters.RichOptional

/*
 * Result metadata of a log append operation on the log
 */
case class LogAppendResult(info: LogAppendInfo,
                           exception: Option[Throwable],
                           hasCustomErrorMessage: Boolean) {
  def error: Errors = exception match {
    case None => Errors.NONE
    case Some(e) => Errors.forException(e)
  }

  def errorMessage: String = {
    exception match {
      case Some(e) if hasCustomErrorMessage => e.getMessage
      case _ => null
    }
  }
}

case class LogDeleteRecordsResult(requestedOffset: Long, lowWatermark: Long, exception: Option[Throwable] = None) {
  def error: Errors = exception match {
    case None => Errors.NONE
    case Some(e) => Errors.forException(e)
  }
}

/**
 * Trait to represent the state of hosted partitions. We create a concrete (active) Partition
 * instance when the broker receives a LeaderAndIsr request from the controller or a metadata
 * log record from the Quorum controller indicating that the broker should be either a leader
 * or follower of a partition.
 */
sealed trait HostedPartition

object HostedPartition {
  /**
   * This broker does not have any state for this partition locally.
   */
  final object None extends HostedPartition

  /**
   * This broker hosts the partition and it is online.
   */
  final case class Online(partition: Partition) extends HostedPartition

  /**
   * This broker hosts the partition, but it is in an offline log directory.
   */
  final case class Offline(partition: Option[Partition]) extends HostedPartition
}

object ReplicaManager {
  val HighWatermarkFilename = "replication-offset-checkpoint"

  private val LeaderCountMetricName = "LeaderCount"
  private val PartitionCountMetricName = "PartitionCount"
  private val OfflineReplicaCountMetricName = "OfflineReplicaCount"
  private val UnderReplicatedPartitionsMetricName = "UnderReplicatedPartitions"
  private val UnderMinIsrPartitionCountMetricName = "UnderMinIsrPartitionCount"
  private val AtMinIsrPartitionCountMetricName = "AtMinIsrPartitionCount"
  private val ReassigningPartitionsMetricName = "ReassigningPartitions"
  private val PartitionsWithLateTransactionsCountMetricName = "PartitionsWithLateTransactionsCount"
  private val ProducerIdCountMetricName = "ProducerIdCount"
  private val IsrExpandsPerSecMetricName = "IsrExpandsPerSec"
  private val IsrShrinksPerSecMetricName = "IsrShrinksPerSec"
  private val FailedIsrUpdatesPerSecMetricName = "FailedIsrUpdatesPerSec"

  private[server] val GaugeMetricNames = Set(
    LeaderCountMetricName,
    PartitionCountMetricName,
    OfflineReplicaCountMetricName,
    UnderReplicatedPartitionsMetricName,
    UnderMinIsrPartitionCountMetricName,
    AtMinIsrPartitionCountMetricName,
    ReassigningPartitionsMetricName,
    PartitionsWithLateTransactionsCountMetricName,
    ProducerIdCountMetricName
  )

  private[server] val MeterMetricNames = Set(
    IsrExpandsPerSecMetricName,
    IsrShrinksPerSecMetricName,
    FailedIsrUpdatesPerSecMetricName
  )

  private[server] val MetricNames = GaugeMetricNames.union(MeterMetricNames)

  private val timestampMinSupportedVersion: immutable.Map[Long, Short] = immutable.Map[Long, Short](
    ListOffsetsRequest.EARLIEST_TIMESTAMP -> 1.toShort,
    ListOffsetsRequest.LATEST_TIMESTAMP -> 1.toShort,
    ListOffsetsRequest.MAX_TIMESTAMP -> 7.toShort,
    ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP -> 8.toShort,
    ListOffsetsRequest.LATEST_TIERED_TIMESTAMP -> 9.toShort
  )

  def createLogReadResult(highWatermark: Long,
                          leaderLogStartOffset: Long,
                          leaderLogEndOffset: Long,
                          e: Throwable): LogReadResult = {
    new LogReadResult(new FetchDataInfo(LogOffsetMetadata.UNKNOWN_OFFSET_METADATA, MemoryRecords.EMPTY),
      Optional.empty(),
      highWatermark,
      leaderLogStartOffset,
      leaderLogEndOffset,
      -1L,
      -1L,
      OptionalLong.empty(),
      Optional.of(e))
  }

  def createLogReadResult(e: Throwable): LogReadResult = {
    new LogReadResult(new FetchDataInfo(LogOffsetMetadata.UNKNOWN_OFFSET_METADATA, MemoryRecords.EMPTY),
      Optional.empty(),
      UnifiedLog.UNKNOWN_OFFSET,
      UnifiedLog.UNKNOWN_OFFSET,
      UnifiedLog.UNKNOWN_OFFSET,
      UnifiedLog.UNKNOWN_OFFSET,
      -1L,
      OptionalLong.empty(),
      Optional.of(e))
  }

  private[server] def isListOffsetsTimestampUnsupported(timestamp: JLong, version: Short): Boolean = {
    timestamp < 0 &&
      (!timestampMinSupportedVersion.contains(timestamp) || version < timestampMinSupportedVersion(timestamp))
  }
}

class ReplicaManager(val config: KafkaConfig,
                     metrics: Metrics,
                     time: Time,
                     scheduler: Scheduler,
                     val logManager: LogManager,
                     val remoteLogManager: Option[RemoteLogManager] = None,
                     quotaManagers: QuotaManagers,
                     val metadataCache: MetadataCache,
                     logDirFailureChannel: LogDirFailureChannel,
                     val alterPartitionManager: AlterPartitionManager,
                     val brokerTopicStats: BrokerTopicStats = new BrokerTopicStats(),
                     val isShuttingDown: AtomicBoolean = new AtomicBoolean(false),
                     delayedProducePurgatoryParam: Option[DelayedOperationPurgatory[DelayedProduce]] = None,
                     delayedFetchPurgatoryParam: Option[DelayedOperationPurgatory[DelayedFetch]] = None,
                     delayedDeleteRecordsPurgatoryParam: Option[DelayedOperationPurgatory[DelayedDeleteRecords]] = None,
                     delayedRemoteFetchPurgatoryParam: Option[DelayedOperationPurgatory[DelayedRemoteFetch]] = None,
                     delayedRemoteListOffsetsPurgatoryParam: Option[DelayedOperationPurgatory[DelayedRemoteListOffsets]] = None,
                     delayedShareFetchPurgatoryParam: Option[DelayedOperationPurgatory[DelayedShareFetch]] = None,
                     threadNamePrefix: Option[String] = None,
                     val brokerEpochSupplier: () => Long = () => -1,
                     addPartitionsToTxnManager: Option[AddPartitionsToTxnManager] = None,
                     val directoryEventHandler: DirectoryEventHandler = DirectoryEventHandler.NOOP,
                     val defaultActionQueue: ActionQueue = new DelayedActionQueue,
                     inklessSharedState: Option[SharedState] = None
                     ) extends Logging {
  private val metricsGroup = new KafkaMetricsGroup(this.getClass)
  private val addPartitionsToTxnConfig = new AddPartitionsToTxnConfig(config)
  private val shareFetchPurgatoryName = "ShareFetch"
  private val delayedShareFetchTimer = new SystemTimer(shareFetchPurgatoryName)

  val delayedProducePurgatory = delayedProducePurgatoryParam.getOrElse(
    new DelayedOperationPurgatory[DelayedProduce](
      "Produce", config.brokerId,
      config.producerPurgatoryPurgeIntervalRequests))
  val delayedFetchPurgatory = delayedFetchPurgatoryParam.getOrElse(
    new DelayedOperationPurgatory[DelayedFetch](
      "Fetch", config.brokerId,
      config.fetchPurgatoryPurgeIntervalRequests))
  val delayedDeleteRecordsPurgatory = delayedDeleteRecordsPurgatoryParam.getOrElse(
    new DelayedOperationPurgatory[DelayedDeleteRecords](
      "DeleteRecords", config.brokerId,
      config.deleteRecordsPurgatoryPurgeIntervalRequests))
  val delayedRemoteFetchPurgatory = delayedRemoteFetchPurgatoryParam.getOrElse(
    new DelayedOperationPurgatory[DelayedRemoteFetch](
      "RemoteFetch", config.brokerId))
  val delayedRemoteListOffsetsPurgatory = delayedRemoteListOffsetsPurgatoryParam.getOrElse(
    new DelayedOperationPurgatory[DelayedRemoteListOffsets](
      "RemoteListOffsets", config.brokerId))
  val delayedShareFetchPurgatory = delayedShareFetchPurgatoryParam.getOrElse(
    new DelayedOperationPurgatory[DelayedShareFetch](
      shareFetchPurgatoryName, delayedShareFetchTimer, config.brokerId,
      config.shareGroupConfig.shareFetchPurgatoryPurgeIntervalRequests))

  private val inklessAppendHandler: Option[AppendHandler] = inklessSharedState.map(new AppendHandler(_))
  private val inklessFetchHandler: Option[FetchHandler] = inklessSharedState.map(new FetchHandler(_))
  private val inklessFetchOffsetHandler: Option[FetchOffsetHandler] = inklessSharedState.map(new FetchOffsetHandler(_))
  private val inklessDeleteRecordsInterceptor: Option[DeleteRecordsInterceptor] = inklessSharedState.map(new DeleteRecordsInterceptor(_))
  private val inklessRetentionEnforcer: Option[RetentionEnforcer] = inklessSharedState.map(new RetentionEnforcer(_))
  private val inklessFileCleaner: Option[FileCleaner] = inklessSharedState.map(new FileCleaner(_))
  // FIXME: FileMerger is having issues with hanging queries. Disabling until fixed.
  private val inklessFileMerger: Option[FileMerger] = None // inklessSharedState.map(new FileMerger(_))

  /* epoch of the controller that last changed the leader */
  @volatile private[server] var controllerEpoch: Int = 0
  protected val localBrokerId = config.brokerId
  protected val allPartitions = new ConcurrentHashMap[TopicPartition, HostedPartition]
  private val replicaStateChangeLock = new Object
  val replicaFetcherManager = createReplicaFetcherManager(metrics, time, threadNamePrefix, quotaManagers.follower)
  private[server] val replicaAlterLogDirsManager = createReplicaAlterLogDirsManager(quotaManagers.alterLogDirs, brokerTopicStats)
  private val highWatermarkCheckPointThreadStarted = new AtomicBoolean(false)
  @volatile private[server] var highWatermarkCheckpoints: Map[String, OffsetCheckpointFile] = logManager.liveLogDirs.map(dir =>
    (dir.getAbsolutePath, new OffsetCheckpointFile(new File(dir, ReplicaManager.HighWatermarkFilename), logDirFailureChannel))).toMap

  @volatile private var isInControlledShutdown = false

  this.logIdent = s"[ReplicaManager broker=$localBrokerId] "
  protected val stateChangeLogger = new StateChangeLogger(localBrokerId, inControllerContext = false, None)

  private var logDirFailureHandler: LogDirFailureHandler = _

  private class LogDirFailureHandler(name: String) extends ShutdownableThread(name) {
    override def doWork(): Unit = {
      val newOfflineLogDir = logDirFailureChannel.takeNextOfflineLogDir()
      handleLogDirFailure(newOfflineLogDir)
    }
  }

  // Visible for testing
  private[server] val replicaSelectorPlugin: Option[Plugin[ReplicaSelector]] = createReplicaSelector(metrics)

  metricsGroup.newGauge(LeaderCountMetricName, () => leaderPartitionsIterator.size)
  // Visible for testing
  private[kafka] val partitionCount = metricsGroup.newGauge(PartitionCountMetricName, () => allPartitions.size)
  metricsGroup.newGauge(OfflineReplicaCountMetricName, () => offlinePartitionCount)
  metricsGroup.newGauge(UnderReplicatedPartitionsMetricName, () => underReplicatedPartitionCount)
  metricsGroup.newGauge(UnderMinIsrPartitionCountMetricName, () => leaderPartitionsIterator.count(_.isUnderMinIsr))
  metricsGroup.newGauge(AtMinIsrPartitionCountMetricName, () => leaderPartitionsIterator.count(_.isAtMinIsr))
  metricsGroup.newGauge(ReassigningPartitionsMetricName, () => reassigningPartitionsCount)
  metricsGroup.newGauge(PartitionsWithLateTransactionsCountMetricName, () => lateTransactionsCount)
  metricsGroup.newGauge(ProducerIdCountMetricName, () => producerIdCount)

  private def reassigningPartitionsCount: Int = leaderPartitionsIterator.count(_.isReassigning)

  private def lateTransactionsCount: Int = {
    val currentTimeMs = time.milliseconds()
    leaderPartitionsIterator.count(_.hasLateTransaction(currentTimeMs))
  }

  def producerIdCount: Int = onlinePartitionsIterator.map(_.producerIdCount).sum

  val isrExpandRate: Meter = metricsGroup.newMeter(IsrExpandsPerSecMetricName, "expands", TimeUnit.SECONDS)
  val isrShrinkRate: Meter = metricsGroup.newMeter(IsrShrinksPerSecMetricName, "shrinks", TimeUnit.SECONDS)
  val failedIsrUpdatesRate: Meter = metricsGroup.newMeter(FailedIsrUpdatesPerSecMetricName, "failedUpdates", TimeUnit.SECONDS)

  def underReplicatedPartitionCount: Int = leaderPartitionsIterator.count(_.isUnderReplicated)

  def startHighWatermarkCheckPointThread(): Unit = {
    if (highWatermarkCheckPointThreadStarted.compareAndSet(false, true))
      scheduler.schedule("highwatermark-checkpoint", () => checkpointHighWatermarks(), 0L, config.replicaHighWatermarkCheckpointIntervalMs)
  }

  // When ReplicaAlterDirThread finishes replacing a current replica with a future replica, it will
  // remove the partition from the partition state map. But it will not close itself even if the
  // partition state map is empty. Thus we need to call shutdownIdleReplicaAlterDirThread() periodically
  // to shutdown idle ReplicaAlterDirThread
  private def shutdownIdleReplicaAlterLogDirsThread(): Unit = {
    replicaAlterLogDirsManager.shutdownIdleFetcherThreads()
  }

  def resizeFetcherThreadPool(newSize: Int): Unit = {
    replicaFetcherManager.resizeThreadPool(newSize)
  }

  def getLog(topicPartition: TopicPartition): Option[UnifiedLog] = logManager.getLog(topicPartition)

  def startup(): Unit = {
    // start ISR expiration thread
    // A follower can lag behind leader for up to config.replicaLagTimeMaxMs x 1.5 before it is removed from ISR
    scheduler.schedule("isr-expiration", () => maybeShrinkIsr(), 0L, config.replicaLagTimeMaxMs / 2)
    scheduler.schedule("shutdown-idle-replica-alter-log-dirs-thread", () => shutdownIdleReplicaAlterLogDirsThread(), 0L, 10000L)

    logDirFailureHandler = new LogDirFailureHandler("LogDirFailureHandler")
    logDirFailureHandler.start()
    addPartitionsToTxnManager.foreach(_.start())
    remoteLogManager.foreach(rlm => rlm.setDelayedOperationPurgatory(delayedRemoteListOffsetsPurgatory))

    // Inkless threads
    inklessSharedState.map { sharedState =>
      scheduler.schedule("inkless-retention-enforcer", () => inklessRetentionEnforcer.foreach(_.run()), config.logInitialTaskDelayMs, 500L)  // the real interval is inside

      scheduler.schedule("inkless-file-cleaner", () => inklessFileCleaner.foreach(_.run()), sharedState.config().fileCleanerInterval().toMillis, sharedState.config().fileCleanerInterval().toMillis)

      // There are internal delays in case of errors or absence of work items, no need for extra delays here.
      scheduler.schedule("inkless-file-merger", () => inklessFileMerger.foreach(_.run()), sharedState.config().fileMergerInterval().toMillis, sharedState.config().fileMergerInterval().toMillis)
    }
  }

  private def maybeRemoveTopicMetrics(topic: String): Unit = {
    val topicHasNonOfflinePartition = allPartitions.values.asScala.exists {
      case online: HostedPartition.Online => topic == online.partition.topic
      case HostedPartition.None | HostedPartition.Offline(_) => false
    }
    if (!topicHasNonOfflinePartition) // nothing online or deferred
      brokerTopicStats.removeMetrics(topic)
  }

  private def completeDelayedOperationsWhenNotPartitionLeader(topicPartition: TopicPartition, topicId: Option[Uuid]): Unit = {
    val topicPartitionOperationKey = new TopicPartitionOperationKey(topicPartition)
    delayedProducePurgatory.checkAndComplete(topicPartitionOperationKey)
    delayedFetchPurgatory.checkAndComplete(topicPartitionOperationKey)
    delayedRemoteFetchPurgatory.checkAndComplete(topicPartitionOperationKey)
    delayedRemoteListOffsetsPurgatory.checkAndComplete(topicPartitionOperationKey)
    if (topicId.isDefined) delayedShareFetchPurgatory.checkAndComplete(
      new DelayedShareFetchPartitionKey(topicId.get, topicPartition.partition()))
  }

  /**
   * Complete any local follower fetches that have been unblocked since new data is available
   * from the leader for one or more partitions. Should only be called by ReplicaFetcherThread
   * after successfully replicating from the leader.
   */
  private[server] def completeDelayedFetchRequests(topicPartitions: Seq[TopicPartition]): Unit = {
    topicPartitions.foreach(tp => delayedFetchPurgatory.checkAndComplete(new TopicPartitionOperationKey(tp)))
  }

  /**
   * Complete any delayed share fetch requests that have been unblocked since new data is available from the leader
   * for one of the partitions. This could happen due to acknowledgements, acquisition lock timeout of records, partition
   * locks getting freed and release of acquired records due to share session close.
   * @param delayedShareFetchKey The key corresponding to which the share fetch request has been stored in the purgatory
   */
  private[server] def completeDelayedShareFetchRequest(delayedShareFetchKey: DelayedShareFetchKey): Unit = {
    delayedShareFetchPurgatory.checkAndComplete(delayedShareFetchKey)
  }

  /**
   * Add and watch a share fetch request in the delayed share fetch purgatory corresponding to a set of keys in case it cannot be
   * completed instantaneously, otherwise complete it.
   * @param delayedShareFetch Refers to the DelayedOperation over share fetch request
   * @param delayedShareFetchKeys The keys corresponding to which the delayed share fetch request will be stored in the purgatory
   */
  private[server] def addDelayedShareFetchRequest(delayedShareFetch: DelayedShareFetch,
                                                  delayedShareFetchKeys : util.List[DelayedShareFetchKey]): Unit = {
    delayedShareFetchPurgatory.tryCompleteElseWatch(delayedShareFetch, delayedShareFetchKeys)
  }

  /**
   * Add a timer task to the delayedShareFetchTimer.
   * @param timerTask The timer task to be added to the delayedShareFetchTimer
   */
  private[server] def addShareFetchTimerRequest(timerTask: TimerTask): Unit = {
    delayedShareFetchTimer.add(timerTask)
  }

  /**
   * Registers the provided listener to the partition iff the partition is online.
   */
  def maybeAddListener(partition: TopicPartition, listener: PartitionListener): Boolean = {
    getPartition(partition) match {
      case HostedPartition.Online(partition) =>
        partition.maybeAddListener(listener)
      case _ =>
        false
    }
  }

  /**
   * Removes the provided listener from the partition.
   */
  def removeListener(partition: TopicPartition, listener: PartitionListener): Unit = {
    getPartition(partition) match {
      case HostedPartition.Online(partition) =>
        partition.removeListener(listener)
      case _ => // Ignore
    }
  }

  /**
   * Stop the given partitions.
   *
   * @param partitionsToStop set of topic-partitions to be stopped which also indicates whether to remove the
   *                         partition data from the local and remote log storage.
   *
   * @return                 A map from partitions to exceptions which occurred.
   *                         If no errors occurred, the map will be empty.
   */
  private def stopPartitions(partitionsToStop: Set[StopPartition]): Map[TopicPartition, Throwable] = {
    // First stop fetchers for all partitions.
    val partitions = partitionsToStop.map(_.topicPartition)
    replicaFetcherManager.removeFetcherForPartitions(partitions)
    replicaAlterLogDirsManager.removeFetcherForPartitions(partitions)

    // Second remove deleted partitions from the partition map. Fetchers rely on the
    // ReplicaManager to get Partition's information so they must be stopped first.
    val partitionsToDelete = mutable.Set.empty[TopicPartition]
    partitionsToStop.foreach { stopPartition =>
      val topicPartition = stopPartition.topicPartition
      var topicId: Option[Uuid] = None
      if (stopPartition.deleteLocalLog) {
        getPartition(topicPartition) match {
          case hostedPartition: HostedPartition.Online =>
            if (allPartitions.remove(topicPartition, hostedPartition)) {
              maybeRemoveTopicMetrics(topicPartition.topic)
              // Logs are not deleted here. They are deleted in a single batch later on.
              // This is done to avoid having to checkpoint for every deletions.
              hostedPartition.partition.delete()
              topicId = hostedPartition.partition.topicId
            }

          case _ =>
        }
        partitionsToDelete += topicPartition
      }
      // If we were the leader, we may have some operations still waiting for completion.
      // We force completion to prevent them from timing out.
      completeDelayedOperationsWhenNotPartitionLeader(topicPartition, topicId)
    }

    // Third delete the logs and checkpoint.
    val errorMap = new mutable.HashMap[TopicPartition, Throwable]()
    val remotePartitionsToStop = partitionsToStop.filter {
      sp => logManager.getLog(sp.topicPartition).exists(unifiedLog => unifiedLog.remoteLogEnabled())
    }
    if (partitionsToDelete.nonEmpty) {
      // Delete the logs and checkpoint.
      logManager.asyncDelete(partitionsToDelete, isStray = false, (tp, e) => errorMap.put(tp, e))
    }
    remoteLogManager.foreach { rlm =>
      // exclude the partitions with offline/error state
      val partitions = remotePartitionsToStop.filterNot(sp => errorMap.contains(sp.topicPartition)).toSet.asJava
      if (!partitions.isEmpty) {
        rlm.stopPartitions(partitions, (tp, e) => errorMap.put(tp, e))
      }
    }
    errorMap
  }

  def topicIdPartition(topicPartition: TopicPartition): TopicIdPartition = {
    val topicId = metadataCache.getTopicId(topicPartition.topic())
    new TopicIdPartition(topicId, topicPartition)
  }

  def getPartition(topicPartition: TopicPartition): HostedPartition = {
    Option(allPartitions.get(topicPartition)).getOrElse(HostedPartition.None)
  }

  def isAddingReplica(topicPartition: TopicPartition, replicaId: Int): Boolean = {
    getPartition(topicPartition) match {
      case Online(partition) => partition.isAddingReplica(replicaId)
      case _ => false
    }
  }

  // Visible for testing
  def createPartition(topicPartition: TopicPartition): Partition = {
    val partition = Partition(topicPartition, time, this)
    allPartitions.put(topicPartition, HostedPartition.Online(partition))
    partition
  }

  def onlinePartition(topicPartition: TopicPartition): Option[Partition] = {
    getPartition(topicPartition) match {
      case HostedPartition.Online(partition) => Some(partition)
      case _ => None
    }
  }

  // An iterator over all non offline partitions. This is a weakly consistent iterator; a partition made offline after
  // the iterator has been constructed could still be returned by this iterator.
  private def onlinePartitionsIterator: Iterator[Partition] = {
    allPartitions.values.asScala.iterator.flatMap {
      case HostedPartition.Online(partition) => Some(partition)
      case _ => None
    }
  }

  private def offlinePartitionCount: Int = {
    allPartitions.values.asScala.iterator.count(_.getClass == HostedPartition.Offline.getClass)
  }

  def getPartitionOrException(topicPartition: TopicPartition): Partition = {
    getPartitionOrError(topicPartition) match {
      case Left(Errors.KAFKA_STORAGE_ERROR) =>
        throw new KafkaStorageException(s"Partition $topicPartition is in an offline log directory")

      case Left(error) =>
        throw error.exception(s"Error while fetching partition state for $topicPartition")

      case Right(partition) => partition
    }
  }

  def getPartitionOrException(topicIdPartition: TopicIdPartition): Partition = {
    getPartitionOrError(topicIdPartition.topicPartition()) match {
      case Left(Errors.KAFKA_STORAGE_ERROR) =>
        throw new KafkaStorageException(s"Partition ${topicIdPartition.topicPartition()} is in an offline log directory")

      case Left(error) =>
        throw error.exception(s"Error while fetching partition state for ${topicIdPartition.topicPartition()}")

      case Right(partition) =>
        // Get topic id for an existing partition from disk if topicId is none get it from the metadata cache
        val topicId = partition.topicId.getOrElse(metadataCache.getTopicId(topicIdPartition.topic()))
        // If topic id is set to zero_uuid fall back to non topic id aware behaviour
        val topicIdNotProvided = topicIdPartition.topicId() == Uuid.ZERO_UUID
        if (topicIdNotProvided || topicId == topicIdPartition.topicId()) {
          partition
        } else {
          throw new UnknownTopicIdException(s"Partition $topicIdPartition's topic id doesn't match the one on disk $topicId.'")
        }
    }
  }

  def getPartitionOrError(topicPartition: TopicPartition): Either[Errors, Partition] = {
    getPartition(topicPartition) match {
      case HostedPartition.Online(partition) =>
        Right(partition)

      case HostedPartition.Offline(_) =>
        Left(Errors.KAFKA_STORAGE_ERROR)

      case HostedPartition.None if metadataCache.contains(topicPartition) =>
        // The topic exists, but this broker is no longer a replica of it, so we return NOT_LEADER_OR_FOLLOWER which
        // forces clients to refresh metadata to find the new location. This can happen, for example,
        // during a partition reassignment if a produce request from the client is sent to a broker after
        // the local replica has been deleted.
        Left(Errors.NOT_LEADER_OR_FOLLOWER)

      case HostedPartition.None =>
        Left(Errors.UNKNOWN_TOPIC_OR_PARTITION)
    }
  }

  def localLogOrException(topicPartition: TopicPartition): UnifiedLog = {
    getPartitionOrException(topicPartition).localLogOrException
  }

  def futureLocalLogOrException(topicPartition: TopicPartition): UnifiedLog = {
    getPartitionOrException(topicPartition).futureLocalLogOrException
  }

  def futureLogExists(topicPartition: TopicPartition): Boolean = {
    getPartitionOrException(topicPartition).futureLog.isDefined
  }

  def futureLogOrException(topicPartition: TopicPartition): UnifiedLog = {
    getPartitionOrException(topicPartition).futureLocalLogOrException
  }

  def localLog(topicPartition: TopicPartition): Option[UnifiedLog] = {
    onlinePartition(topicPartition).flatMap(_.log)
  }

  def tryCompleteActions(): Unit = defaultActionQueue.tryCompleteActions()

  def addToActionQueue(action: Runnable): Unit = defaultActionQueue.add(action)

  /**
   * Append messages to leader replicas of the partition, without waiting on replication.
   *
   * Noted that all pending delayed check operations are stored in a queue. All callers to ReplicaManager.appendRecordsToLeader()
   * are expected to call ActionQueue.tryCompleteActions for all affected partitions, without holding any conflicting
   * locks.
   *
   * @param requiredAcks                  the required acks -- it is only used to ensure that the append meets the
   *                                      required acks.
   * @param internalTopicsAllowed         boolean indicating whether internal topics can be appended to
   * @param origin                        source of the append request (ie, client, replication, coordinator)
   * @param entriesPerPartition           the records per topic partition to be appended.
   *                                      If topic partition contains Uuid.ZERO_UUID as topicId the method
   *                                      will fall back to the old behaviour and rely on topic name.
   * @param requestLocal                  container for the stateful instances scoped to this request -- this must correspond to the
   *                                      thread calling this method
   * @param actionQueue                   the action queue to use. ReplicaManager#defaultActionQueue is used by default.
   * @param verificationGuards            the mapping from topic partition to verification guards if transaction verification is used
   */
  def appendRecordsToLeader(
    requiredAcks: Short,
    internalTopicsAllowed: Boolean,
    origin: AppendOrigin,
    entriesPerPartition: Map[TopicIdPartition, MemoryRecords],
    requestLocal: RequestLocal = RequestLocal.noCaching,
    actionQueue: ActionQueue = this.defaultActionQueue,
    verificationGuards: Map[TopicPartition, VerificationGuard] = Map.empty
  ): Map[TopicIdPartition, LogAppendResult] = {
    val startTimeMs = time.milliseconds
    val localProduceResultsWithTopicId = appendToLocalLog(
      internalTopicsAllowed = internalTopicsAllowed,
      origin,
      entriesPerPartition,
      requiredAcks,
      requestLocal,
      verificationGuards.toMap
    )
    debug("Produce to local log in %d ms".format(time.milliseconds - startTimeMs))

    addCompletePurgatoryAction(actionQueue, localProduceResultsWithTopicId)

    localProduceResultsWithTopicId
  }

  /**
   * Append messages to leader replicas of the partition, and wait for them to be replicated to other replicas;
   * the callback function will be triggered either when timeout or the required acks are satisfied;
   * if the callback function itself is already synchronized on some object then pass this object to avoid deadlock.
   *
   * Noted that all pending delayed check operations are stored in a queue. All callers to ReplicaManager.appendRecords()
   * are expected to call ActionQueue.tryCompleteActions for all affected partitions, without holding any conflicting
   * locks.
   *
   * @param timeout                       maximum time we will wait to append before returning
   * @param requiredAcks                  number of replicas who must acknowledge the append before sending the response
   * @param internalTopicsAllowed         boolean indicating whether internal topics can be appended to
   * @param origin                        source of the append request (ie, client, replication, coordinator)
   * @param entriesPerPartition           the records per topic partition to be appended.
   *                                      If topic partition contains Uuid.ZERO_UUID as topicId the method
   *                                      will fall back to the old behaviour and rely on topic name.
   * @param responseCallback              callback for sending the response
   * @param recordValidationStatsCallback callback for updating stats on record conversions
   * @param requestLocal                  container for the stateful instances scoped to this request -- this must correspond to the
   *                                      thread calling this method
   * @param verificationGuards            the mapping from topic partition to verification guards if transaction verification is used
   */
  def appendRecords(timeout: Long,
                    requiredAcks: Short,
                    internalTopicsAllowed: Boolean,
                    origin: AppendOrigin,
                    entriesPerPartition: Map[TopicIdPartition, MemoryRecords],
                    responseCallback: Map[TopicIdPartition, PartitionResponse] => Unit,
                    recordValidationStatsCallback: Map[TopicIdPartition, RecordValidationStats] => Unit = _ => (),
                    requestLocal: RequestLocal = RequestLocal.noCaching,
                    verificationGuards: Map[TopicPartition, VerificationGuard] = Map.empty): Unit = {
    if (!isValidRequiredAcks(requiredAcks)) {
      sendInvalidRequiredAcksResponse(entriesPerPartition, responseCallback)
      return
    }

    val (inklessEntries, classicEntries) = entriesPerPartition.partition { case (k, _) => isInklessTopic(k.topic()) }

    val inklessResponsesFuture = inklessAppendHandler match {
      case Some(interceptor) => interceptor.handle(inklessEntries.asJava, requestLocal)
      case _ => CompletableFuture.completedFuture(util.Map.of[TopicIdPartition, PartitionResponse]())
    }

    def classicResponseCallback(classicResult: Map[TopicIdPartition, PartitionResponse]): Unit = {
      inklessResponsesFuture.whenComplete { case (result, e) =>
        val inklessResult: Map[TopicIdPartition, PartitionResponse] = if (result != null) result.asScala else {
          error("Inkless append future failed", e)
          inklessEntries.map{ case (tp, _) => tp -> new PartitionResponse(Errors.UNKNOWN_SERVER_ERROR)}
        }
        val inklessAppendResults = new mutable.HashMap[TopicIdPartition, LogAppendResult]
        inklessResult.foreach {
          case (tp, _) =>
            // Consider every successful produce request as if it increased the high watermark. This is a simplification
            // that will always result in the attempt to complete the delayed operations.
            // The current implementation does not take into account that other brokers might have performed appends
            // that would unblock delayed operations in the purgatories of this broker.
            val logAppendInfo = LogAppendInfo.UNKNOWN_LOG_APPEND_INFO.copy(LeaderHwChange.INCREASED)
            val logAppendResult =  LogAppendResult(logAppendInfo, None, hasCustomErrorMessage = false)
            inklessAppendResults += (tp -> logAppendResult)
        }
        addCompletePurgatoryAction(this.defaultActionQueue, inklessAppendResults)
        responseCallback(inklessResult ++ classicResult)
      }
    }

    if (classicEntries.isEmpty) {
      classicResponseCallback(Map.empty)
      return
    }

    val sTime = time.milliseconds
    val localProduceResults = appendRecordsToLeader(
      requiredAcks,
      internalTopicsAllowed,
      origin,
      classicEntries,
      requestLocal,
      defaultActionQueue,
      verificationGuards
    )
    debug("Produce to local log in %d ms".format(time.milliseconds - sTime))

    val produceStatus = buildProducePartitionStatus(localProduceResults)

    recordValidationStatsCallback(localProduceResults.map { case (k, v) =>
      k -> v.info.recordValidationStats
    })

    maybeAddDelayedProduce(
      requiredAcks,
      timeout,
      classicEntries,
      localProduceResults,
      produceStatus,
      classicResponseCallback,
    )
  }

  /**
   * Handles the produce request by starting any transactional verification before appending.
   *
   * @param timeout                       maximum time we will wait to append before returning
   * @param requiredAcks                  number of replicas who must acknowledge the append before sending the response
   * @param internalTopicsAllowed         boolean indicating whether internal topics can be appended to
   * @param transactionalId               the transactional ID for the produce request or null if there is none.
   * @param entriesPerPartition           the records per partition to be appended
   * @param responseCallback              callback for sending the response
   * @param recordValidationStatsCallback callback for updating stats on record conversions
   * @param requestLocal                  container for the stateful instances scoped to this request -- this must correspond to the
   *                                      thread calling this method
   * @param transactionSupportedOperation determines the supported Operation based on the client's Request api version
   *
   * The responseCallback is wrapped so that it is scheduled on a request handler thread. There, it should be called with
   * that request handler thread's thread local and not the one supplied to this method.
   */
  def handleProduceAppend(timeout: Long,
                          requiredAcks: Short,
                          internalTopicsAllowed: Boolean,
                          transactionalId: String,
                          entriesPerPartition: Map[TopicIdPartition, MemoryRecords],
                          responseCallback: Map[TopicIdPartition, PartitionResponse] => Unit,
                          recordValidationStatsCallback: Map[TopicIdPartition, RecordValidationStats] => Unit = _ => (),
                          requestLocal: RequestLocal = RequestLocal.noCaching,
                          transactionSupportedOperation: TransactionSupportedOperation): Unit = {

    val transactionalProducerInfo = mutable.HashSet[(Long, Short)]()
    val topicPartitionBatchInfo = mutable.Map[TopicPartition, Int]()
    val topicIds = entriesPerPartition.keys.map(tp => tp.topic() -> tp.topicId()).toMap
    entriesPerPartition.foreachEntry { (topicIdPartition, records) =>
      // Produce requests (only requests that require verification) should only have one batch per partition in "batches" but check all just to be safe.
      val transactionalBatches = records.batches.asScala.filter(batch => batch.hasProducerId && batch.isTransactional)
      transactionalBatches.foreach(batch => transactionalProducerInfo.add(batch.producerId, batch.producerEpoch))
      if (transactionalBatches.nonEmpty) topicPartitionBatchInfo.put(topicIdPartition.topicPartition(), records.firstBatch.baseSequence)
    }
    if (transactionalProducerInfo.size > 1) {
      throw new InvalidPidMappingException("Transactional records contained more than one producer ID")
    }

    def postVerificationCallback(newRequestLocal: RequestLocal,
                                 results: (Map[TopicPartition, Errors], Map[TopicPartition, VerificationGuard])): Unit = {
      val (preAppendErrors, verificationGuards) = results
      val errorResults: Map[TopicIdPartition, LogAppendResult] = preAppendErrors.map {
        case (topicPartition, error) =>
          // translate transaction coordinator errors to known producer response errors
          val customException =
            error match {
              case Errors.INVALID_TXN_STATE => Some(error.exception("Partition was not added to the transaction"))
              // Transaction verification can fail with a retriable error that older clients may not
              // retry correctly. Translate these to an error which will cause such clients to retry
              // the produce request. We pick `NOT_ENOUGH_REPLICAS` because it does not trigger a
              // metadata refresh.
              case Errors.NETWORK_EXCEPTION |
                   Errors.COORDINATOR_LOAD_IN_PROGRESS |
                   Errors.COORDINATOR_NOT_AVAILABLE |
                   Errors.NOT_COORDINATOR => Some(new NotEnoughReplicasException(
                s"Unable to verify the partition has been added to the transaction. Underlying error: ${error.toString}"))
              case Errors.CONCURRENT_TRANSACTIONS =>
                if (!transactionSupportedOperation.supportsEpochBump) {
                  Some(new NotEnoughReplicasException(
                    s"Unable to verify the partition has been added to the transaction. Underlying error: ${error.toString}"))
                } else {
                  // Don't convert the Concurrent Transaction exception for TV2. Because the error is very common during
                  // the transaction commit phase. Returning Concurrent Transaction is less confusing to the client.
                  None
                }
              case _ => None
            }
          new TopicIdPartition(topicIds.getOrElse(topicPartition.topic(), Uuid.ZERO_UUID), topicPartition) -> LogAppendResult(
            LogAppendInfo.UNKNOWN_LOG_APPEND_INFO,
            Some(customException.getOrElse(error.exception)),
            hasCustomErrorMessage = customException.isDefined
          )
      }
      val entriesWithoutErrorsPerPartition = entriesPerPartition.filter { case (key, _) => !errorResults.contains(key) }

      val preAppendPartitionResponses = buildProducePartitionStatus(errorResults).map { case (k, status) => k -> status.responseStatus }

      def newResponseCallback(responses: Map[TopicIdPartition, PartitionResponse]): Unit = {
        responseCallback(preAppendPartitionResponses ++ responses)
      }

      appendRecords(
        timeout = timeout,
        requiredAcks = requiredAcks,
        internalTopicsAllowed = internalTopicsAllowed,
        origin = AppendOrigin.CLIENT,
        entriesPerPartition = entriesWithoutErrorsPerPartition,
        responseCallback = newResponseCallback,
        recordValidationStatsCallback = recordValidationStatsCallback,
        requestLocal = newRequestLocal,
        verificationGuards = verificationGuards
      )
    }

    if (transactionalProducerInfo.size < 1) {
      postVerificationCallback(
        requestLocal,
        (Map.empty[TopicPartition, Errors], Map.empty[TopicPartition, VerificationGuard])
      )
      return
    }

    // Wrap the callback to be handled on an arbitrary request handler thread
    // when transaction verification is complete. The request local passed in
    // is only used when the callback is executed immediately.
    val wrappedPostVerificationCallback = KafkaRequestHandler.wrapAsyncCallback(
      postVerificationCallback,
      requestLocal
    )

    val retryTimeoutMs = Math.min(addPartitionsToTxnConfig.addPartitionsToTxnRetryBackoffMaxMs(), config.requestTimeoutMs)
    val addPartitionsRetryBackoffMs = addPartitionsToTxnConfig.addPartitionsToTxnRetryBackoffMs()
    val startVerificationTimeMs = time.milliseconds

    def maybeRetryOnConcurrentTransactions(results: (Map[TopicPartition, Errors], Map[TopicPartition, VerificationGuard])): Unit = {
      if (time.milliseconds() - startVerificationTimeMs >= retryTimeoutMs) {
        // We've exceeded the retry timeout, so just call the callback with whatever results we have
        wrappedPostVerificationCallback(results)
      } else if (results._1.values.exists(_ == Errors.CONCURRENT_TRANSACTIONS)) {
        // Retry the verification with backoff
        scheduler.scheduleOnce("retry-add-partitions-to-txn", () => {
          maybeSendPartitionsToTransactionCoordinator(
            topicPartitionBatchInfo,
            transactionalId,
            transactionalProducerInfo.head._1,
            transactionalProducerInfo.head._2,
            maybeRetryOnConcurrentTransactions,
            transactionSupportedOperation
          )
        }, addPartitionsRetryBackoffMs * 1L)
      } else {
        // We don't have concurrent transaction errors, so just call the callback with the results
        wrappedPostVerificationCallback(results)
      }
    }

    maybeSendPartitionsToTransactionCoordinator(
      topicPartitionBatchInfo,
      transactionalId,
      transactionalProducerInfo.head._1,
      transactionalProducerInfo.head._2,
      // If we add partition directly from produce request,
      // we should retry on concurrent transaction error here because:
      //  - the produce backoff adds too much delay
      //  - the produce request is expensive to retry
      if (transactionSupportedOperation.supportsEpochBump) maybeRetryOnConcurrentTransactions else wrappedPostVerificationCallback,
      transactionSupportedOperation
    )
  }

  private def buildProducePartitionStatus(
    results: Map[TopicIdPartition, LogAppendResult]
  ): Map[TopicIdPartition, ProducePartitionStatus] = {
    results.map { case (topicIdPartition, result) =>
      topicIdPartition -> ProducePartitionStatus(
        result.info.lastOffset + 1, // required offset
        new PartitionResponse(
          result.error,
          result.info.firstOffset,
          result.info.logAppendTime,
          result.info.logStartOffset,
          result.info.recordErrors,
          result.errorMessage
        )
      )
    }
  }

  private def addCompletePurgatoryAction(
    actionQueue: ActionQueue,
    appendResults: Map[TopicIdPartition, LogAppendResult]
  ): Unit = {
    actionQueue.add {
      () => appendResults.foreach { case (topicIdPartition, result) =>
        val requestKey = new TopicPartitionOperationKey(topicIdPartition.topicPartition)
        result.info.leaderHwChange match {
          case LeaderHwChange.INCREASED =>
            // some delayed operations may be unblocked after HW changed
            delayedProducePurgatory.checkAndComplete(requestKey)
            delayedFetchPurgatory.checkAndComplete(requestKey)
            delayedDeleteRecordsPurgatory.checkAndComplete(requestKey)
            if (topicIdPartition.topicId != Uuid.ZERO_UUID) delayedShareFetchPurgatory.checkAndComplete(new DelayedShareFetchPartitionKey(
              topicIdPartition.topicId, topicIdPartition.partition))
          case LeaderHwChange.SAME =>
            // probably unblock some follower fetch requests since log end offset has been updated
            delayedFetchPurgatory.checkAndComplete(requestKey)
          case LeaderHwChange.NONE =>
          // nothing
        }
      }
    }
  }

  private def maybeAddDelayedProduce(
    requiredAcks: Short,
    timeoutMs: Long,
    entriesPerPartition: Map[TopicIdPartition, MemoryRecords],
    initialAppendResults: Map[TopicIdPartition, LogAppendResult],
    initialProduceStatus: Map[TopicIdPartition, ProducePartitionStatus],
    responseCallback: Map[TopicIdPartition, PartitionResponse] => Unit,
  ): Unit = {
    if (delayedProduceRequestRequired(requiredAcks, entriesPerPartition, initialAppendResults)) {
      // create delayed produce operation
      val produceMetadata = ProduceMetadata(requiredAcks, initialProduceStatus)
      val delayedProduce = new DelayedProduce(timeoutMs, produceMetadata, this, responseCallback)

      // create a list of (topic, partition) pairs to use as keys for this delayed produce operation
      val producerRequestKeys = entriesPerPartition.keys.map(new TopicPartitionOperationKey(_)).toList

      // try to complete the request immediately, otherwise put it into the purgatory
      // this is because while the delayed produce operation is being created, new
      // requests may arrive and hence make this operation completable.
      delayedProducePurgatory.tryCompleteElseWatch(delayedProduce, producerRequestKeys.asJava)
    } else {
      // we can respond immediately
      val produceResponseStatus = initialProduceStatus.map { case (k, status) => k -> status.responseStatus }
      responseCallback(produceResponseStatus)
    }
  }

  private def sendInvalidRequiredAcksResponse(
    entries: Map[TopicIdPartition, MemoryRecords],
    responseCallback: Map[TopicIdPartition, PartitionResponse] => Unit): Unit = {
    // If required.acks is outside accepted range, something is wrong with the client
    // Just return an error and don't handle the request at all
    val responseStatus = entries.map { case (topicIdPartition, _) =>
      topicIdPartition -> new PartitionResponse(
        Errors.INVALID_REQUIRED_ACKS,
        LogAppendInfo.UNKNOWN_LOG_APPEND_INFO.firstOffset,
        RecordBatch.NO_TIMESTAMP,
        LogAppendInfo.UNKNOWN_LOG_APPEND_INFO.logStartOffset
      )
    }
    responseCallback(responseStatus)
  }

  /**
   *
   * @param topicPartition                                    the topic partition to maybe verify or add
   * @param transactionalId               the transactional id for the transaction
   * @param producerId                    the producer id for the producer writing to the transaction
   * @param producerEpoch                 the epoch of the producer writing to the transaction
   * @param baseSequence                  the base sequence of the first record in the batch we are trying to append
   * @param callback                      the method to execute once the verification is either completed or returns an error
   * @param transactionSupportedOperation determines the supported operation based on the client's Request API version
   *
   *                                                          If this is the first time a partition appears in a transaction, it must be verified or added to the partition depending on the
   *                                                          transactionSupported operation.
   *                                                          If verifying, when the verification returns, the callback will be supplied the error if it exists or Errors.NONE.
   * If the verification guard exists, it will also be supplied. Otherwise the SENTINEL verification guard will be returned.
   * This guard can not be used for verification and any appends that attempt to use it will fail.
   *
   *                                                          If adding, the callback will be supplied the error if it exists or Errors.NONE.
   */
  def maybeSendPartitionToTransactionCoordinator(
    topicPartition: TopicPartition,
    transactionalId: String,
    producerId: Long,
    producerEpoch: Short,
    baseSequence: Int,
    callback: ((Errors, VerificationGuard)) => Unit,
    transactionSupportedOperation: TransactionSupportedOperation
  ): Unit = {
    def generalizedCallback(results: (Map[TopicPartition, Errors], Map[TopicPartition, VerificationGuard])): Unit = {
      val (preAppendErrors, verificationGuards) = results
      callback((
        preAppendErrors.getOrElse(topicPartition, Errors.NONE),
        verificationGuards.getOrElse(topicPartition, VerificationGuard.SENTINEL)
      ))
    }

    maybeSendPartitionsToTransactionCoordinator(
      Map(topicPartition -> baseSequence),
      transactionalId,
      producerId,
      producerEpoch,
      generalizedCallback,
      transactionSupportedOperation
    )
  }

  /**
   *
   * @param topicPartitionBatchInfo                         the topic partitions to maybe verify or add mapped to the base sequence of their first record batch
   * @param transactionalId                 the transactional id for the transaction
   * @param producerId                      the producer id for the producer writing to the transaction
   * @param producerEpoch                   the epoch of the producer writing to the transaction
   * @param callback                        the method to execute once the verification is either completed or returns an error
   * @param transactionSupportedOperation   determines the supported operation based on the client's Request API version
   *
   *                                                        If this is the first time the partitions appear in a transaction, they must be verified or added to the partition depending on the
   *                                                        transactionSupported operation.
   *                                                        If verifying, when the verification returns, the callback will be supplied the errors per topic partition if there were errors.
   * The callback will also be supplied the verification guards per partition if they exist. It is possible to have an
   * error and a verification guard for a topic partition if the topic partition was unable to be verified by the transaction
   * coordinator. Transaction coordinator errors are mapped to append-friendly errors.
   *
   *                                                        If adding, the callback will be e supplied the errors per topic partition if there were errors.
   */
  private def maybeSendPartitionsToTransactionCoordinator(
    topicPartitionBatchInfo: Map[TopicPartition, Int],
    transactionalId: String,
    producerId: Long,
    producerEpoch: Short,
    callback: ((Map[TopicPartition, Errors], Map[TopicPartition, VerificationGuard])) => Unit,
    transactionSupportedOperation: TransactionSupportedOperation
  ): Unit = {
    def transactionPartitionVerificationEnable = {
      new TransactionLogConfig(config).transactionPartitionVerificationEnable
    }
    // Skip verification if the request is not transactional or transaction verification is disabled.
    if (transactionalId == null
      || addPartitionsToTxnManager.isEmpty
      || (!transactionSupportedOperation.supportsEpochBump && !transactionPartitionVerificationEnable)
    ) {
      callback((Map.empty[TopicPartition, Errors], Map.empty[TopicPartition, VerificationGuard]))
      return
    }

    val verificationGuards = mutable.Map[TopicPartition, VerificationGuard]()
    val errors = mutable.Map[TopicPartition, Errors]()

    topicPartitionBatchInfo.map { case (topicPartition, baseSequence) =>
      val errorOrGuard = maybeStartTransactionVerificationForPartition(
        topicPartition,
        producerId,
        producerEpoch,
        baseSequence,
        transactionSupportedOperation.supportsEpochBump
      )

      errorOrGuard match {
        case Left(error) => errors.put(topicPartition, error)
        case Right(verificationGuard) => if (verificationGuard != VerificationGuard.SENTINEL)
          verificationGuards.put(topicPartition, verificationGuard)
      }
    }

    if (verificationGuards.isEmpty) {
      callback((errors.toMap, Map.empty[TopicPartition, VerificationGuard]))
      return
    }

    def invokeCallback(
      verificationErrors: java.util.Map[TopicPartition, Errors]
    ): Unit = {
      callback((errors ++ verificationErrors.asScala, verificationGuards.toMap))
    }

    addPartitionsToTxnManager.foreach(_.addOrVerifyTransaction(
      transactionalId,
      producerId,
      producerEpoch,
      verificationGuards.keys.toSeq.asJava,
      invokeCallback,
      transactionSupportedOperation
    ))

  }

  private def maybeStartTransactionVerificationForPartition(
    topicPartition: TopicPartition,
    producerId: Long,
    producerEpoch: Short,
    baseSequence: Int,
    supportsEpochBump: Boolean
  ): Either[Errors, VerificationGuard] = {
    try {
      val verificationGuard = getPartitionOrException(topicPartition)
        .maybeStartTransactionVerification(producerId, baseSequence, producerEpoch, supportsEpochBump)
      Right(verificationGuard)
    } catch {
      case e: Exception =>
        Left(Errors.forException(e))
    }
  }

  /**
   * Delete records on leader replicas of the partition, and wait for delete records operation be propagated to other replicas;
   * the callback function will be triggered either when timeout or logStartOffset of all live replicas have reached the specified offset
   */
  private def deleteRecordsOnLocalLog(offsetPerPartition: Map[TopicPartition, Long], allowInternalTopicDeletion: Boolean): Map[TopicPartition, LogDeleteRecordsResult] = {
    trace("Delete records on local logs to offsets [%s]".format(offsetPerPartition))
    offsetPerPartition.map { case (topicPartition, requestedOffset) =>
      // reject delete records operation for internal topics unless allowInternalTopicDeletion is true
      if (Topic.isInternal(topicPartition.topic) && !allowInternalTopicDeletion) {
        (topicPartition, LogDeleteRecordsResult(-1L, -1L, Some(new InvalidTopicException(s"Cannot delete records of internal topic ${topicPartition.topic}"))))
      } else {
        try {
          val partition = getPartitionOrException(topicPartition)
          val logDeleteResult = partition.deleteRecordsOnLeader(requestedOffset)
          (topicPartition, logDeleteResult)
        } catch {
          case e@ (_: UnknownTopicOrPartitionException |
                   _: NotLeaderOrFollowerException |
                   _: OffsetOutOfRangeException |
                   _: PolicyViolationException |
                   _: KafkaStorageException) =>
            (topicPartition, LogDeleteRecordsResult(-1L, -1L, Some(e)))
          case t: Throwable =>
            error("Error processing delete records operation on partition %s".format(topicPartition), t)
            (topicPartition, LogDeleteRecordsResult(-1L, -1L, Some(t)))
        }
      }
    }
  }

  // If there exists a topic partition that meets the following requirement,
  // we need to put a delayed DeleteRecordsRequest and wait for the delete records operation to complete
  //
  // 1. the delete records operation on this partition is successful
  // 2. low watermark of this partition is smaller than the specified offset
  private def delayedDeleteRecordsRequired(localDeleteRecordsResults: Map[TopicPartition, LogDeleteRecordsResult]): Boolean = {
    localDeleteRecordsResults.exists{ case (_, deleteRecordsResult) =>
      deleteRecordsResult.exception.isEmpty && deleteRecordsResult.lowWatermark < deleteRecordsResult.requestedOffset
    }
  }

  /**
   * For each pair of partition and log directory specified in the map, if the partition has already been created on
   * this broker, move its log files to the specified log directory. Otherwise, record the pair in the memory so that
   * the partition will be created in the specified log directory when broker receives LeaderAndIsrRequest for the partition later.
   */
  def alterReplicaLogDirs(partitionDirs: Map[TopicPartition, String]): Map[TopicPartition, Errors] = {
    replicaStateChangeLock synchronized {
      partitionDirs.map { case (topicPartition, destinationDir) =>
        try {
          /* If the topic name is exceptionally long, we can't support altering the log directory.
           * See KAFKA-4893 for details.
           * TODO: fix this by implementing topic IDs. */
          if (UnifiedLog.logFutureDirName(topicPartition).length > 255)
            throw new InvalidTopicException("The topic name is too long.")
          if (!logManager.isLogDirOnline(destinationDir))
            throw new KafkaStorageException(s"Log directory $destinationDir is offline")

          getPartition(topicPartition) match {
            case HostedPartition.Online(partition) =>
              // Stop current replica movement if the destinationDir is different from the existing destination log directory
              if (partition.futureReplicaDirChanged(destinationDir)) {
                replicaAlterLogDirsManager.removeFetcherForPartitions(Set(topicPartition))
                partition.removeFutureLocalReplica()
              }
            case HostedPartition.Offline(_) =>
              throw new KafkaStorageException(s"Partition $topicPartition is offline")

            case HostedPartition.None => // Do nothing
          }

          // If the log for this partition has not been created yet:
          // 1) Record the destination log directory in the memory so that the partition will be created in this log directory
          //    when broker receives LeaderAndIsrRequest for this partition later.
          // 2) Respond with NotLeaderOrFollowerException for this partition in the AlterReplicaLogDirsResponse
          logManager.maybeUpdatePreferredLogDir(topicPartition, destinationDir)

          // throw NotLeaderOrFollowerException if replica does not exist for the given partition
          val partition = getPartitionOrException(topicPartition)
          val log = partition.localLogOrException
          val topicId = log.topicId

          // If the destinationLDir is different from the current log directory of the replica:
          // - If there is no offline log directory, create the future log in the destinationDir (if it does not exist) and
          //   start ReplicaAlterDirThread to move data of this partition from the current log to the future log
          // - Otherwise, return KafkaStorageException. We do not create the future log while there is offline log directory
          //   so that we can avoid creating future log for the same partition in multiple log directories.
          val highWatermarkCheckpoints = new LazyOffsetCheckpoints(this.highWatermarkCheckpoints.asJava)
          if (partition.maybeCreateFutureReplica(destinationDir, highWatermarkCheckpoints)) {
            val futureLog = futureLocalLogOrException(topicPartition)
            logManager.abortAndPauseCleaning(topicPartition)

            val initialFetchState = InitialFetchState(topicId.toScala, new BrokerEndPoint(config.brokerId, "localhost", -1),
              partition.getLeaderEpoch, futureLog.highWatermark)
            replicaAlterLogDirsManager.addFetcherForPartitions(Map(topicPartition -> initialFetchState))
          }

          (topicPartition, Errors.NONE)
        } catch {
          case e@(_: InvalidTopicException |
                  _: LogDirNotFoundException |
                  _: ReplicaNotAvailableException |
                  _: KafkaStorageException) =>
            warn(s"Unable to alter log dirs for $topicPartition", e)
            (topicPartition, Errors.forException(e))
          case e: NotLeaderOrFollowerException =>
            // Retaining REPLICA_NOT_AVAILABLE exception for ALTER_REPLICA_LOG_DIRS for compatibility
            warn(s"Unable to alter log dirs for $topicPartition", e)
            (topicPartition, Errors.REPLICA_NOT_AVAILABLE)
          case t: Throwable =>
            error("Error while changing replica dir for partition %s".format(topicPartition), t)
            (topicPartition, Errors.forException(t))
        }
      }
    }
  }

  /*
   * Get the LogDirInfo for the specified list of partitions.
   *
   * Each LogDirInfo specifies the following information for a given log directory:
   * 1) Error of the log directory, e.g. whether the log is online or offline
   * 2) size and lag of current and future logs for each partition in the given log directory. Only logs of the queried partitions
   *    are included. There may be future logs (which will replace the current logs of the partition in the future) on the broker after KIP-113 is implemented.
   */
  def describeLogDirs(partitions: Set[TopicPartition]): util.List[DescribeLogDirsResponseData.DescribeLogDirsResult] = {
    val logsByDir = logManager.allLogs.groupBy(log => log.parentDir)

    config.logDirs.stream().distinct().map(logDir => {
      val file = Paths.get(logDir)
      val absolutePath = file.toAbsolutePath.toString
      try {
        if (!logManager.isLogDirOnline(absolutePath))
          throw new KafkaStorageException(s"Log directory $absolutePath is offline")

        val fileStore = Files.getFileStore(file)
        val totalBytes = adjustForLargeFileSystems(fileStore.getTotalSpace)
        val usableBytes = adjustForLargeFileSystems(fileStore.getUsableSpace)
        val topicInfos = logsByDir.get(absolutePath) match {
          case Some(logs) =>
            logs.groupBy(_.topicPartition.topic).map { case (topic, logs) =>
              new DescribeLogDirsResponseData.DescribeLogDirsTopic().setName(topic).setPartitions(
                logs.filter { log =>
                  partitions.contains(log.topicPartition)
                }.map { log =>
                  new DescribeLogDirsResponseData.DescribeLogDirsPartition()
                    .setPartitionSize(log.size)
                    .setPartitionIndex(log.topicPartition.partition)
                    .setOffsetLag(getLogEndOffsetLag(log.topicPartition, log.logEndOffset, log.isFuture))
                    .setIsFutureKey(log.isFuture)
                }.toList.asJava)
            }.filterNot(_.partitions().isEmpty).toList.asJava
          case None =>
            Collections.emptyList[DescribeLogDirsTopic]()
        }

        val describeLogDirsResult = new DescribeLogDirsResponseData.DescribeLogDirsResult()
          .setLogDir(absolutePath).setTopics(topicInfos)
          .setErrorCode(Errors.NONE.code)
          .setTotalBytes(totalBytes).setUsableBytes(usableBytes)
        if (!topicInfos.isEmpty)
          describeLogDirsResult.setTopics(topicInfos)
        describeLogDirsResult

      } catch {
        case e: KafkaStorageException =>
          warn("Unable to describe replica dirs for %s".format(absolutePath), e)
          new DescribeLogDirsResponseData.DescribeLogDirsResult()
            .setLogDir(absolutePath)
            .setErrorCode(Errors.KAFKA_STORAGE_ERROR.code)
        case t: Throwable =>
          error(s"Error while describing replica in dir $absolutePath", t)
          new DescribeLogDirsResponseData.DescribeLogDirsResult()
            .setLogDir(absolutePath)
            .setErrorCode(Errors.forException(t).code)
      }
    }).collect(Collectors.toList[DescribeLogDirsResponseData.DescribeLogDirsResult]())
  }

  // See: https://bugs.openjdk.java.net/browse/JDK-8162520
  private def adjustForLargeFileSystems(space: Long): Long = {
    if (space < 0)
      return Long.MaxValue
    space
  }

  def getLogEndOffsetLag(topicPartition: TopicPartition, logEndOffset: Long, isFuture: Boolean): Long = {
    localLog(topicPartition) match {
      case Some(log) =>
        if (isFuture)
          log.logEndOffset - logEndOffset
        else
          math.max(log.highWatermark - logEndOffset, 0)
      case None =>
        // return -1L to indicate that the LEO lag is not available if the replica is not created or is offline
        DescribeLogDirsResponse.INVALID_OFFSET_LAG
    }
  }

  def deleteRecords(timeout: Long,
                    offsetPerPartition: Map[TopicPartition, Long],
                    responseCallback: Map[TopicPartition, DeleteRecordsPartitionResult] => Unit,
                    allowInternalTopicDeletion: Boolean = false): Unit = {

    if (inklessDeleteRecordsInterceptor.exists(_.intercept(
      offsetPerPartition.view.mapValues(java.lang.Long.valueOf).toMap.asJava,
      r => responseCallback(r.asScala)))) {
      return
    }

    val timeBeforeLocalDeleteRecords = time.milliseconds
    val localDeleteRecordsResults = deleteRecordsOnLocalLog(offsetPerPartition, allowInternalTopicDeletion)
    debug("Delete records on local log in %d ms".format(time.milliseconds - timeBeforeLocalDeleteRecords))

    val deleteRecordsStatus = localDeleteRecordsResults.map { case (topicPartition, result) =>
      topicPartition ->
        new DeleteRecordsPartitionStatus(
          result.requestedOffset, // requested offset
          new DeleteRecordsPartitionResult()
            .setLowWatermark(result.lowWatermark)
            .setErrorCode(result.error.code)
            .setPartitionIndex(topicPartition.partition)) // response status
    }

    if (delayedDeleteRecordsRequired(localDeleteRecordsResults)) {
      def onAcks(topicPartition: TopicPartition, status: DeleteRecordsPartitionStatus): Unit = {
        val (lowWatermarkReached, error, lw) = getPartition(topicPartition) match {
          case HostedPartition.Online(partition) =>
            partition.leaderLogIfLocal match {
              case Some(_) =>
                val leaderLW = partition.lowWatermarkIfLeader
                (leaderLW >= status.requiredOffset, Errors.NONE, leaderLW)
              case None =>
                (false, Errors.NOT_LEADER_OR_FOLLOWER, DeleteRecordsResponse.INVALID_LOW_WATERMARK)
            }

          case HostedPartition.Offline(_) =>
            (false, Errors.KAFKA_STORAGE_ERROR, DeleteRecordsResponse.INVALID_LOW_WATERMARK)

          case HostedPartition.None =>
            (false, Errors.UNKNOWN_TOPIC_OR_PARTITION, DeleteRecordsResponse.INVALID_LOW_WATERMARK)
        }
        if (error != Errors.NONE || lowWatermarkReached) {
          status.setAcksPending(false)
          status.responseStatus.setErrorCode(error.code)
          status.responseStatus.setLowWatermark(lw)
        }
      }
      // create delayed delete records operation
      val delayedDeleteRecords = new DelayedDeleteRecords(timeout, deleteRecordsStatus.asJava, onAcks,  response => responseCallback(response.asScala))

      // create a list of (topic, partition) pairs to use as keys for this delayed delete records operation
      val deleteRecordsRequestKeys = offsetPerPartition.keys.map(new TopicPartitionOperationKey(_)).toList

      // try to complete the request immediately, otherwise put it into the purgatory
      // this is because while the delayed delete records operation is being created, new
      // requests may arrive and hence make this operation completable.
      delayedDeleteRecordsPurgatory.tryCompleteElseWatch(delayedDeleteRecords, deleteRecordsRequestKeys.asJava)
    } else {
      // we can respond immediately
      val deleteRecordsResponseStatus = deleteRecordsStatus.map { case (k, status) => k -> status.responseStatus }
      responseCallback(deleteRecordsResponseStatus)
    }
  }

  // If all the following conditions are true, we need to put a delayed produce request and wait for replication to complete
  //
  // 1. required acks = -1
  // 2. there is data to append
  // 3. at least one partition append was successful (fewer errors than partitions)
  private def delayedProduceRequestRequired(requiredAcks: Short,
                                            entriesPerPartition: Map[TopicIdPartition, MemoryRecords],
                                            localProduceResults: Map[TopicIdPartition, LogAppendResult]): Boolean = {
    requiredAcks == -1 &&
    entriesPerPartition.nonEmpty &&
    localProduceResults.values.count(_.exception.isDefined) < entriesPerPartition.size
  }

  private def isValidRequiredAcks(requiredAcks: Short): Boolean = {
    requiredAcks == -1 || requiredAcks == 1 || requiredAcks == 0
  }

  /**
   * Append the messages to the local replica logs
   */
  private def appendToLocalLog(internalTopicsAllowed: Boolean,
                               origin: AppendOrigin,
                               entriesPerPartition: Map[TopicIdPartition, MemoryRecords],
                               requiredAcks: Short,
                               requestLocal: RequestLocal,
                               verificationGuards: Map[TopicPartition, VerificationGuard]):
  Map[TopicIdPartition, LogAppendResult] = {
    val traceEnabled = isTraceEnabled
    def processFailedRecord(topicIdPartition: TopicIdPartition, t: Throwable) = {
      val logStartOffset = onlinePartition(topicIdPartition.topicPartition()).map(_.logStartOffset).getOrElse(-1L)
      brokerTopicStats.topicStats(topicIdPartition.topic).failedProduceRequestRate.mark()
      brokerTopicStats.allTopicsStats.failedProduceRequestRate.mark()
      t match {
        case _: InvalidProducerEpochException =>
          info(s"Error processing append operation on partition $topicIdPartition", t)
        case _ =>
          error(s"Error processing append operation on partition $topicIdPartition", t)
      }

      logStartOffset
    }

    if (traceEnabled)
      trace(s"Append [$entriesPerPartition] to local log")

    entriesPerPartition.map { case (topicIdPartition, records) =>
      brokerTopicStats.topicStats(topicIdPartition.topic).totalProduceRequestRate.mark()
      brokerTopicStats.allTopicsStats.totalProduceRequestRate.mark()

      // reject appending to internal topics if it is not allowed
      if (Topic.isInternal(topicIdPartition.topic) && !internalTopicsAllowed) {
        (topicIdPartition, LogAppendResult(
          LogAppendInfo.UNKNOWN_LOG_APPEND_INFO,
          Some(new InvalidTopicException(s"Cannot append to internal topic ${topicIdPartition.topic}")),
          hasCustomErrorMessage = false))
      } else {
        try {
          val partition = getPartitionOrException(topicIdPartition)
          val info = partition.appendRecordsToLeader(records, origin, requiredAcks, requestLocal,
            verificationGuards.getOrElse(topicIdPartition.topicPartition(), VerificationGuard.SENTINEL))
          val numAppendedMessages = info.numMessages

          // update stats for successfully appended bytes and messages as bytesInRate and messageInRate
          brokerTopicStats.topicStats(topicIdPartition.topic).bytesInRate.mark(records.sizeInBytes)
          brokerTopicStats.allTopicsStats.bytesInRate.mark(records.sizeInBytes)
          brokerTopicStats.topicStats(topicIdPartition.topic).messagesInRate.mark(numAppendedMessages)
          brokerTopicStats.allTopicsStats.messagesInRate.mark(numAppendedMessages)

          if (traceEnabled)
            trace(s"${records.sizeInBytes} written to log $topicIdPartition beginning at offset " +
              s"${info.firstOffset} and ending at offset ${info.lastOffset}")

          (topicIdPartition, LogAppendResult(info, exception = None, hasCustomErrorMessage = false))

        } catch {
          // NOTE: Failed produce requests metric is not incremented for known exceptions
          // it is supposed to indicate un-expected failures of a broker in handling a produce request
          case e@ (_: UnknownTopicOrPartitionException |
                   _: NotLeaderOrFollowerException |
                   _: RecordTooLargeException |
                   _: RecordBatchTooLargeException |
                   _: CorruptRecordException |
                   _: KafkaStorageException |
                   _: UnknownTopicIdException) =>
            (topicIdPartition, LogAppendResult(LogAppendInfo.UNKNOWN_LOG_APPEND_INFO, Some(e), hasCustomErrorMessage = false))
          case rve: RecordValidationException =>
            val logStartOffset = processFailedRecord(topicIdPartition, rve.invalidException)
            val recordErrors = rve.recordErrors
            (topicIdPartition, LogAppendResult(LogAppendInfo.unknownLogAppendInfoWithAdditionalInfo(logStartOffset, recordErrors),
              Some(rve.invalidException), hasCustomErrorMessage = true))
          case t: Throwable =>
            val logStartOffset = processFailedRecord(topicIdPartition, t)
            (topicIdPartition, LogAppendResult(LogAppendInfo.unknownLogAppendInfoWithLogStartOffset(logStartOffset),
              Some(t), hasCustomErrorMessage = false))
        }
      }
    }
  }

  def fetchOffset(topics: Seq[ListOffsetsTopic],
                  duplicatePartitions: Set[TopicPartition],
                  isolationLevel: IsolationLevel,
                  replicaId: Int,
                  clientId: String,
                  correlationId: Int,
                  version: Short,
                  buildErrorResponse: (Errors, ListOffsetsPartition) => ListOffsetsPartitionResponse,
                  responseCallback: Consumer[util.Collection[ListOffsetsTopicResponse]],
                  timeoutMs: Int = 0): Unit = {
    val maybeFetchOffsetJob: Option[FetchOffsetHandler.Job] = inklessFetchOffsetHandler.map(_.createJob())
    val statusByPartition = mutable.Map[TopicPartition, ListOffsetsPartitionStatus]()
    topics.foreach { topic =>
      topic.partitions.asScala.foreach { partition =>
        val topicPartition = new TopicPartition(topic.name, partition.partitionIndex)
        if (duplicatePartitions.contains(topicPartition)) {
          debug(s"OffsetRequest with correlation id $correlationId from client $clientId on partition $topicPartition " +
            s"failed because the partition is duplicated in the request.")
          statusByPartition += topicPartition ->
            ListOffsetsPartitionStatus.builder().responseOpt(Optional.of(buildErrorResponse(Errors.INVALID_REQUEST, partition))).build()
        } else if (isListOffsetsTimestampUnsupported(partition.timestamp(), version)) {
          statusByPartition += topicPartition ->
            ListOffsetsPartitionStatus.builder().responseOpt(Optional.of(buildErrorResponse(Errors.UNSUPPORTED_VERSION, partition))).build()
        } else if (maybeFetchOffsetJob.exists(_.mustHandle(topic.name()))) {
          statusByPartition += topicPartition ->
            inklessFetchOffset(maybeFetchOffsetJob.get, topicPartition, partition)
        } else {
          try {
            val fetchOnlyFromLeader = replicaId != ListOffsetsRequest.DEBUGGING_REPLICA_ID
            val isClientRequest = replicaId == ListOffsetsRequest.CONSUMER_REPLICA_ID
            val isolationLevelOpt = if (isClientRequest)
              Some(isolationLevel)
            else
              None

            val resultHolder = fetchOffsetForTimestamp(topicPartition,
              partition.timestamp,
              isolationLevelOpt,
              if (partition.currentLeaderEpoch == ListOffsetsResponse.UNKNOWN_EPOCH) Optional.empty() else Optional.of(partition.currentLeaderEpoch),
              fetchOnlyFromLeader)

            val status = {
              if (resultHolder.timestampAndOffsetOpt().isPresent) {
                // This case is for normal topic that does not have remote storage.
                val timestampAndOffsetOpt = resultHolder.timestampAndOffsetOpt.get
                var partitionResponse = buildErrorResponse(Errors.NONE, partition)
                if (resultHolder.lastFetchableOffset.isPresent &&
                  timestampAndOffsetOpt.offset >= resultHolder.lastFetchableOffset.get) {
                  resultHolder.maybeOffsetsError.map(e => throw e)
                } else {
                  partitionResponse = new ListOffsetsPartitionResponse()
                    .setPartitionIndex(partition.partitionIndex)
                    .setErrorCode(Errors.NONE.code)
                    .setTimestamp(timestampAndOffsetOpt.timestamp)
                    .setOffset(timestampAndOffsetOpt.offset)
                  if (timestampAndOffsetOpt.leaderEpoch.isPresent && version >= 4)
                    partitionResponse.setLeaderEpoch(timestampAndOffsetOpt.leaderEpoch.get)
                }
                ListOffsetsPartitionStatus.builder().responseOpt(Optional.of(partitionResponse)).build()
              } else if (resultHolder.timestampAndOffsetOpt.isEmpty && resultHolder.futureHolderOpt.isEmpty) {
                // This is an empty offset response scenario
                resultHolder.maybeOffsetsError.map(e => throw e)
                ListOffsetsPartitionStatus.builder().responseOpt(Optional.of(buildErrorResponse(Errors.NONE, partition))).build()
              } else if (resultHolder.timestampAndOffsetOpt.isEmpty && resultHolder.futureHolderOpt.isPresent) {
                // This case is for topic enabled with remote storage and we want to search the timestamp in
                // remote storage using async fashion.
                ListOffsetsPartitionStatus.builder()
                  .futureHolderOpt(resultHolder.futureHolderOpt())
                  .lastFetchableOffset(resultHolder.lastFetchableOffset)
                  .maybeOffsetsError(resultHolder.maybeOffsetsError)
                  .build()
              } else {
                throw new IllegalStateException(s"Unexpected result holder state $resultHolder")
              }
            }
            statusByPartition += topicPartition -> status
          } catch {
            // NOTE: These exceptions are special cases since these error messages are typically transient or the client
            // would have received a clear exception and there is no value in logging the entire stack trace for the same
            case e @ (_ : UnknownTopicOrPartitionException |
                      _ : NotLeaderOrFollowerException |
                      _ : UnknownLeaderEpochException |
                      _ : FencedLeaderEpochException |
                      _ : KafkaStorageException |
                      _ : UnsupportedForMessageFormatException) =>
              debug(s"Offset request with correlation id $correlationId from client $clientId on " +
                s"partition $topicPartition failed due to ${e.getMessage}")
              statusByPartition += topicPartition ->
                ListOffsetsPartitionStatus.builder().responseOpt(Optional.of(buildErrorResponse(Errors.forException(e), partition))).build()
            // Only V5 and newer ListOffset calls should get OFFSET_NOT_AVAILABLE
            case e: OffsetNotAvailableException =>
              if (version >= 5) {
                statusByPartition += topicPartition ->
                  ListOffsetsPartitionStatus.builder().responseOpt(Optional.of(buildErrorResponse(Errors.forException(e), partition))).build()
              } else {
                statusByPartition += topicPartition ->
                  ListOffsetsPartitionStatus.builder().responseOpt(Optional.of(buildErrorResponse(Errors.LEADER_NOT_AVAILABLE, partition))).build()
              }

            case e: Throwable =>
              error("Error while responding to offset request", e)
              statusByPartition += topicPartition ->
                ListOffsetsPartitionStatus.builder().responseOpt(Optional.of(buildErrorResponse(Errors.forException(e), partition))).build()
          }
        }
      }
    }

    maybeFetchOffsetJob.foreach(_.start())

    if (delayedRemoteListOffsetsRequired(statusByPartition)) {
      val delayMs: Long = if (timeoutMs > 0) timeoutMs else config.remoteLogManagerConfig.remoteListOffsetsRequestTimeoutMs()
      // create delayed remote list offsets operation
      val delayedRemoteListOffsets = new DelayedRemoteListOffsets(delayMs, version, statusByPartition.asJava, tp => getPartitionOrException(tp), isInklessTopic, responseCallback)
      // create a list of (topic, partition) pairs to use as keys for this delayed remote list offsets operation
      val listOffsetsRequestKeys = statusByPartition.keys.map(new TopicPartitionOperationKey(_)).toList
      // try to complete the request immediately, otherwise put it into the purgatory
      delayedRemoteListOffsetsPurgatory.tryCompleteElseWatch(delayedRemoteListOffsets, listOffsetsRequestKeys.asJava)
    } else {
      // we can respond immediately
      val responseTopics = statusByPartition.groupBy(e => e._1.topic()).map {
        case (topic, status) =>
          new ListOffsetsTopicResponse().setName(topic).setPartitions(status.values.flatMap(s => Some(s.responseOpt.get())).toList.asJava)
      }.toList
      responseCallback.accept(responseTopics.asJava)
    }
  }

  private def inklessFetchOffset(fetchOffsetJob: FetchOffsetHandler.Job,
                                 topicPartition: TopicPartition,
                                 partition: ListOffsetsPartition): ListOffsetsPartitionStatus = {
    val taskFuture = fetchOffsetJob.add(topicPartition, partition)
    taskFuture.handle((_ignoredValue, _ignoredError) => {
      val key = new TopicPartitionOperationKey(topicPartition.topic, topicPartition.partition)
      delayedRemoteListOffsetsPurgatory.checkAndComplete(key)
    })
    val futureHolder = new AsyncOffsetReadFutureHolder[OffsetResultHolder.FileRecordsOrError](
      fetchOffsetJob.cancelHandler(),
      taskFuture
    )
    ListOffsetsPartitionStatus.builder()
      .futureHolderOpt(Optional.of(futureHolder))
      .build()
  }

  private def delayedRemoteListOffsetsRequired(responseByPartition: Map[TopicPartition, ListOffsetsPartitionStatus]): Boolean = {
    responseByPartition.values.exists(status => status.futureHolderOpt.isPresent)
  }

  def fetchOffsetForTimestamp(topicPartition: TopicPartition,
                              timestamp: Long,
                              isolationLevel: Option[IsolationLevel],
                              currentLeaderEpoch: Optional[Integer],
                              fetchOnlyFromLeader: Boolean): OffsetResultHolder = {
    val partition = getPartitionOrException(topicPartition)
    partition.fetchOffsetForTimestamp(timestamp, isolationLevel, currentLeaderEpoch, fetchOnlyFromLeader, remoteLogManager)
  }

  /**
   * Returns [[LogReadResult]] with error if a task for RemoteStorageFetchInfo could not be scheduled successfully
   * else returns [[None]].
   */
  private def processRemoteFetch(remoteFetchInfo: RemoteStorageFetchInfo,
                                 params: FetchParams,
                                 responseCallback: Seq[(TopicIdPartition, FetchPartitionData)] => Unit,
                                 logReadResults: Seq[(TopicIdPartition, LogReadResult)],
                                 fetchPartitionStatus: Seq[(TopicIdPartition, FetchPartitionStatus)]): Option[LogReadResult] = {
    val key = new TopicPartitionOperationKey(remoteFetchInfo.topicPartition.topic(), remoteFetchInfo.topicPartition.partition())
    val remoteFetchResult = new CompletableFuture[RemoteLogReadResult]
    var remoteFetchTask: Future[Void] = null
    try {
      remoteFetchTask = remoteLogManager.get.asyncRead(remoteFetchInfo, (result: RemoteLogReadResult) => {
        remoteFetchResult.complete(result)
        delayedRemoteFetchPurgatory.checkAndComplete(key)
      })
    } catch {
      case e: RejectedExecutionException =>
        // Return the error if any in scheduling the remote fetch task
        warn("Unable to fetch data from remote storage", e)
        return Some(createLogReadResult(e))
    }

    val remoteFetchMaxWaitMs = config.remoteLogManagerConfig.remoteFetchMaxWaitMs().toLong
    val remoteFetch = new DelayedRemoteFetch(remoteFetchTask, remoteFetchResult, remoteFetchInfo, remoteFetchMaxWaitMs,
      fetchPartitionStatus, params, logReadResults, this, responseCallback)
    delayedRemoteFetchPurgatory.tryCompleteElseWatch(remoteFetch, util.Collections.singletonList(key))
    None
  }

  private def buildPartitionToFetchPartitionData(logReadResults: Seq[(TopicIdPartition, LogReadResult)],
                                                 remoteFetchTopicPartition: TopicPartition,
                                                 error: LogReadResult): Seq[(TopicIdPartition, FetchPartitionData)] = {
    logReadResults.map { case (tp, result) =>
      val fetchPartitionData = {
        if (tp.topicPartition().equals(remoteFetchTopicPartition))
          error
        else
          result
      }.toFetchPartitionData(false)

      tp -> fetchPartitionData
    }
  }

  def fetchInklessMessages(params: FetchParams,
                           fetchInfos: Seq[(TopicIdPartition, PartitionData)]): CompletableFuture[Seq[(TopicIdPartition, FetchPartitionData)]] = {
    inklessFetchHandler match {
      case Some(handler) => handler.handle(params, fetchInfos.toMap.asJava).thenApply(_.asScala.toSeq)
      case _ => CompletableFuture.completedFuture(Seq.empty)
    }
  }

  /**
   * Create new FetchParams by scaling the maxBytes by a percentage.
   */
  def fetchParamsWithNewMaxBytes(originalParams: FetchParams, percentage: Float): FetchParams = {
    new FetchParams(originalParams.replicaId, originalParams.replicaEpoch, originalParams.maxWaitMs, originalParams.minBytes,
      Math.max((originalParams.maxBytes * percentage).toInt, originalParams.maxBytes),
      originalParams.isolation, originalParams.clientMetadata, originalParams.shareFetchRequest)
  }

  /**
   * Fetch messages from a replica, and wait until enough data can be fetched and return;
   * the callback function will be triggered either when timeout or required fetch info is satisfied.
   * Consumers may fetch from any replica, but followers can only fetch from the leader.
   */
  def fetchMessages(params: FetchParams,
                    fetchInfos: Seq[(TopicIdPartition, PartitionData)],
                    quota: ReplicaQuota,
                    responseCallback: Seq[(TopicIdPartition, FetchPartitionData)] => Unit): Unit = {
    val (inklessFetchInfos, classicFetchInfos) = fetchInfos.partition { case (k, _) => isInklessTopic(k.topic()) }
    val inklessParams = fetchParamsWithNewMaxBytes(params, inklessFetchInfos.size.toFloat / fetchInfos.size.toFloat)
    val classicParams = fetchParamsWithNewMaxBytes(params, classicFetchInfos.size.toFloat / fetchInfos.size.toFloat)

    val inklessResponsesFuture = fetchInklessMessages(inklessParams, inklessFetchInfos)

    def responseCallbackWithInkless(classicFetchResult: Seq[(TopicIdPartition, FetchPartitionData)],
                                    classicBytesReadable: Long,
                                    classicFetchPartitionStatus: Seq[(TopicIdPartition, FetchPartitionStatus)]): Unit = {
      inklessResponsesFuture.whenComplete { case (inklessFetchResult, _) =>
        val inklessBytesReadable = inklessFetchResult.map { case (_, partitionData) => partitionData.records.sizeInBytes() }.sum
        val bytesReadable = inklessBytesReadable + classicBytesReadable
        if (bytesReadable >= params.minBytes || params.maxWaitMs <= 0) {
          responseCallback(inklessFetchResult ++ classicFetchResult)
        } else {
          // If there is not enough data to respond, we will let the fetch request wait for new data.
          val inklessFetchPartitionStatus = inklessFetchInfos.map {
            case (k, partitionData) =>
              k -> FetchPartitionStatus(new LogOffsetMetadata(partitionData.fetchOffset), partitionData)
          }

          val fetchPartitionStatus = classicFetchPartitionStatus ++ inklessFetchPartitionStatus

          val delayedFetch = new DelayedFetch(
            params = params,
            fetchPartitionStatus = fetchPartitionStatus,
            replicaManager = this,
            isInklessTopic = isInklessTopic,
            quota = quota,
            responseCallback = responseCallback
          )

          // create a list of (topic, partition) pairs to use as keys for this delayed fetch operation
          val delayedFetchKeys = fetchPartitionStatus.map { case (tp, _) => new TopicPartitionOperationKey(tp) }.toList

          // try to complete the request immediately, otherwise put it into the purgatory;
          // this is because while the delayed fetch operation is being created, new requests
          // may arrive and hence make this operation completable.
          delayedFetchPurgatory.tryCompleteElseWatch(delayedFetch, delayedFetchKeys.asJava)
        }
      }
    }

    if (classicFetchInfos.isEmpty) {
      responseCallbackWithInkless(Seq.empty, 0L, Seq.empty)
      return
    }

    // check if this fetch request can be satisfied right away
    val logReadResults = readFromLog(classicParams, classicFetchInfos, quota, readFromPurgatory = false)
    var bytesReadable: Long = 0
    var errorReadingData = false

    // The 1st topic-partition that has to be read from remote storage
    var remoteFetchInfo: Optional[RemoteStorageFetchInfo] = Optional.empty()

    var hasDivergingEpoch = false
    var hasPreferredReadReplica = false
    val logReadResultMap = new mutable.HashMap[TopicIdPartition, LogReadResult]

    logReadResults.foreach { case (topicIdPartition, logReadResult) =>
      brokerTopicStats.topicStats(topicIdPartition.topicPartition.topic).totalFetchRequestRate.mark()
      brokerTopicStats.allTopicsStats.totalFetchRequestRate.mark()
      if (logReadResult.error != Errors.NONE)
        errorReadingData = true
      if (!remoteFetchInfo.isPresent && logReadResult.info.delayedRemoteStorageFetch.isPresent) {
        remoteFetchInfo = logReadResult.info.delayedRemoteStorageFetch
      }
      if (logReadResult.divergingEpoch.isPresent)
        hasDivergingEpoch = true
      if (logReadResult.preferredReadReplica.isPresent)
        hasPreferredReadReplica = true
      bytesReadable = bytesReadable + logReadResult.info.records.sizeInBytes
      logReadResultMap.put(topicIdPartition, logReadResult)
    }

    val fetchPartitionData = logReadResults.map { case (tp, result) =>
      val isReassignmentFetch = params.isFromFollower && isAddingReplica(tp.topicPartition, params.replicaId)
      tp -> result.toFetchPartitionData(isReassignmentFetch)
    }

    // Respond immediately if no remote fetches are required and any of the below conditions is true
    //                        1) fetch request does not want to wait
    //                        2) fetch request does not require any data
    //                        3) has enough data to respond
    //                        4) some error happens while reading data
    //                        5) we found a diverging epoch
    //                        6) has a preferred read replica
    if (!remoteFetchInfo.isPresent && inklessFetchInfos.isEmpty && (params.maxWaitMs <= 0 || bytesReadable >= params.minBytes || errorReadingData ||
      hasDivergingEpoch || hasPreferredReadReplica)) {
      responseCallback(fetchPartitionData)
    } else {
      // construct the fetch results from the read results
      val fetchPartitionStatus = new mutable.ArrayBuffer[(TopicIdPartition, FetchPartitionStatus)]
      classicFetchInfos.foreach { case (topicIdPartition, partitionData) =>
        logReadResultMap.get(topicIdPartition).foreach(logReadResult => {
          val logOffsetMetadata = logReadResult.info.fetchOffsetMetadata
          fetchPartitionStatus += (topicIdPartition -> FetchPartitionStatus(logOffsetMetadata, partitionData))
        })
      }

      if (remoteFetchInfo.isPresent) {
        // In case of remote fetches, synchronously wait for Inkless records and then perform the remote fetch.
        // This is currently a workaround to avoid modifying the DelayedRemoteFetch in order to correctly process
        // inkless fetches.
        val inklessFetchResults = try {
          val response = inklessResponsesFuture.get(params.maxWaitMs, TimeUnit.MILLISECONDS)
          response.map { case (tp, data) =>
            val exception: Optional[Throwable] = data.error match {
              case Errors.NONE => Optional.empty()
              case ex => Optional.of(ex.exception())
            }
            tp -> new LogReadResult(
              new FetchDataInfo(new LogOffsetMetadata(0L), data.records), // offset is ignored
              data.divergingEpoch, data.highWatermark, data.logStartOffset, data.highWatermark, data.logStartOffset,
              0L, // fetchTimeMs is ignored
              data.lastStableOffset, data.preferredReadReplica,
              exception
            )
          }
        } catch {
          case e: Throwable =>
            inklessFetchInfos.map { case (tp, _) =>
              tp -> new LogReadResult(
                FetchDataInfo.empty(-1L),
                Optional.empty(), -1L, -1L, -1L, -1L, 0L, OptionalLong.empty(), OptionalInt.empty(),
                Optional.of(e)
              )
            }
        }
        val readResults = logReadResults ++ inklessFetchResults
        val maybeLogReadResultWithError = processRemoteFetch(remoteFetchInfo.get(), classicParams, responseCallback, readResults, fetchPartitionStatus)
        if (maybeLogReadResultWithError.isDefined) {
          // If there is an error in scheduling the remote fetch task, return what we currently have
          // (the data read from local log segment for the other topic-partitions) and an error for the topic-partition
          // that we couldn't read from remote storage
          val partitionToFetchPartitionData = buildPartitionToFetchPartitionData(readResults, remoteFetchInfo.get().topicPartition, maybeLogReadResultWithError.get)
          responseCallback(partitionToFetchPartitionData)
        }
      } else {
        responseCallbackWithInkless(fetchPartitionData, bytesReadable, fetchPartitionStatus)
      }
    }
  }

  /**
   * Read from multiple topic partitions at the given offset up to maxSize bytes
   */
  def readFromLog(
    params: FetchParams,
    readPartitionInfo: Seq[(TopicIdPartition, PartitionData)],
    quota: ReplicaQuota,
    readFromPurgatory: Boolean): Seq[(TopicIdPartition, LogReadResult)] = {
    val traceEnabled = isTraceEnabled

    def checkFetchDataInfo(partition: Partition, givenFetchedDataInfo: FetchDataInfo) = {
      if (params.isFromFollower && shouldLeaderThrottle(quota, partition, params.replicaId)) {
        // If the partition is being throttled, simply return an empty set.
        new FetchDataInfo(givenFetchedDataInfo.fetchOffsetMetadata, MemoryRecords.EMPTY)
      } else if (givenFetchedDataInfo.firstEntryIncomplete) {
        // Replace incomplete message sets with an empty one as consumers can make progress in such
        // cases and don't need to report a `RecordTooLargeException`
        new FetchDataInfo(givenFetchedDataInfo.fetchOffsetMetadata, MemoryRecords.EMPTY)
      } else {
        givenFetchedDataInfo
      }
    }

    def read(tp: TopicIdPartition, fetchInfo: PartitionData, limitBytes: Int, minOneMessage: Boolean): LogReadResult = {
      val offset = fetchInfo.fetchOffset
      val partitionFetchSize = fetchInfo.maxBytes
      val followerLogStartOffset = fetchInfo.logStartOffset

      val adjustedMaxBytes = math.min(fetchInfo.maxBytes, limitBytes)
      var log: UnifiedLog = null
      var partition : Partition = null
      val fetchTimeMs = time.milliseconds
      try {
        if (traceEnabled)
          trace(s"Fetching log segment for partition $tp, offset $offset, partition fetch size $partitionFetchSize, " +
            s"remaining response limit $limitBytes" +
            (if (minOneMessage) s", ignoring response/partition size limits" else ""))

        partition = getPartitionOrException(tp.topicPartition)

        // Check if topic ID from the fetch request/session matches the ID in the log
        val topicId = if (tp.topicId == Uuid.ZERO_UUID) None else Some(tp.topicId)
        if (!hasConsistentTopicId(topicId, partition.topicId))
          throw new InconsistentTopicIdException("Topic ID in the fetch session did not match the topic ID in the log.")

        // If we are the leader, determine the preferred read-replica
        val preferredReadReplica = params.clientMetadata.toScala.flatMap(
          metadata => findPreferredReadReplica(partition, metadata, params.replicaId, fetchInfo.fetchOffset, fetchTimeMs))

        if (preferredReadReplica.isDefined) {
          replicaSelectorPlugin.foreach { selector =>
            debug(s"Replica selector ${selector.get.getClass.getSimpleName} returned preferred replica " +
              s"${preferredReadReplica.get} for ${params.clientMetadata}")
          }
          // If a preferred read-replica is set, skip the read
          val offsetSnapshot = partition.fetchOffsetSnapshot(fetchInfo.currentLeaderEpoch, fetchOnlyFromLeader = false)
          new LogReadResult(new FetchDataInfo(LogOffsetMetadata.UNKNOWN_OFFSET_METADATA, MemoryRecords.EMPTY),
            Optional.empty(),
            offsetSnapshot.highWatermark.messageOffset,
            offsetSnapshot.logStartOffset,
            offsetSnapshot.logEndOffset.messageOffset,
            followerLogStartOffset,
            -1L,
            OptionalLong.of(offsetSnapshot.lastStableOffset.messageOffset),
            if (preferredReadReplica.isDefined) OptionalInt.of(preferredReadReplica.get) else OptionalInt.empty(),
            Optional.empty())
        } else {
          log = partition.localLogWithEpochOrThrow(fetchInfo.currentLeaderEpoch, params.fetchOnlyLeader())

          // Try the read first, this tells us whether we need all of adjustedFetchSize for this partition
          val readInfo: LogReadInfo = partition.fetchRecords(
            fetchParams = params,
            fetchPartitionData = fetchInfo,
            fetchTimeMs = fetchTimeMs,
            maxBytes = adjustedMaxBytes,
            minOneMessage = minOneMessage,
            updateFetchState = !readFromPurgatory)

          val fetchDataInfo = checkFetchDataInfo(partition, readInfo.fetchedData)

          new LogReadResult(fetchDataInfo,
            readInfo.divergingEpoch,
            readInfo.highWatermark,
            readInfo.logStartOffset,
            readInfo.logEndOffset,
            followerLogStartOffset,
            fetchTimeMs,
            OptionalLong.of(readInfo.lastStableOffset),
            if (preferredReadReplica.isDefined) OptionalInt.of(preferredReadReplica.get) else OptionalInt.empty(),
            Optional.empty()
          )
        }
      } catch {
        // NOTE: Failed fetch requests metric is not incremented for known exceptions since it
        // is supposed to indicate un-expected failure of a broker in handling a fetch request
        case e@ (_: UnknownTopicOrPartitionException |
                 _: NotLeaderOrFollowerException |
                 _: UnknownLeaderEpochException |
                 _: FencedLeaderEpochException |
                 _: ReplicaNotAvailableException |
                 _: KafkaStorageException |
                 _: InconsistentTopicIdException) =>
          createLogReadResult(e)
        case e: OffsetOutOfRangeException =>
          handleOffsetOutOfRangeError(tp, params, fetchInfo, adjustedMaxBytes, minOneMessage, log, fetchTimeMs, e)
        case e: Throwable =>
          brokerTopicStats.topicStats(tp.topic).failedFetchRequestRate.mark()
          brokerTopicStats.allTopicsStats.failedFetchRequestRate.mark()

          val fetchSource = FetchRequest.describeReplicaId(params.replicaId)
          error(s"Error processing fetch with max size $adjustedMaxBytes from $fetchSource " +
            s"on partition $tp: $fetchInfo", e)

          new LogReadResult(new FetchDataInfo(LogOffsetMetadata.UNKNOWN_OFFSET_METADATA, MemoryRecords.EMPTY),
            Optional.empty(),
            UnifiedLog.UNKNOWN_OFFSET,
            UnifiedLog.UNKNOWN_OFFSET,
            UnifiedLog.UNKNOWN_OFFSET,
            UnifiedLog.UNKNOWN_OFFSET,
            -1L,
            OptionalLong.empty(),
            Optional.of(e)
          )
      }
    }

    var limitBytes = params.maxBytes
    val result = new mutable.ArrayBuffer[(TopicIdPartition, LogReadResult)]
    var minOneMessage = true
    readPartitionInfo.foreach { case (tp, fetchInfo) =>
      val readResult = read(tp, fetchInfo, limitBytes, minOneMessage)
      val recordBatchSize = readResult.info.records.sizeInBytes
      // Once we read from a non-empty partition, we stop ignoring request and partition level size limits
      if (recordBatchSize > 0)
        minOneMessage = false
      limitBytes = math.max(0, limitBytes - recordBatchSize)
      result += (tp -> readResult)
    }
    result
  }

  private def handleOffsetOutOfRangeError(tp: TopicIdPartition, params: FetchParams, fetchInfo: PartitionData,
                                          adjustedMaxBytes: Int, minOneMessage:
                                          Boolean, log: UnifiedLog, fetchTimeMs: Long,
                                          exception: OffsetOutOfRangeException): LogReadResult = {
    val offset = fetchInfo.fetchOffset
    // In case of offset out of range errors, handle it for tiered storage only if all the below conditions are true.
    //   1) remote log manager is enabled and it is available
    //   2) `log` instance should not be null here as that would have been caught earlier with NotLeaderOrFollowerException or ReplicaNotAvailableException.
    //   3) fetch offset is within the offset range of the remote storage layer
    if (remoteLogManager.isDefined && log != null && log.remoteLogEnabled() &&
      log.logStartOffset <= offset && offset < log.localLogStartOffset())
    {
      val highWatermark = log.highWatermark
      val leaderLogStartOffset = log.logStartOffset
      val leaderLogEndOffset = log.logEndOffset

      if (params.isFromFollower || params.isFromFuture) {
        // If it is from a follower or from a future replica, then send the offset metadata only as the data is already available in remote
        // storage and throw an error saying that this offset is moved to tiered storage.
        createLogReadResult(highWatermark, leaderLogStartOffset, leaderLogEndOffset,
          new OffsetMovedToTieredStorageException("Given offset" + offset + " is moved to tiered storage"))
      } else {
        val throttleTimeMs = remoteLogManager.get.getFetchThrottleTimeMs
        val fetchDataInfo = if (throttleTimeMs > 0) {
          // Record the throttle time for the remote log fetches
          remoteLogManager.get.fetchThrottleTimeSensor().record(throttleTimeMs, time.milliseconds())

          // We do not want to send an exception in a LogReadResult response (like we do in other cases when we send
          // UnknownOffsetMetadata), because it is classified as an error in reading the data, and a response is
          // immediately sent back to the client. Instead, we want to serve data for the other topic partitions of the
          // fetch request via delayed fetch if required (when sending immediate response, we skip delayed fetch).
          new FetchDataInfo(
            LogOffsetMetadata.UNKNOWN_OFFSET_METADATA,
            MemoryRecords.EMPTY,
            false,
            Optional.empty(),
            Optional.empty()
          )
        } else {
          // For consume fetch requests, create a dummy FetchDataInfo with the remote storage fetch information.
          // For the first topic-partition that needs remote data, we will use this information to read the data in another thread.
          new FetchDataInfo(new LogOffsetMetadata(offset), MemoryRecords.EMPTY, false, Optional.empty(),
            Optional.of(new RemoteStorageFetchInfo(adjustedMaxBytes, minOneMessage, tp.topicPartition(),
              fetchInfo, params.isolation)))
        }

        new LogReadResult(fetchDataInfo,
          Optional.empty(),
          highWatermark,
          leaderLogStartOffset,
          leaderLogEndOffset,
          fetchInfo.logStartOffset,
          fetchTimeMs,
          OptionalLong.of(log.lastStableOffset),
          Optional.empty[Throwable]())
      }
    } else {
      createLogReadResult(exception)
    }
  }

  /**
    * Using the configured [[ReplicaSelector]], determine the preferred read replica for a partition given the
    * client metadata, the requested offset, and the current set of replicas. If the preferred read replica is the
    * leader, return None
    */
  def findPreferredReadReplica(partition: Partition,
                               clientMetadata: ClientMetadata,
                               replicaId: Int,
                               fetchOffset: Long,
                               currentTimeMs: Long): Option[Int] = {
    partition.leaderIdIfLocal.flatMap { leaderReplicaId =>
      // Don't look up preferred for follower fetches via normal replication
      if (FetchRequest.isValidBrokerId(replicaId))
        None
      else {
        replicaSelectorPlugin.flatMap { replicaSelector =>
          val replicaEndpoints = metadataCache.getPartitionReplicaEndpoints(partition.topicPartition,
            new ListenerName(clientMetadata.listenerName)).asScala
          val replicaInfoSet = mutable.Set[ReplicaView]()

          partition.remoteReplicas.foreach { replica =>
            val replicaState = replica.stateSnapshot
            // Exclude replicas that are not in the ISR as the follower may lag behind. Worst case, the follower
            // will continue to lag and the consumer will fall behind the produce. The leader will
            // continuously pick the lagging follower when the consumer refreshes its preferred read replica.
            // This can go on indefinitely.
            if (partition.inSyncReplicaIds.contains(replica.brokerId) &&
                replicaState.logEndOffset >= fetchOffset &&
                replicaState.logStartOffset <= fetchOffset) {

              replicaInfoSet.add(new DefaultReplicaView(
                replicaEndpoints.getOrElse(replica.brokerId, Node.noNode()),
                replicaState.logEndOffset,
                currentTimeMs - replicaState.lastCaughtUpTimeMs
              ))
            }
          }

          val leaderReplica = new DefaultReplicaView(
            replicaEndpoints.getOrElse(leaderReplicaId, Node.noNode()),
            partition.localLogOrException.logEndOffset,
            0L
          )
          replicaInfoSet.add(leaderReplica)

          val partitionInfo = new DefaultPartitionView(replicaInfoSet.asJava, leaderReplica)
          replicaSelector.get.select(partition.topicPartition, clientMetadata, partitionInfo).toScala.collect {
            // Even though the replica selector can return the leader, we don't want to send it out with the
            // FetchResponse, so we exclude it here
            case selected if !selected.endpoint.isEmpty && selected != leaderReplica => selected.endpoint.id
          }
        }
      }
    }
  }

  /**
   *  To avoid ISR thrashing, we only throttle a replica on the leader if it's in the throttled replica list,
   *  the quota is exceeded and the replica is not in sync.
   */
  def shouldLeaderThrottle(quota: ReplicaQuota, partition: Partition, replicaId: Int): Boolean = {
    val isReplicaInSync = partition.inSyncReplicaIds.contains(replicaId)
    !isReplicaInSync && quota.isThrottled(partition.topicPartition) && quota.isQuotaExceeded
  }

  def getLogConfig(topicPartition: TopicPartition): Option[LogConfig] = localLog(topicPartition).map(_.config)

  def becomeLeaderOrFollower(correlationId: Int,
                             leaderAndIsrRequest: LeaderAndIsrRequest,
                             onLeadershipChange: (Iterable[Partition], Iterable[Partition]) => Unit): LeaderAndIsrResponse = {
    val startMs = time.milliseconds()
    replicaStateChangeLock synchronized {
      val controllerId = leaderAndIsrRequest.controllerId
      val requestPartitionStates = leaderAndIsrRequest.partitionStates.asScala
      stateChangeLogger.info(s"Handling LeaderAndIsr request correlationId $correlationId from controller " +
        s"$controllerId for ${requestPartitionStates.size} partitions")
      if (stateChangeLogger.isTraceEnabled)
        requestPartitionStates.foreach { partitionState =>
          stateChangeLogger.trace(s"Received LeaderAndIsr request $partitionState " +
            s"correlation id $correlationId from controller $controllerId " +
            s"epoch ${leaderAndIsrRequest.controllerEpoch}")
        }
      val topicIds = leaderAndIsrRequest.topicIds()
      def topicIdFromRequest(topicName: String): Option[Uuid] = {
        val topicId = topicIds.get(topicName)
        // if invalid topic ID return None
        if (topicId == null || topicId == Uuid.ZERO_UUID)
          None
        else
          Some(topicId)
      }

      val response = {
        if (leaderAndIsrRequest.controllerEpoch < controllerEpoch) {
          stateChangeLogger.warn(s"Ignoring LeaderAndIsr request from controller $controllerId with " +
            s"correlation id $correlationId since its controller epoch ${leaderAndIsrRequest.controllerEpoch} is old. " +
            s"Latest known controller epoch is $controllerEpoch")
          leaderAndIsrRequest.getErrorResponse(Errors.STALE_CONTROLLER_EPOCH.exception)
        } else {
          val responseMap = new mutable.HashMap[TopicPartition, Errors]
          controllerEpoch = leaderAndIsrRequest.controllerEpoch

          val partitions = new mutable.HashSet[Partition]()
          val partitionsToBeLeader = new mutable.HashMap[Partition, LeaderAndIsrRequest.PartitionState]()
          val partitionsToBeFollower = new mutable.HashMap[Partition, LeaderAndIsrRequest.PartitionState]()
          val topicIdUpdateFollowerPartitions = new mutable.HashSet[Partition]()
          val allTopicPartitionsInRequest = new mutable.HashSet[TopicPartition]()

          // First create the partition if it doesn't exist already
          requestPartitionStates.foreach { partitionState =>
            val topicPartition = new TopicPartition(partitionState.topicName, partitionState.partitionIndex)
            allTopicPartitionsInRequest += topicPartition
            val partitionOpt = getPartition(topicPartition) match {
              case HostedPartition.Offline(_) =>
                stateChangeLogger.warn(s"Ignoring LeaderAndIsr request from " +
                  s"controller $controllerId with correlation id $correlationId " +
                  s"epoch $controllerEpoch for partition $topicPartition as the local replica for the " +
                  "partition is in an offline log directory")
                responseMap.put(topicPartition, Errors.KAFKA_STORAGE_ERROR)
                None

              case HostedPartition.Online(partition) =>
                Some(partition)

              case HostedPartition.None =>
                val partition = Partition(topicPartition, time, this)
                allPartitions.putIfAbsent(topicPartition, HostedPartition.Online(partition))
                Some(partition)
            }

            // Next check the topic ID and the partition's leader epoch
            partitionOpt.foreach { partition =>
              val currentLeaderEpoch = partition.getLeaderEpoch
              val requestLeaderEpoch = partitionState.leaderEpoch
              val requestTopicId = topicIdFromRequest(topicPartition.topic)
              val logTopicId = partition.topicId

              if (!hasConsistentTopicId(requestTopicId, logTopicId)) {
                stateChangeLogger.error(s"Topic ID in memory: ${logTopicId.get} does not" +
                  s" match the topic ID for partition $topicPartition received: " +
                  s"${requestTopicId.get}.")
                responseMap.put(topicPartition, Errors.INCONSISTENT_TOPIC_ID)
              } else if (requestLeaderEpoch >= currentLeaderEpoch) {
                // If the leader epoch is valid record the epoch of the controller that made the leadership decision.
                // This is useful while updating the isr to maintain the decision maker controller's epoch in the zookeeper path
                if (partitionState.replicas.contains(localBrokerId)) {
                  partitions += partition
                  if (partitionState.leader == localBrokerId) {
                    partitionsToBeLeader.put(partition, partitionState)
                  } else {
                    partitionsToBeFollower.put(partition, partitionState)
                  }
                } else {
                  stateChangeLogger.warn(s"Ignoring LeaderAndIsr request from controller $controllerId with " +
                    s"correlation id $correlationId epoch $controllerEpoch for partition $topicPartition as itself is not " +
                    s"in assigned replica list ${partitionState.replicas.asScala.mkString(",")}")
                  responseMap.put(topicPartition, Errors.UNKNOWN_TOPIC_OR_PARTITION)
                }
              } else if (requestLeaderEpoch < currentLeaderEpoch) {
                stateChangeLogger.warn(s"Ignoring LeaderAndIsr request from " +
                  s"controller $controllerId with correlation id $correlationId " +
                  s"epoch $controllerEpoch for partition $topicPartition since its associated " +
                  s"leader epoch $requestLeaderEpoch is smaller than the current " +
                  s"leader epoch $currentLeaderEpoch")
                responseMap.put(topicPartition, Errors.STALE_CONTROLLER_EPOCH)
              } else {
                val error = requestTopicId match {
                  case Some(topicId) if logTopicId.isEmpty =>
                    // The controller may send LeaderAndIsr to upgrade to using topic IDs without bumping the epoch.
                    // If we have a matching epoch, we expect the log to be defined.
                    val log = localLogOrException(partition.topicPartition)
                    log.assignTopicId(topicId)
                    stateChangeLogger.info(s"Updating log for $topicPartition to assign topic ID " +
                      s"$topicId from LeaderAndIsr request from controller $controllerId with correlation " +
                      s"id $correlationId epoch $controllerEpoch")
                    if (partitionState.leader != localBrokerId)
                      topicIdUpdateFollowerPartitions.add(partition)
                    Errors.NONE
                  case None if logTopicId.isDefined && partitionState.leader != localBrokerId =>
                    // If we have a topic ID in the log but not in the request, we must have previously had topic IDs but
                    // are now downgrading. If we are a follower, remove the topic ID from the PartitionFetchState.
                    stateChangeLogger.info(s"Updating PartitionFetchState for $topicPartition to remove log topic ID " +
                      s"${logTopicId.get} since LeaderAndIsr request from controller $controllerId with correlation " +
                      s"id $correlationId epoch $controllerEpoch did not contain a topic ID")
                    topicIdUpdateFollowerPartitions.add(partition)
                    Errors.NONE
                  case _ =>
                    stateChangeLogger.info(s"Ignoring LeaderAndIsr request from " +
                      s"controller $controllerId with correlation id $correlationId " +
                      s"epoch $controllerEpoch for partition $topicPartition since its associated " +
                      s"leader epoch $requestLeaderEpoch matches the current leader epoch")
                    Errors.STALE_CONTROLLER_EPOCH
                }
                responseMap.put(topicPartition, error)
              }
            }
          }

          val highWatermarkCheckpoints = new LazyOffsetCheckpoints(this.highWatermarkCheckpoints.asJava)
          val partitionsBecomeLeader = if (partitionsToBeLeader.nonEmpty)
            makeLeaders(controllerId, controllerEpoch, partitionsToBeLeader, correlationId, responseMap,
              highWatermarkCheckpoints, topicIdFromRequest)
          else
            Set.empty[Partition]
          val partitionsBecomeFollower = if (partitionsToBeFollower.nonEmpty)
            makeFollowers(controllerId, controllerEpoch, partitionsToBeFollower, correlationId, responseMap,
              highWatermarkCheckpoints, topicIdFromRequest)
          else
            Set.empty[Partition]

          val followerTopicSet = partitionsBecomeFollower.map(_.topic).toSet
          updateLeaderAndFollowerMetrics(followerTopicSet)

          if (topicIdUpdateFollowerPartitions.nonEmpty)
            updateTopicIdForFollowers(controllerId, controllerEpoch, topicIdUpdateFollowerPartitions, correlationId, topicIdFromRequest)

          // We initialize highwatermark thread after the first LeaderAndIsr request. This ensures that all the partitions
          // have been completely populated before starting the checkpointing there by avoiding weird race conditions
          startHighWatermarkCheckPointThread()

          maybeAddLogDirFetchers(partitions, highWatermarkCheckpoints, topicIdFromRequest)

          replicaFetcherManager.shutdownIdleFetcherThreads()
          replicaAlterLogDirsManager.shutdownIdleFetcherThreads()

          remoteLogManager.foreach(rlm => rlm.onLeadershipChange((partitionsBecomeLeader.toSet: Set[TopicPartitionLog]).asJava, (partitionsBecomeFollower.toSet: Set[TopicPartitionLog]).asJava, topicIds))

          onLeadershipChange(partitionsBecomeLeader, partitionsBecomeFollower)

          val topics = new util.LinkedHashMap[Uuid, util.List[LeaderAndIsrResponse.PartitionError]]
          responseMap.foreachEntry { (tp, error) =>
            val topicId = topicIds.get(tp.topic)
            var partitionErrors = topics.get(topicId)
            if (partitionErrors == null) {
              partitionErrors = new util.ArrayList[LeaderAndIsrResponse.PartitionError]()
              topics.put(topicId, partitionErrors)
            }
            partitionErrors.add(new LeaderAndIsrResponse.PartitionError(tp.partition(), error.code))
          }
          new LeaderAndIsrResponse(Errors.NONE, topics)
        }
      }
      val endMs = time.milliseconds()
      val elapsedMs = endMs - startMs
      stateChangeLogger.info(s"Finished LeaderAndIsr request in ${elapsedMs}ms correlationId $correlationId from controller " +
        s"$controllerId for ${requestPartitionStates.size} partitions")
      response
    }
  }

  /**
   * Checks if the topic ID provided in the request is consistent with the topic ID in the log.
   * When using this method to handle a Fetch request, the topic ID may have been provided by an earlier request.
   *
   * If the request had an invalid topic ID (null or zero), then we assume that topic IDs are not supported.
   * The topic ID was not inconsistent, so return true.
   * If the log does not exist or the topic ID is not yet set, logTopicIdOpt will be None.
   * In both cases, the ID is not inconsistent so return true.
   *
   * @param requestTopicIdOpt the topic ID from the request if it exists
   * @param logTopicIdOpt the topic ID in the log if the log and the topic ID exist
   * @return true if the request topic id is consistent, false otherwise
   */
  private def hasConsistentTopicId(requestTopicIdOpt: Option[Uuid], logTopicIdOpt: Option[Uuid]): Boolean = {
    requestTopicIdOpt match {
      case None => true
      case Some(requestTopicId) => logTopicIdOpt.isEmpty || logTopicIdOpt.contains(requestTopicId)
    }
  }

  /**
   * KAFKA-8392
   * For topic partitions of which the broker is no longer a leader, delete metrics related to
   * those topics. Note that this means the broker stops being either a replica or a leader of
   * partitions of said topics
   */
  private def updateLeaderAndFollowerMetrics(newFollowerTopics: Set[String]): Unit = {
    val leaderTopicSet = leaderPartitionsIterator.map(_.topic).toSet
    newFollowerTopics.diff(leaderTopicSet).foreach(brokerTopicStats.removeOldLeaderMetrics)

    // remove metrics for brokers which are not followers of a topic
    leaderTopicSet.diff(newFollowerTopics).foreach(brokerTopicStats.removeOldFollowerMetrics)
  }

  protected[server] def maybeAddLogDirFetchers(partitions: Set[Partition],
                                               offsetCheckpoints: OffsetCheckpoints,
                                               topicIds: String => Option[Uuid]): Unit = {
    val futureReplicasAndInitialOffset = new mutable.HashMap[TopicPartition, InitialFetchState]
    for (partition <- partitions) {
      val topicPartition = partition.topicPartition
      logManager.getLog(topicPartition, isFuture = true).foreach { futureLog =>
        partition.log.foreach { _ =>
          val leader = new BrokerEndPoint(config.brokerId, "localhost", -1)

          // Add future replica log to partition's map if it's not existed
          if (partition.maybeCreateFutureReplica(futureLog.parentDir, offsetCheckpoints, topicIds(partition.topic))) {
            // pause cleaning for partitions that are being moved and start ReplicaAlterDirThread to move
            // replica from source dir to destination dir
            logManager.abortAndPauseCleaning(topicPartition)
          }

          futureReplicasAndInitialOffset.put(topicPartition, InitialFetchState(topicIds(topicPartition.topic), leader,
            partition.getLeaderEpoch, futureLog.highWatermark))
        }
      }
    }

    if (futureReplicasAndInitialOffset.nonEmpty) {
      // Even though it's possible that there is another thread adding fetcher for this future log partition,
      // but it's fine because `BrokerIdAndFetcherId` will be identical and the operation will be no-op.
      replicaAlterLogDirsManager.addFetcherForPartitions(futureReplicasAndInitialOffset)
    }
  }

  /*
   * Make the current broker to become leader for a given set of partitions by:
   *
   * 1. Stop fetchers for these partitions
   * 2. Update the partition metadata in cache
   * 3. Add these partitions to the leader partitions set
   *
   * If an unexpected error is thrown in this function, it will be propagated to KafkaApis where
   * the error message will be set on each partition since we do not know which partition caused it. Otherwise,
   * return the set of partitions that are made leader due to this method
   *
   *  TODO: the above may need to be fixed later
   */
  private def makeLeaders(controllerId: Int,
                          controllerEpoch: Int,
                          partitionStates: Map[Partition, LeaderAndIsrRequest.PartitionState],
                          correlationId: Int,
                          responseMap: mutable.Map[TopicPartition, Errors],
                          highWatermarkCheckpoints: OffsetCheckpoints,
                          topicIds: String => Option[Uuid]): Set[Partition] = {
    val traceEnabled = stateChangeLogger.isTraceEnabled
    partitionStates.keys.foreach { partition =>
      if (traceEnabled)
        stateChangeLogger.trace(s"Handling LeaderAndIsr request correlationId $correlationId from " +
          s"controller $controllerId epoch $controllerEpoch starting the become-leader transition for " +
          s"partition ${partition.topicPartition}")
      responseMap.put(partition.topicPartition, Errors.NONE)
    }

    val partitionsToMakeLeaders = mutable.Set[Partition]()

    try {
      // First stop fetchers for all the partitions
      replicaFetcherManager.removeFetcherForPartitions(partitionStates.keySet.map(_.topicPartition))
      stateChangeLogger.info(s"Stopped fetchers as part of LeaderAndIsr request correlationId $correlationId from " +
        s"controller $controllerId epoch $controllerEpoch as part of the become-leader transition for " +
        s"${partitionStates.size} partitions")
      // Update the partition information to be the leader
      partitionStates.foreachEntry { (partition, partitionState) =>
        try {
          if (partition.makeLeader(partitionState, highWatermarkCheckpoints, topicIds(partitionState.topicName))) {
            partitionsToMakeLeaders += partition
          }
        } catch {
          case e: KafkaStorageException =>
            stateChangeLogger.error(s"Skipped the become-leader state change with " +
              s"correlation id $correlationId from controller $controllerId epoch $controllerEpoch for partition ${partition.topicPartition} " +
              s"(last update controller epoch ${partitionState.controllerEpoch}) since " +
              s"the replica for the partition is offline due to storage error $e")
            // If there is an offline log directory, a Partition object may have been created and have been added
            // to `ReplicaManager.allPartitions` before `createLogIfNotExists()` failed to create local replica due
            // to KafkaStorageException. In this case `ReplicaManager.allPartitions` will map this topic-partition
            // to an empty Partition object. We need to map this topic-partition to OfflinePartition instead.
            markPartitionOffline(partition.topicPartition)
            responseMap.put(partition.topicPartition, Errors.KAFKA_STORAGE_ERROR)
        }
      }

    } catch {
      case e: Throwable =>
        partitionStates.keys.foreach { partition =>
          stateChangeLogger.error(s"Error while processing LeaderAndIsr request correlationId $correlationId received " +
            s"from controller $controllerId epoch $controllerEpoch for partition ${partition.topicPartition}", e)
        }
        // Re-throw the exception for it to be caught in KafkaApis
        throw e
    }

    if (traceEnabled)
      partitionStates.keys.foreach { partition =>
        stateChangeLogger.trace(s"Completed LeaderAndIsr request correlationId $correlationId from controller $controllerId " +
          s"epoch $controllerEpoch for the become-leader transition for partition ${partition.topicPartition}")
      }

    partitionsToMakeLeaders
  }

  /*
   * Make the current broker to become follower for a given set of partitions by:
   *
   * 1. Remove these partitions from the leader partitions set.
   * 2. Mark the replicas as followers so that no more data can be added from the producer clients.
   * 3. Stop fetchers for these partitions so that no more data can be added by the replica fetcher threads.
   * 4. Truncate the log and checkpoint offsets for these partitions.
   * 5. Clear the produce and fetch requests in the purgatory
   * 6. If the broker is not shutting down, add the fetcher to the new leaders.
   *
   * The ordering of doing these steps make sure that the replicas in transition will not
   * take any more messages before checkpointing offsets so that all messages before the checkpoint
   * are guaranteed to be flushed to disks
   *
   * If an unexpected error is thrown in this function, it will be propagated to KafkaApis where
   * the error message will be set on each partition since we do not know which partition caused it. Otherwise,
   * return the set of partitions that are made follower due to this method
   */
  private def makeFollowers(controllerId: Int,
                            controllerEpoch: Int,
                            partitionStates: Map[Partition, LeaderAndIsrRequest.PartitionState],
                            correlationId: Int,
                            responseMap: mutable.Map[TopicPartition, Errors],
                            highWatermarkCheckpoints: OffsetCheckpoints,
                            topicIds: String => Option[Uuid]) : Set[Partition] = {
    val traceLoggingEnabled = stateChangeLogger.isTraceEnabled
    partitionStates.foreachEntry { (partition, partitionState) =>
      if (traceLoggingEnabled)
        stateChangeLogger.trace(s"Handling LeaderAndIsr request correlationId $correlationId from controller $controllerId " +
          s"epoch $controllerEpoch starting the become-follower transition for partition ${partition.topicPartition} with leader " +
          s"${partitionState.leader}")
      responseMap.put(partition.topicPartition, Errors.NONE)
    }

    val partitionsToMakeFollower: mutable.Set[Partition] = mutable.Set()
    try {
      partitionStates.foreachEntry { (partition, partitionState) =>
        val newLeaderBrokerId = partitionState.leader
        try {
          if (metadataCache.hasAliveBroker(newLeaderBrokerId)) {
            // Only change partition state when the leader is available
            if (partition.makeFollower(partitionState, highWatermarkCheckpoints, topicIds(partitionState.topicName))) {
              // Skip invoking onBecomingFollower listeners as the listeners are not registered for zk-based features.
              partitionsToMakeFollower += partition
            }
          } else {
            // The leader broker should always be present in the metadata cache.
            // If not, we should record the error message and abort the transition process for this partition
            stateChangeLogger.error(s"Received LeaderAndIsrRequest with correlation id $correlationId from " +
              s"controller $controllerId epoch $controllerEpoch for partition ${partition.topicPartition} " +
              s"(last update controller epoch ${partitionState.controllerEpoch}) " +
              s"but cannot become follower since the new leader $newLeaderBrokerId is unavailable.")
            // Create the local replica even if the leader is unavailable. This is required to ensure that we include
            // the partition's high watermark in the checkpoint file (see KAFKA-1647)
            partition.createLogIfNotExists(isNew = partitionState.isNew, isFutureReplica = false,
              highWatermarkCheckpoints, topicIds(partitionState.topicName))
          }
        } catch {
          case e: KafkaStorageException =>
            stateChangeLogger.error(s"Skipped the become-follower state change with correlation id $correlationId from " +
              s"controller $controllerId epoch $controllerEpoch for partition ${partition.topicPartition} " +
              s"(last update controller epoch ${partitionState.controllerEpoch}) with leader " +
              s"$newLeaderBrokerId since the replica for the partition is offline due to storage error $e")
            // If there is an offline log directory, a Partition object may have been created and have been added
            // to `ReplicaManager.allPartitions` before `createLogIfNotExists()` failed to create local replica due
            // to KafkaStorageException. In this case `ReplicaManager.allPartitions` will map this topic-partition
            // to an empty Partition object. We need to map this topic-partition to OfflinePartition instead.
            markPartitionOffline(partition.topicPartition)
            responseMap.put(partition.topicPartition, Errors.KAFKA_STORAGE_ERROR)
        }
      }

      // Stopping the fetchers must be done first in order to initialize the fetch
      // position correctly.
      replicaFetcherManager.removeFetcherForPartitions(partitionsToMakeFollower.map(_.topicPartition))
      stateChangeLogger.info(s"Stopped fetchers as part of become-follower request from controller $controllerId " +
        s"epoch $controllerEpoch with correlation id $correlationId for ${partitionsToMakeFollower.size} partitions")

      partitionsToMakeFollower.foreach { partition =>
        completeDelayedOperationsWhenNotPartitionLeader(partition.topicPartition, partition.topicId)
      }

      if (isShuttingDown.get()) {
        if (traceLoggingEnabled) {
          partitionsToMakeFollower.foreach { partition =>
            stateChangeLogger.trace(s"Skipped the adding-fetcher step of the become-follower state " +
              s"change with correlation id $correlationId from controller $controllerId epoch $controllerEpoch for " +
              s"partition ${partition.topicPartition} with leader ${partitionStates(partition).leader} " +
              "since it is shutting down")
          }
        }
      } else {
        // we do not need to check if the leader exists again since this has been done at the beginning of this process
        val partitionsToMakeFollowerWithLeaderAndOffset = partitionsToMakeFollower.map { partition =>
          val leaderNode = partition.leaderReplicaIdOpt match {
            case Some(leaderId) => metadataCache.getAliveBrokerNode(leaderId, config.interBrokerListenerName).orElse(Node.noNode())
            case None => Node.noNode()
          }
          val leader = new BrokerEndPoint(leaderNode.id(), leaderNode.host(), leaderNode.port())
          val log = partition.localLogOrException
          val fetchOffset = initialFetchOffset(log)
          partition.topicPartition -> InitialFetchState(topicIds(partition.topic), leader, partition.getLeaderEpoch, fetchOffset)
        }.toMap

        replicaFetcherManager.addFetcherForPartitions(partitionsToMakeFollowerWithLeaderAndOffset)
      }
    } catch {
      case e: Throwable =>
        stateChangeLogger.error(s"Error while processing LeaderAndIsr request with correlationId $correlationId " +
          s"received from controller $controllerId epoch $controllerEpoch", e)
        // Re-throw the exception for it to be caught in KafkaApis
        throw e
    }

    if (traceLoggingEnabled)
      partitionStates.keys.foreach { partition =>
        stateChangeLogger.trace(s"Completed LeaderAndIsr request correlationId $correlationId from controller $controllerId " +
          s"epoch $controllerEpoch for the become-follower transition for partition ${partition.topicPartition} with leader " +
          s"${partitionStates(partition).leader}")
      }

    partitionsToMakeFollower
  }

  private def updateTopicIdForFollowers(controllerId: Int,
                                        controllerEpoch: Int,
                                        partitions: Set[Partition],
                                        correlationId: Int,
                                        topicIds: String => Option[Uuid]): Unit = {
    val traceLoggingEnabled = stateChangeLogger.isTraceEnabled

    try {
      if (isShuttingDown.get()) {
        if (traceLoggingEnabled) {
          partitions.foreach { partition =>
            stateChangeLogger.trace(s"Skipped the update topic ID step of the become-follower state " +
              s"change with correlation id $correlationId from controller $controllerId epoch $controllerEpoch for " +
              s"partition ${partition.topicPartition} since it is shutting down")
          }
        }
      } else {
        val partitionsToUpdateFollowerWithLeader = mutable.Map.empty[TopicPartition, Int]
        partitions.foreach { partition =>
          partition.leaderReplicaIdOpt.foreach { leader =>
            if (metadataCache.hasAliveBroker(leader)) {
              partitionsToUpdateFollowerWithLeader += partition.topicPartition -> leader
            }
          }
        }
        replicaFetcherManager.maybeUpdateTopicIds(partitionsToUpdateFollowerWithLeader, topicIds)
      }
    } catch {
      case e: Throwable =>
        stateChangeLogger.error(s"Error while processing LeaderAndIsr request with correlationId $correlationId " +
          s"received from controller $controllerId epoch $controllerEpoch when trying to update topic IDs in the fetchers", e)
        // Re-throw the exception for it to be caught in KafkaApis
        throw e
    }
  }

  /**
   * From IBP 2.7 onwards, we send latest fetch epoch in the request and truncate if a
   * diverging epoch is returned in the response, avoiding the need for a separate
   * OffsetForLeaderEpoch request.
   */
  protected def initialFetchOffset(log: UnifiedLog): Long = {
    if (log.latestEpoch.isPresent)
      log.logEndOffset
    else
      log.highWatermark
  }

  private def maybeShrinkIsr(): Unit = {
    trace("Evaluating ISR list of partitions to see which replicas can be removed from the ISR")

    // Shrink ISRs for non offline partitions
    allPartitions.forEach { (topicPartition, _) =>
      if (!isInklessTopic(topicPartition.topic()))
        onlinePartition(topicPartition).foreach(_.maybeShrinkIsr())
    }
  }

  private def leaderPartitionsIterator: Iterator[Partition] =
    onlinePartitionsIterator.filter(_.leaderLogIfLocal.isDefined)

  def getLogEndOffset(topicPartition: TopicPartition): Option[Long] =
    onlinePartition(topicPartition).flatMap(_.leaderLogIfLocal.map(_.logEndOffset))

  // Flushes the highwatermark value for all partitions to the highwatermark file
  def checkpointHighWatermarks(): Unit = {
    def putHw(logDirToCheckpoints: mutable.AnyRefMap[String, mutable.AnyRefMap[TopicPartition, JLong]],
              log: UnifiedLog): Unit = {
      val checkpoints = logDirToCheckpoints.getOrElseUpdate(log.parentDir,
        new mutable.AnyRefMap[TopicPartition, JLong]())
      checkpoints.put(log.topicPartition, log.highWatermark)
    }

    val logDirToHws = new mutable.AnyRefMap[String, mutable.AnyRefMap[TopicPartition, JLong]](
      allPartitions.size)
    onlinePartitionsIterator.foreach { partition =>
      partition.log.foreach(putHw(logDirToHws, _))
      partition.futureLog.foreach(putHw(logDirToHws, _))
    }

    for ((logDir, hws) <- logDirToHws) {
      try highWatermarkCheckpoints.get(logDir).foreach(_.write(hws.asJava))
      catch {
        case e: KafkaStorageException =>
          error(s"Error while writing to highwatermark file in directory $logDir", e)
      }
    }
  }

  def markPartitionOffline(tp: TopicPartition): Unit = replicaStateChangeLock synchronized {
    allPartitions.get(tp) match {
      case HostedPartition.Online(partition) =>
        allPartitions.put(tp, HostedPartition.Offline(Some(partition)))
        partition.markOffline()
      case _ =>
        allPartitions.put(tp, HostedPartition.Offline(None))
    }
  }

  /**
   * The log directory failure handler for the replica
   *
   * @param dir                     the absolute path of the log directory
   * @param notifyController        check if we need to send notification to the Controller (needed for unit test)
   */
  def handleLogDirFailure(dir: String, notifyController: Boolean = true): Unit = {
    if (!logManager.isLogDirOnline(dir))
      return
    // retrieve the UUID here because logManager.handleLogDirFailure handler removes it
    val uuid = logManager.directoryId(dir)
    warn(s"Stopping serving replicas in dir $dir with uuid $uuid because the log directory has failed.")
    replicaStateChangeLock synchronized {
      val newOfflinePartitions = onlinePartitionsIterator.filter { partition =>
        partition.log.exists { _.parentDir == dir }
      }.map(_.topicPartition).toSet

      val partitionsWithOfflineFutureReplica = onlinePartitionsIterator.filter { partition =>
        partition.futureLog.exists { _.parentDir == dir }
      }.toSet

      replicaFetcherManager.removeFetcherForPartitions(newOfflinePartitions)
      replicaAlterLogDirsManager.removeFetcherForPartitions(newOfflinePartitions ++ partitionsWithOfflineFutureReplica.map(_.topicPartition))

      partitionsWithOfflineFutureReplica.foreach(partition => partition.removeFutureLocalReplica(deleteFromLogDir = false))
      newOfflinePartitions.foreach { topicPartition =>
        markPartitionOffline(topicPartition)
      }
      newOfflinePartitions.map(_.topic).foreach { topic: String =>
        maybeRemoveTopicMetrics(topic)
      }
      highWatermarkCheckpoints = highWatermarkCheckpoints.filter { case (checkpointDir, _) => checkpointDir != dir }

      warn(s"Broker $localBrokerId stopped fetcher for partitions ${newOfflinePartitions.mkString(",")} and stopped moving logs " +
           s"for partitions ${partitionsWithOfflineFutureReplica.mkString(",")} because they are in the failed log directory $dir.")
    }
    logManager.handleLogDirFailure(dir)
    if (dir == new File(config.metadataLogDir).getAbsolutePath && config.processRoles.nonEmpty) {
      fatal(s"Shutdown broker because the metadata log dir $dir has failed")
      Exit.halt(1)
    }

    if (notifyController) {
      if (uuid.isDefined) {
        directoryEventHandler.handleFailure(uuid.get)
      } else {
        fatal(s"Unable to propagate directory failure disabled because directory $dir has no UUID")
        Exit.halt(1)
      }
    }
    warn(s"Stopped serving replicas in dir $dir")
  }

  def removeMetrics(): Unit = {
    ReplicaManager.MetricNames.foreach(metricsGroup.removeMetric)
  }

  def beginControlledShutdown(): Unit = {
    isInControlledShutdown = true
  }

  // High watermark do not need to be checkpointed only when under unit tests
  def shutdown(checkpointHW: Boolean = true): Unit = {
    info("Shutting down")
    removeMetrics()
    if (logDirFailureHandler != null)
      logDirFailureHandler.shutdown()
    replicaFetcherManager.shutdown()
    replicaAlterLogDirsManager.shutdown()
    delayedFetchPurgatory.shutdown()
    delayedRemoteFetchPurgatory.shutdown()
    delayedRemoteListOffsetsPurgatory.shutdown()
    delayedProducePurgatory.shutdown()
    delayedDeleteRecordsPurgatory.shutdown()
    delayedShareFetchPurgatory.shutdown()
    if (checkpointHW)
      checkpointHighWatermarks()
    replicaSelectorPlugin.foreach(_.close)
    removeAllTopicMetrics()
    addPartitionsToTxnManager.foreach(_.shutdown())
    inklessAppendHandler.foreach(_.close())
    inklessFetchHandler.foreach(_.close())
    inklessFetchOffsetHandler.foreach(_.close())
    inklessRetentionEnforcer.foreach(_.close())
    inklessSharedState.foreach(_.close())
    info("Shut down completely")
  }

  private def removeAllTopicMetrics(): Unit = {
    val allTopics = new util.HashSet[String]
    allPartitions.forEach((partition, _) =>
      if (allTopics.add(partition.topic())) {
        brokerTopicStats.removeMetrics(partition.topic())
      })
  }

  protected def createReplicaFetcherManager(metrics: Metrics, time: Time, threadNamePrefix: Option[String], quotaManager: ReplicationQuotaManager) = {
    new ReplicaFetcherManager(config, this, metrics, time, threadNamePrefix, quotaManager, () => metadataCache.metadataVersion(), brokerEpochSupplier)
  }

  protected def createReplicaAlterLogDirsManager(quotaManager: ReplicationQuotaManager, brokerTopicStats: BrokerTopicStats) = {
    new ReplicaAlterLogDirsManager(config, this, quotaManager, brokerTopicStats, directoryEventHandler)
  }

  private def createReplicaSelector(metrics: Metrics): Option[Plugin[ReplicaSelector]] = {
    config.replicaSelectorClassName.map { className =>
      val tmpReplicaSelector: ReplicaSelector = Utils.newInstance(className, classOf[ReplicaSelector])
      tmpReplicaSelector.configure(config.originals())
      Plugin.wrapInstance(tmpReplicaSelector, metrics, ReplicationConfigs.REPLICA_SELECTOR_CLASS_CONFIG)
    }
  }

  def lastOffsetForLeaderEpoch(
    requestedEpochInfo: Seq[OffsetForLeaderTopic]
  ): Seq[OffsetForLeaderTopicResult] = {
    // First start job to collect inkless offsets
    val inklessFetchOffsetHandlerJob: Option[FetchOffsetHandler.Job] = inklessFetchOffsetHandler.map(_.createJob())

    val inklessOffsetFuturesByTopic: Map[String, Seq[CompletableFuture[EpochEndOffset]]] = requestedEpochInfo
      .filter { offsetForLeaderTopic =>
        inklessFetchOffsetHandlerJob.exists(_.mustHandle(offsetForLeaderTopic.topic))
      }
      .map { offsetForLeaderTopic =>
        val topic = offsetForLeaderTopic.topic()
        val partitions = offsetForLeaderTopic.partitions.asScala.map { offsetForLeaderPartition =>
          val partition = offsetForLeaderPartition.partition
          val leaderEpoch = offsetForLeaderPartition.leaderEpoch()
          val tp = new TopicPartition(topic, partition)

          val partitionRequest = new ListOffsetsPartition()
            .setPartitionIndex(partition)
            .setCurrentLeaderEpoch(LeaderAndIsr.INITIAL_LEADER_EPOCH)
            .setTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP)

          inklessFetchOffsetHandlerJob.get
            .add(tp, partitionRequest)
            .thenApply[EpochEndOffset](epochEndOffset => {
              val endOffset = epochEndOffset.timestampAndOffset().get().offset
              val errorCode = epochEndOffset.exception()
                .map[Errors](e => Errors.forException(e))
                .orElse(Errors.NONE)
                .code
              new EpochEndOffset()
                .setPartition(tp.partition())
                .setErrorCode(errorCode)
                .setLeaderEpoch(leaderEpoch)
                .setEndOffset(endOffset)
            })
        }.toSeq
        topic -> partitions
      }.toMap

    inklessFetchOffsetHandlerJob.foreach(_.start())

    // Then build response
    requestedEpochInfo
      .map { offsetForLeaderTopic =>
        if (isInklessTopic(offsetForLeaderTopic.topic())) {
          inklessOffsetFuturesByTopic.get(offsetForLeaderTopic.topic())
            .map { futures =>
              // Wait until offsetForLeaderTopic is completed
              // TODO check if this can be bound -- currently as last offset is available locally on classic topics
              //   it is not needed to have a purgatory; but it may be needed.
              val offsets = futures.map(_.get())
              new OffsetForLeaderTopicResult()
                .setTopic(offsetForLeaderTopic.topic())
                .setPartitions(offsets.toList.asJava)
            }
            .get
        } else {
          val partitions = offsetForLeaderTopic.partitions.asScala.map { offsetForLeaderPartition =>
            val tp = new TopicPartition(offsetForLeaderTopic.topic, offsetForLeaderPartition.partition)
            getPartition(tp) match {
              case HostedPartition.Online(partition) =>
                val currentLeaderEpochOpt =
                  if (offsetForLeaderPartition.currentLeaderEpoch == RecordBatch.NO_PARTITION_LEADER_EPOCH)
                    Optional.empty[Integer]
                  else
                    Optional.of[Integer](offsetForLeaderPartition.currentLeaderEpoch)

                partition.lastOffsetForLeaderEpoch(
                  currentLeaderEpochOpt,
                  offsetForLeaderPartition.leaderEpoch,
                  fetchOnlyFromLeader = true)

              case HostedPartition.Offline(_) =>
                new EpochEndOffset()
                  .setPartition(offsetForLeaderPartition.partition)
                  .setErrorCode(Errors.KAFKA_STORAGE_ERROR.code)

              case HostedPartition.None if metadataCache.contains(tp) =>
                new EpochEndOffset()
                  .setPartition(offsetForLeaderPartition.partition)
                  .setErrorCode(Errors.NOT_LEADER_OR_FOLLOWER.code)

              case HostedPartition.None =>
                new EpochEndOffset()
                  .setPartition(offsetForLeaderPartition.partition)
                  .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
            }
          }

          new OffsetForLeaderTopicResult()
            .setTopic(offsetForLeaderTopic.topic)
            .setPartitions(partitions.toList.asJava)
        }
      }
  }

  private def isInklessTopic(topic: String): Boolean = {
    inklessSharedState.exists(_.metadata().isInklessTopic(topic))
  }

  def activeProducerState(requestPartition: TopicPartition): DescribeProducersResponseData.PartitionResponse = {
    getPartitionOrError(requestPartition) match {
      case Left(error) => new DescribeProducersResponseData.PartitionResponse()
        .setPartitionIndex(requestPartition.partition)
        .setErrorCode(error.code)
      case Right(partition) => partition.activeProducerState
    }
  }

  private[kafka] def getOrCreatePartition(tp: TopicPartition,
                                          delta: TopicsDelta,
                                          topicId: Uuid): Option[(Partition, Boolean)] = {
    getPartition(tp) match {
      case HostedPartition.Offline(offlinePartition) =>
        if (offlinePartition.flatMap(p => p.topicId).contains(topicId)) {
          stateChangeLogger.warn(s"Unable to bring up new local leader $tp " +
            s"with topic id $topicId because it resides in an offline log " +
            "directory.")
          None
        } else {
          stateChangeLogger.info(s"Creating new partition $tp with topic id " + s"$topicId." +
            s"A topic with the same name but different id exists but it resides in an offline log " +
            s"directory.")
          val partition = Partition(new TopicIdPartition(topicId, tp), time, this)
          allPartitions.put(tp, HostedPartition.Online(partition))
          Some(partition, true)
        }

      case HostedPartition.Online(partition) =>
        if (partition.topicId.exists(_ != topicId)) {
          // Note: Partition#topicId will be None here if the Log object for this partition
          // has not been created.
          throw new IllegalStateException(s"Topic $tp exists, but its ID is " +
            s"${partition.topicId.get}, not $topicId as expected")
        }
        Some(partition, false)

      case HostedPartition.None =>
        if (delta.image().topicsById().containsKey(topicId)) {
          stateChangeLogger.error(s"Expected partition $tp with topic id " +
            s"$topicId to exist, but it was missing. Creating...")
        } else {
          stateChangeLogger.info(s"Creating new partition $tp with topic id " +
            s"$topicId.")
        }
        // it's a partition that we don't know about yet, so create it and mark it online
        val partition = Partition(new TopicIdPartition(topicId, tp), time, this)
        allPartitions.put(tp, HostedPartition.Online(partition))
        Some(partition, true)
    }
  }

  /**
   * Apply a KRaft topic change delta.
   *
   * @param delta           The delta to apply.
   * @param newImage        The new metadata image.
   */
  def applyDelta(delta: TopicsDelta, newImage: MetadataImage): Unit = {
    // Before taking the lock, compute the local changes
    val localChanges = delta.localChanges(config.nodeId)
    val metadataVersion = newImage.features().metadataVersionOrThrow()

    replicaStateChangeLock.synchronized {
      // Handle deleted partitions. We need to do this first because we might subsequently
      // create new partitions with the same names as the ones we are deleting here.
      if (!localChanges.deletes.isEmpty) {
        val deletes = localChanges.deletes.asScala
          .map { tp =>
            val isCurrentLeader = Option(delta.image().getTopic(tp.topic()))
              .map(image => image.partitions().get(tp.partition()))
              .exists(partition => partition.leader == config.nodeId)
            val deleteRemoteLog = delta.topicWasDeleted(tp.topic()) && isCurrentLeader
            new StopPartition(tp, true, deleteRemoteLog, false)
          }
          .toSet
        stateChangeLogger.info(s"Deleting ${deletes.size} partition(s).")
        stopPartitions(deletes).foreachEntry { (topicPartition, e) =>
          if (e.isInstanceOf[KafkaStorageException]) {
            stateChangeLogger.error(s"Unable to delete replica $topicPartition because " +
              "the local replica for the partition is in an offline log directory")
          } else {
            stateChangeLogger.error(s"Unable to delete replica $topicPartition because " +
              s"we got an unexpected ${e.getClass.getName} exception: ${e.getMessage}")
          }
        }
      }

      // Handle partitions which we are now the leader or follower for.
      if (!localChanges.leaders.isEmpty || !localChanges.followers.isEmpty) {
        val lazyOffsetCheckpoints = new LazyOffsetCheckpoints(this.highWatermarkCheckpoints.asJava)
        val leaderChangedPartitions = new mutable.HashSet[Partition]
        val followerChangedPartitions = new mutable.HashSet[Partition]
        if (!localChanges.leaders.isEmpty) {
          applyLocalLeadersDelta(leaderChangedPartitions, delta, lazyOffsetCheckpoints, localChanges.leaders.asScala, localChanges.directoryIds.asScala)
        }
        if (!localChanges.followers.isEmpty) {
          applyLocalFollowersDelta(followerChangedPartitions, newImage, delta, lazyOffsetCheckpoints, localChanges.followers.asScala, localChanges.directoryIds.asScala)
        }

        maybeAddLogDirFetchers(leaderChangedPartitions ++ followerChangedPartitions, lazyOffsetCheckpoints,
          name => Option(newImage.topics().getTopic(name)).map(_.id()))

        replicaFetcherManager.shutdownIdleFetcherThreads()
        replicaAlterLogDirsManager.shutdownIdleFetcherThreads()

        remoteLogManager.foreach(rlm => rlm.onLeadershipChange((leaderChangedPartitions.toSet: Set[TopicPartitionLog]).asJava, (followerChangedPartitions.toSet: Set[TopicPartitionLog]).asJava, localChanges.topicIds()))
      }

      if (metadataVersion.isDirectoryAssignmentSupported) {
        // We only want to update the directoryIds if DirectoryAssignment is supported!
        localChanges.directoryIds.forEach(maybeUpdateTopicAssignment)
      }
    }
  }

  private def applyLocalLeadersDelta(
    changedPartitions: mutable.Set[Partition],
    delta: TopicsDelta,
    offsetCheckpoints: OffsetCheckpoints,
    localLeaders: mutable.Map[TopicPartition, LocalReplicaChanges.PartitionInfo],
    directoryIds: mutable.Map[TopicIdPartition, Uuid]
  ): Unit = {
    stateChangeLogger.info(s"Transitioning ${localLeaders.size} partition(s) to " +
      "local leaders.")
    replicaFetcherManager.removeFetcherForPartitions(localLeaders.keySet)
    localLeaders.foreachEntry { (tp, info) =>
      if (!isInklessTopic(tp.topic()))
        getOrCreatePartition(tp, delta, info.topicId).foreach { case (partition, isNew) =>
          try {
            val state = info.partition.toLeaderAndIsrPartitionState(tp, isNew)
            val partitionAssignedDirectoryId = directoryIds.find(_._1.topicPartition() == tp).map(_._2)
            partition.makeLeader(state, offsetCheckpoints, Some(info.topicId), partitionAssignedDirectoryId)

            changedPartitions.add(partition)
          } catch {
            case e: KafkaStorageException =>
              stateChangeLogger.info(s"Skipped the become-leader state change for $tp " +
                s"with topic id ${info.topicId} due to a storage error ${e.getMessage}")
              // If there is an offline log directory, a Partition object may have been created by
              // `getOrCreatePartition()` before `createLogIfNotExists()` failed to create local replica due
              // to KafkaStorageException. In this case `ReplicaManager.allPartitions` will map this topic-partition
              // to an empty Partition object. We need to map this topic-partition to OfflinePartition instead.
              markPartitionOffline(tp)
          }
        }
    }
  }

  private def applyLocalFollowersDelta(
    changedPartitions: mutable.Set[Partition],
    newImage: MetadataImage,
    delta: TopicsDelta,
    offsetCheckpoints: OffsetCheckpoints,
    localFollowers: mutable.Map[TopicPartition, LocalReplicaChanges.PartitionInfo],
    directoryIds: mutable.Map[TopicIdPartition, Uuid]
  ): Unit = {
    stateChangeLogger.info(s"Transitioning ${localFollowers.size} partition(s) to " +
      "local followers.")
    val partitionsToStartFetching = new mutable.HashMap[TopicPartition, Partition]
    val partitionsToStopFetching = new mutable.HashMap[TopicPartition, Boolean]
    val followerTopicSet = new mutable.HashSet[String]
    localFollowers.foreachEntry { (tp, info) =>
      if (!isInklessTopic(tp.topic()))
        getOrCreatePartition(tp, delta, info.topicId).foreach { case (partition, isNew) =>
          try {
            followerTopicSet.add(tp.topic)

            // We always update the follower state.
            // - This ensure that a replica with no leader can step down;
            // - This also ensures that the local replica is created even if the leader
            //   is unavailable. This is required to ensure that we include the partition's
            //   high watermark in the checkpoint file (see KAFKA-1647).
            val state = info.partition.toLeaderAndIsrPartitionState(tp, isNew)
            val partitionAssignedDirectoryId = directoryIds.find(_._1.topicPartition() == tp).map(_._2)
            val isNewLeaderEpoch = partition.makeFollower(state, offsetCheckpoints, Some(info.topicId), partitionAssignedDirectoryId)

            if (isInControlledShutdown && (info.partition.leader == NO_LEADER ||
              !info.partition.isr.contains(config.brokerId))) {
              // During controlled shutdown, replica with no leaders and replica
              // where this broker is not in the ISR are stopped.
              partitionsToStopFetching.put(tp, false)
            } else if (isNewLeaderEpoch) {
              // Invoke the follower transition listeners for the partition.
              partition.invokeOnBecomingFollowerListeners()
              // Otherwise, fetcher is restarted if the leader epoch has changed.
              partitionsToStartFetching.put(tp, partition)
            }

            changedPartitions.add(partition)
          } catch {
            case e: KafkaStorageException =>
              stateChangeLogger.error(s"Unable to start fetching $tp " +
                s"with topic ID ${info.topicId} due to a storage error ${e.getMessage}", e)
              replicaFetcherManager.addFailedPartition(tp)
              // If there is an offline log directory, a Partition object may have been created by
              // `getOrCreatePartition()` before `createLogIfNotExists()` failed to create local replica due
              // to KafkaStorageException. In this case `ReplicaManager.allPartitions` will map this topic-partition
              // to an empty Partition object. We need to map this topic-partition to OfflinePartition instead.
              markPartitionOffline(tp)

            case e: Throwable =>
              stateChangeLogger.error(s"Unable to start fetching $tp " +
                s"with topic ID ${info.topicId} due to ${e.getClass.getSimpleName}", e)
              replicaFetcherManager.addFailedPartition(tp)
          }
        }
    }

    if (partitionsToStartFetching.nonEmpty) {
      // Stopping the fetchers must be done first in order to initialize the fetch
      // position correctly.
      replicaFetcherManager.removeFetcherForPartitions(partitionsToStartFetching.keySet)
      stateChangeLogger.info(s"Stopped fetchers as part of become-follower for ${partitionsToStartFetching.size} partitions")

      val listenerName = config.interBrokerListenerName.value
      val partitionAndOffsets = new mutable.HashMap[TopicPartition, InitialFetchState]

      partitionsToStartFetching.foreachEntry { (topicPartition, partition) =>
        val nodeOpt = partition.leaderReplicaIdOpt
          .flatMap(leaderId => Option(newImage.cluster.broker(leaderId)))
          .flatMap(_.node(listenerName).toScala)

        nodeOpt match {
          case Some(node) =>
            val log = partition.localLogOrException
            partitionAndOffsets.put(topicPartition, InitialFetchState(
              log.topicId.toScala,
              new BrokerEndPoint(node.id, node.host, node.port),
              partition.getLeaderEpoch,
              initialFetchOffset(log)
            ))
          case None =>
            stateChangeLogger.trace(s"Unable to start fetching $topicPartition with topic ID ${partition.topicId} " +
              s"from leader ${partition.leaderReplicaIdOpt} because it is not alive.")
        }
      }

      replicaFetcherManager.addFetcherForPartitions(partitionAndOffsets)
      stateChangeLogger.info(s"Started fetchers as part of become-follower for ${partitionsToStartFetching.size} partitions")

      partitionsToStartFetching.foreach{ case (topicPartition, partition) =>
        completeDelayedOperationsWhenNotPartitionLeader(topicPartition, partition.topicId)}

      updateLeaderAndFollowerMetrics(followerTopicSet)
    }

    if (partitionsToStopFetching.nonEmpty) {
      val partitionsToStop = partitionsToStopFetching.map { case (tp, deleteLocalLog) => new StopPartition(tp, deleteLocalLog, false, false) }.toSet
      stopPartitions(partitionsToStop)
      stateChangeLogger.info(s"Stopped fetchers as part of controlled shutdown for ${partitionsToStop.size} partitions")
    }
  }

  private def maybeUpdateTopicAssignment(partition: TopicIdPartition, partitionDirectoryId: Uuid): Unit = {
    for {
      topicPartitionActualLog <- logManager.getLog(partition.topicPartition())
      topicPartitionActualDirectoryId <- logManager.directoryId(topicPartitionActualLog.dir.getParent)
      if partitionDirectoryId != topicPartitionActualDirectoryId
    } directoryEventHandler.handleAssignment(
      new common.TopicIdPartition(partition.topicId, partition.partition()),
      topicPartitionActualDirectoryId,
      "Applying metadata delta",
      () => ()
    )
  }

  def findInklessBatches(requests: Seq[FindBatchRequest], maxBytes: Int): Option[util.List[FindBatchResponse]] = {
    inklessSharedState.map { sharedState =>
      sharedState.controlPlane().findBatches(requests.asJava, maxBytes)
    }
  }

}
