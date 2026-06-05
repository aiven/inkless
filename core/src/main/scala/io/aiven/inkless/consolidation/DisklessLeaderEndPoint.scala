/*
 * Inkless
 * Copyright (C) 2024 - 2026 Aiven OY
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package io.aiven.inkless.consolidation

import io.aiven.inkless.consume.{FetchHandler, FetchOffsetHandler}
import kafka.server.{KafkaConfig, ReplicaManager, ReplicaQuota}
import kafka.utils.Logging
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.errors.{KafkaStorageException, UnknownTopicOrPartitionException}
import org.apache.kafka.common.message.{FetchResponseData, OffsetForLeaderEpochRequestData}
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsPartition
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.FileRecords.TimestampAndOffset
import org.apache.kafka.common.requests.{FetchRequest, FetchResponse, ListOffsetsRequest, OffsetsForLeaderEpochResponse}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.metadata.{LeaderAndIsr, PartitionRegistration}
import org.apache.kafka.server.common.{MetadataVersion, OffsetAndEpoch}
import org.apache.kafka.server.network.BrokerEndPoint
import org.apache.kafka.server.storage.log.{FetchIsolation, FetchParams}
import org.apache.kafka.server.{LeaderEndPoint, PartitionFetchState, ReplicaFetch, ResultWithPartitions}
import org.apache.kafka.storage.internals.log.OffsetResultHolder.FileRecordsOrError
import org.apache.kafka.storage.internals.log.UnifiedLog

import java.util
import java.util.Optional
import java.util.concurrent.CompletableFuture
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.Try

/**
 * Leader endpoint for consolidation fetching from Inkless (object storage) on this broker.
 * [[FetchHandler]] performs the same diskless fetch path as the broker’s main fetch pipeline;
 * [[FetchOffsetHandler]] backs list-offsets style APIs used by [[kafka.server.AbstractFetcherThread]].
 *
 * Fetch and offset handler instances are owned by [[kafka.server.ReplicaManager]]; this class does not close them.
 */
class DisklessLeaderEndPoint(
  brokerEndPoint: BrokerEndPoint,
  fetchHandler: FetchHandler,
  fetchOffsetHandler: FetchOffsetHandler,
  replicaManager: ReplicaManager,
  brokerConfig: KafkaConfig,
  quota: ReplicaQuota,
  metadataVersionSupplier: () => MetadataVersion,
  brokerEpochSupplier: () => Long
) extends LeaderEndPoint with Logging {

  private val replicaId = brokerConfig.brokerId
  private val maxWait = brokerConfig.disklessConsolidationFetchMaxWaitMs
  private val minBytes = brokerConfig.disklessConsolidationFetchMinBytes
  private val maxBytes = brokerConfig.disklessConsolidationFetchResponseMaxBytes
  private val fetchSize = brokerConfig.disklessConsolidationFetchMaxBytes

  override def isTruncationOnFetchSupported: Boolean = false

  override def initiateClose(): Unit = ()

  override def close(): Unit = ()

  override def brokerEndPoint(): BrokerEndPoint = brokerEndPoint

  override def fetch(fetchRequest: FetchRequest.Builder): util.Map[TopicPartition, FetchResponseData.PartitionData] = {
    val request = fetchRequest.build()
    val topicNames = new mutable.HashMap[Uuid, String]()
    request.data.topics.forEach { topic =>
      topicNames.put(topic.topicId, topic.topic)
    }
    val fetchInfos = request.fetchData(topicNames.asJava)

    val fetchParams = new FetchParams(
      FetchRequest.FUTURE_LOCAL_REPLICA_ID,
      -1,
      0L,
      request.minBytes,
      request.maxBytes,
      FetchIsolation.LOG_END,
      Optional.empty()
    )

    val response = fetchHandler.handle(fetchParams, fetchInfos).get()
    response.asScala.map { case (tp, data) =>
      val abortedTransactions = data.abortedTransactions.orElse(null)
      val lastStableOffset: Long = data.lastStableOffset.orElse(FetchResponse.INVALID_LAST_STABLE_OFFSET)
      val fetchResponseData = new FetchResponseData.PartitionData()
        .setPartitionIndex(tp.topicPartition.partition)
        .setErrorCode(data.error.code)
        .setHighWatermark(data.highWatermark)
        .setLastStableOffset(lastStableOffset)
        .setAbortedTransactions(abortedTransactions)
        .setRecords(data.records)
      // set local LSO if possible instead of the diskless start offset that is data.logStartOffset
      replicaManager.getPartitionOrError(tp.topicPartition) match {
        case Left(error) =>
          // If we couldn't read the partition, then we won't be able to set the log start offset
          // of the unified log. If there is no error coming from diskless, then we can propagate
          // the error in the partition, but otherwise don't mask it.
          if (fetchResponseData.errorCode == Errors.NONE.code) {
            fetchResponseData.setErrorCode(error.code)
          }
          fetchResponseData.setLogStartOffset(UnifiedLog.UNKNOWN_OFFSET)
        case Right(partition) =>
          val logStartOffset = Try(partition.localLogOrException).toOption match {
            case Some(localLog) => localLog.logStartOffset
            case None =>
                    logger.warn("Local log unavailable for topic-partition {}, returning unknown log start offset", tp.topicPartition)
                    UnifiedLog.UNKNOWN_OFFSET
          }
          if (fetchResponseData.errorCode == Errors.NONE.code) {
            // in case of an inconsistency log an error, set an unknown offset and also return unknown server error
            if (logStartOffset > data.highWatermark) {
              logger.error("Local log start offset ({}) is higher than high watermark ({}) for topic-partition {}, this may indicate a transient inconsistency during migration",
                logStartOffset, data.highWatermark, partition.topicPartition)
              fetchResponseData.setLogStartOffset(UnifiedLog.UNKNOWN_OFFSET)
              fetchResponseData.setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code)
            } else {
              fetchResponseData.setLogStartOffset(logStartOffset)
            }
          } else {
            fetchResponseData.setLogStartOffset(UnifiedLog.UNKNOWN_OFFSET)
          }
      }
      tp.topicPartition -> fetchResponseData
    }.toMap.asJava
  }

  override def fetchEarliestOffset(topicPartition: TopicPartition, currentLeaderEpoch: Int): OffsetAndEpoch =
    listDisklessOffset(topicPartition, currentLeaderEpoch, ListOffsetsRequest.EARLIEST_TIMESTAMP)

  override def fetchLatestOffset(topicPartition: TopicPartition, currentLeaderEpoch: Int): OffsetAndEpoch =
    listDisklessOffset(topicPartition, currentLeaderEpoch, ListOffsetsRequest.LATEST_TIMESTAMP)

  override def fetchEarliestLocalOffset(topicPartition: TopicPartition, currentLeaderEpoch: Int): OffsetAndEpoch =
    listDisklessOffset(topicPartition, currentLeaderEpoch, ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP)

  private def listDisklessOffset(topicPartition: TopicPartition, currentLeaderEpoch: Int, timestamp: Long): OffsetAndEpoch = {
    val job = fetchOffsetHandler.createJob()
    if (!job.mustHandle(topicPartition.topic)) {
      throw Errors.UNKNOWN_TOPIC_OR_PARTITION.exception()
    }
    val partitionRequest = new ListOffsetsPartition()
      .setPartitionIndex(topicPartition.partition)
      .setCurrentLeaderEpoch(currentLeaderEpoch)
      .setTimestamp(timestamp)
    val future = job.add(topicPartition, partitionRequest)
    job.start()
    val holder = future.get()
    if (holder.hasException) {
      throw Errors.forException(holder.exception().get()).exception()
    }
    val tao: TimestampAndOffset = holder.timestampAndOffset().get()
    new OffsetAndEpoch(tao.offset, tao.leaderEpoch.orElse(0))
  }

  /**
   * Answers OffsetsForLeaderEpoch for diskless consolidating partitions so followers can perform
   * standard divergence truncation against the diskless leader.
   *
   * A switched partition's local log is laid out as a classic prefix `[logStart, seal)` carrying the
   * original classic leader epochs, followed by the diskless region `[seal, LEO)` stamped with the
   * diskless leader epoch `E_d` captured at the switch (see [[ConsolidationFetcherThread]]). Two cases:
   *   - queried epoch `< E_d`: the epoch belongs to the classic prefix, whose lineage on the diskless
   *     leader ends at the seal, so the end offset is the seal. A follower whose stale classic tail
   *     runs past the seal truncates back to it. Collapsing every classic epoch to the seal (rather
   *     than the start of the next epoch, as standard OffsetsForLeaderEpoch would) is correct only
   *     because the classic prefix `[logStart, seal)` is committed and identical across all replicas,
   *     so intra-prefix divergence cannot occur.
   *   - queried epoch `>= E_d` (or a born-diskless partition with no captured epoch): the end offset is
   *     the current diskless LEO, fetched via a LATEST list-offsets call.
   */
  override def fetchEpochEndOffsets(
    partitions: util.Map[TopicPartition, OffsetForLeaderEpochRequestData.OffsetForLeaderPartition]
  ): util.Map[TopicPartition, EpochEndOffset] = {
    if (partitions.isEmpty) {
      return util.Map.of()
    }

    val job = fetchOffsetHandler.createJob()
    val futures = mutable.Map.empty[TopicPartition, CompletableFuture[FileRecordsOrError]]
    // Partitions whose queried epoch resolves to the seal without needing a diskless list-offsets call.
    val sealEndOffsets = mutable.Map.empty[TopicPartition, Long]

    partitions.forEach { (tp, epochData) =>
      if (epochData.leaderEpoch != OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH && job.mustHandle(tp.topic)) {
        val seal = replicaManager.classicToDisklessStartOffset(tp)
        val disklessLeaderEpoch = replicaManager.disklessLeaderEpoch(tp)
        // Upgrade caveat: partitions switched before this change carry a seal but no E_d (no tag 102),
        // so they fall through to the LATEST-LEO branch and keep the old (no divergence truncation)
        // behavior until they are re-switched. This is a safe fallback, not a correctness regression.
        if (seal >= 0 &&
            disklessLeaderEpoch != PartitionRegistration.NO_DISKLESS_LEADER_EPOCH &&
            epochData.leaderEpoch < disklessLeaderEpoch) {
          sealEndOffsets.put(tp, seal)
        } else {
          val partitionRequest = new ListOffsetsPartition()
            .setPartitionIndex(tp.partition)
            .setCurrentLeaderEpoch(LeaderAndIsr.INITIAL_LEADER_EPOCH)
            .setTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP)
          futures.put(tp, job.add(tp, partitionRequest))
        }
      }
    }

    job.start()

    partitions.asScala.map { case (tp, epochData) =>
      if (epochData.leaderEpoch == OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH) {
        tp -> new EpochEndOffset()
          .setPartition(tp.partition)
          .setErrorCode(Errors.NONE.code)
      } else if (sealEndOffsets.contains(tp)) {
        tp -> new EpochEndOffset()
          .setPartition(tp.partition)
          .setErrorCode(Errors.NONE.code)
          .setLeaderEpoch(epochData.leaderEpoch)
          .setEndOffset(sealEndOffsets(tp))
      } else if (!futures.contains(tp)) {
        tp -> new EpochEndOffset()
          .setPartition(tp.partition)
          .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
      } else {
        try {
          val holder = futures(tp).get()
          val leaderEpoch = epochData.leaderEpoch
          if (holder.hasException) {
            val err = Errors.forException(holder.exception().get())
            tp -> new EpochEndOffset()
              .setPartition(tp.partition)
              .setErrorCode(err.code)
          } else {
            val endOffset = holder.timestampAndOffset().get().offset
            tp -> new EpochEndOffset()
              .setPartition(tp.partition)
              .setErrorCode(Errors.NONE.code)
              .setLeaderEpoch(leaderEpoch)
              .setEndOffset(endOffset)
          }
        } catch {
          case t: Throwable =>
            tp -> new EpochEndOffset()
              .setPartition(tp.partition)
              .setErrorCode(Errors.forException(t).code)
        }
      }
    }.toMap.asJava
  }

  override def buildFetch(partitions: util.Map[TopicPartition, PartitionFetchState]): ResultWithPartitions[Optional[ReplicaFetch]] = {
    if (quota.isQuotaExceeded) {
      new ResultWithPartitions(Optional.empty(), util.Set.of())
    } else {
      val partitionsWithError = mutable.Set[TopicPartition]()
      val requestMap = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]()

      partitions.forEach { (topicPartition, fetchState) =>
        if (fetchState.isReadyForFetch && !shouldFollowerThrottle(quota, fetchState, topicPartition)) {
          try {
            val logStartOffset = replicaManager.localLogOrException(topicPartition).logStartOffset
            val lastFetchedEpoch = Optional.empty[Integer]()
            requestMap.put(
              topicPartition,
              new FetchRequest.PartitionData(
                fetchState.topicId().orElse(Uuid.ZERO_UUID),
                fetchState.fetchOffset(),
                logStartOffset,
                fetchSize,
                Optional.of(fetchState.currentLeaderEpoch()),
                lastFetchedEpoch
              )
            )
          } catch {
            // UnknownTopicOrPartitionException from localLogOrException when partition is
            // deleted from allPartitions before the fetcher's partitionStates is cleaned up.
            case e @ (_: KafkaStorageException | _: UnknownTopicOrPartitionException) =>
              logger.info("Partition {} unavailable during buildFetch: {}", topicPartition, e.getMessage)
              partitionsWithError += topicPartition
          }
        }
      }

      val fetchRequestOpt = if (requestMap.isEmpty) {
        Optional.empty[ReplicaFetch]()
      } else {
        val metadataVersion = metadataVersionSupplier()
        val canUseTopicIds = requestMap.asScala.values.forall(_.topicId != Uuid.ZERO_UUID)
        val version: Short =
          if (!canUseTopicIds) 12
          else metadataVersion.fetchRequestVersion
        val requestBuilder = FetchRequest.Builder
          .forReplica(version, replicaId, brokerEpochSupplier(), maxWait, minBytes, requestMap)
          .setMaxBytes(maxBytes)
        Optional.of(new ReplicaFetch(requestMap, requestBuilder))
      }

      new ResultWithPartitions(fetchRequestOpt, partitionsWithError.asJava)
    }
  }

  private def shouldFollowerThrottle(quota: ReplicaQuota, fetchState: PartitionFetchState, topicPartition: TopicPartition): Boolean = {
    !fetchState.isReplicaInSync() && quota.isThrottled(topicPartition) && quota.isQuotaExceeded
  }
}
