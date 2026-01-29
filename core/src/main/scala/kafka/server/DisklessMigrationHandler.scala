/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/

package kafka.server

import io.aiven.inkless.control_plane.{ControlPlane, InitDisklessLogRequest, ProducerStateSnapshot}
import kafka.cluster.{Partition, SealResult}
import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition

import scala.collection.mutable
import scala.jdk.CollectionConverters._

/**
 * Result of a diskless migration operation for a single partition.
 */
sealed trait MigrationResult
case class MigrationSuccess(topicPartition: TopicPartition, disklessStartOffset: Long) extends MigrationResult
case class MigrationSkipped(topicPartition: TopicPartition, reason: String) extends MigrationResult
case class MigrationFailed(topicPartition: TopicPartition, exception: Throwable) extends MigrationResult

/**
 * Handles the migration of topic partitions from classic (diskful) to diskless storage.
 *
 * This handler implements the Delos-style chain sealing protocol to ensure safe migration:
 *
 * 1. **Seal Phase**: For each partition, acquires the leaderIsrUpdateLock write lock to:
 *    - Block any in-flight append operations
 *    - Capture the exact Log End Offset (LEO) as B0 (diskless start offset)
 *    - Mark the partition as sealed to prevent future classic appends
 *
 * 2. **Initialize Phase**: Initializes the diskless log in the control plane with:
 *    - The captured B0 offset (using high watermark for safety, as it represents committed data)
 *    - Producer state snapshots for idempotent producer support
 *
 * The protocol guarantees:
 * - No data loss: B0 is captured while holding the lock, ensuring no appends are in-flight
 * - Safe boundary: All offsets < B0 are in classic format, all offsets >= B0 are in diskless format
 * - Online migration: No broker restart required, only produces are briefly blocked during sealing
 *
 * @param replicaManager The ReplicaManager to access partitions
 * @param controlPlane   Optional ControlPlane for diskless log initialization
 */
class DisklessMigrationHandler(
  replicaManager: ReplicaManager,
  controlPlane: Option[ControlPlane]
) extends Logging {

  /**
   * Migrates all partitions of a topic from classic to diskless storage.
   *
   * This method is called when the topic configuration changes to diskless.enable=true.
   * It performs the following steps:
   *
   * 1. Seals all leader partitions (non-leaders are skipped)
   * 2. Initializes the diskless log in the control plane
   *
   * The migration is idempotent - if a partition is already sealed, it will be skipped.
   *
   * @param topic The topic name to migrate
   * @return A map of partition to migration result
   */
  def migrateTopicToDiskless(topic: String): Map[TopicPartition, MigrationResult] = {
    info(s"Starting diskless migration for topic $topic")

    val results = mutable.Map[TopicPartition, MigrationResult]()

    // Step 1: Get all partitions for this topic that this broker is leader for
    val partitionsToSeal = getPartitionsToSeal(topic)
    if (partitionsToSeal.isEmpty) {
      info(s"No leader partitions found for topic $topic on this broker")
      return results.toMap
    }

    info(s"Found ${partitionsToSeal.size} leader partitions to seal for topic $topic: " +
      s"${partitionsToSeal.map(_.topicPartition).mkString(", ")}")

    // Step 2: Seal each partition and collect seal results
    val sealResults = mutable.Map[TopicPartition, SealResult]()
    val initRequests = mutable.Set[InitDisklessLogRequest]()

    for (partition <- partitionsToSeal) {
      val tp = partition.topicPartition
      try {
        partition.sealForDisklessMigration() match {
          case Some(sealResult) =>
            sealResults(tp) = sealResult

            // Create InitDisklessLogRequest using the captured state from SealResult
            // This is critical: we use the state captured atomically during sealing,
            // not the current state of the log, to ensure consistency
            sealResult.topicId match {
              case Some(topicId) =>
                val initRequest = createInitRequestFromSealResult(sealResult, topicId)
                initRequests.add(initRequest)
              case None =>
                error(s"Topic ID is required for diskless initialization of partition $tp")
                results(tp) = MigrationFailed(tp,
                  new IllegalStateException("Topic ID is required for diskless initialization"))
            }

          case None =>
            results(tp) = MigrationSkipped(tp, "Broker is not the leader for this partition")
        }
      } catch {
        case e: Exception =>
          error(s"Failed to seal partition $tp for diskless migration", e)
          results(tp) = MigrationFailed(tp, e)
      }
    }

    // Step 3: Initialize diskless logs in the control plane
    if (initRequests.nonEmpty) {
      controlPlane match {
        case Some(cp) =>
          try {
            info(s"Initializing diskless logs in control plane for ${initRequests.size} partitions")
            cp.initDisklessLog(initRequests.asJava)

            // Mark all sealed partitions as successful
            for ((tp, sealResult) <- sealResults if !results.contains(tp)) {
              results(tp) = MigrationSuccess(tp, sealResult.highWatermark)
              info(s"Successfully migrated partition $tp to diskless with B0=${sealResult.highWatermark}")
            }
          } catch {
            case e: Exception =>
              error(s"Failed to initialize diskless logs in control plane for topic $topic", e)
              // Mark all sealed partitions as failed
              // Note: The partitions are still sealed, but the control plane initialization failed.
              // This is intentional - we don't unseal to prevent data inconsistency.
              // The initialization should be retried.
              for ((tp, _) <- sealResults if !results.contains(tp)) {
                results(tp) = MigrationFailed(tp, e)
              }
          }

        case None =>
          warn(s"Control plane not available. Partitions are sealed but diskless logs not initialized. " +
            s"This should not happen in production - diskless storage system should be enabled.")
          for ((tp, sealResult) <- sealResults if !results.contains(tp)) {
            results(tp) = MigrationFailed(tp,
              new IllegalStateException("Control plane not available for diskless initialization"))
          }
      }
    }

    info(s"Completed diskless migration for topic $topic. " +
      s"Success: ${results.values.count(_.isInstanceOf[MigrationSuccess])}, " +
      s"Skipped: ${results.values.count(_.isInstanceOf[MigrationSkipped])}, " +
      s"Failed: ${results.values.count(_.isInstanceOf[MigrationFailed])}")

    results.toMap
  }

  /**
   * Gets all partitions for a topic that this broker is the leader for.
   */
  private def getPartitionsToSeal(topic: String): Seq[Partition] = {
    replicaManager.logManager.logsByTopic(topic).flatMap { log =>
      replicaManager.onlinePartition(log.topicPartition).filter(_.isLeader)
    }.toSeq
  }

  /**
   * Creates an InitDisklessLogRequest from the state captured during sealing.
   *
   * IMPORTANT: This method uses the state captured atomically during sealing (in SealResult),
   * NOT the current state of the log. This is critical for data safety:
   * - The SealResult was captured while holding the write lock
   * - No appends were in-flight when these values were captured
   * - Using the captured state ensures consistency between the seal point and initialization
   *
   * We use highWatermark as disklessStartOffset because:
   * - The highWatermark represents committed data that has been acknowledged
   * - For diskless topics with replication factor 1, HW should equal LEO
   * - Using HW provides an extra safety margin for data that is "committed"
   */
  private def createInitRequestFromSealResult(
    sealResult: SealResult,
    topicId: org.apache.kafka.common.Uuid
  ): InitDisklessLogRequest = {
    // Extract producer state from the captured producer state manager
    // This must be done immediately after sealing while the partition is still sealed
    val producerStateEntries = extractProducerState(sealResult.producerStateManager)

    new InitDisklessLogRequest(
      topicId,
      sealResult.topicPartition.topic(),
      sealResult.topicPartition.partition(),
      sealResult.logStartOffset,
      sealResult.highWatermark, // Use HW as disklessStartOffset for safety
      sealResult.leaderEpoch,
      producerStateEntries.asJava
    )
  }

  /**
   * Extracts the producer state from the ProducerStateManager.
   *
   * For each active producer, this method extracts all retained batch metadata entries
   * to support duplicate detection after the transition to diskless.
   */
  private def extractProducerState(
    producerStateManager: org.apache.kafka.storage.internals.log.ProducerStateManager
  ): List[ProducerStateSnapshot] = {
    val snapshots = new mutable.ListBuffer[ProducerStateSnapshot]()

    producerStateManager.activeProducers().forEach { case (producerId, state) =>
      state.batchMetadata().forEach { batch =>
        snapshots += new ProducerStateSnapshot(
          producerId,
          state.producerEpoch(),
          batch.firstSeq(),
          batch.lastSeq,
          batch.firstOffset(),
          batch.timestamp
        )
      }
    }

    snapshots.toList
  }
}
