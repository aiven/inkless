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

package kafka.server

import kafka.cluster.Partition
import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderPartition
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.server.common.OffsetAndEpoch
import org.apache.kafka.server.log.remote.storage.{RemoteLogManager, RemoteLogSegmentMetadata, RemoteStorageManager}
import org.apache.kafka.server.{LeaderEndPoint, PartitionFetchState, ReplicaState}
import org.apache.kafka.storage.internals.epoch.LeaderEpochFileCache
import org.apache.kafka.storage.internals.log.{EpochEntry, ProducerStateManager, UnifiedLog}
import org.apache.kafka.storage.log.metrics.BrokerTopicStats
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{any, eq => eqTo}
import org.mockito.Mockito.{doReturn, mock, never, verify, when}

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.Optional
import scala.jdk.CollectionConverters._

/**
 * Exercises the *real* [[TierStateMachine.start]] / `buildRemoteLogAuxState` path that recovers a
 * consolidating partition's remote prefix after a local-log loss, with the diskless
 * [[org.apache.kafka.server.LeaderEndPoint]] (mocked here) standing in for the control plane.
 *
 * This is the PR-runnable companion to the born-only `consolidation_read_from_remote_after_prune`
 * ducktape system test: it pins the behaviour that differs between a **born-consolidated** topic
 * (the diskless prefix rides at epoch 0, so the rebuild short-circuits the epoch lookup) and a
 * **switched** (classic -> diskless) topic (the diskless interval rides at `last_classic_epoch + 1`,
 * so the boundary offset must be resolved back to the *classic* epoch via
 * `fetchEpochEndOffsets` before the matching remote segment can be located). The switched case is
 * the higher-risk one and is otherwise unverified outside of expensive system tests.
 */
class ConsolidationTierStateRebuildTest {

  private val topicPartition = new TopicPartition("consolidating-topic", 0)
  private val topicId = Uuid.randomUuid()
  // The partition's current leader epoch (advanced past the diskless epoch).
  private val currentLeaderEpoch = 5
  // The diskless WAL starts here; the remote prefix is [0, disklessWalStart).
  private val disklessWalStart = 100L
  // The whole-log start that the diskless leader endpoint preserves (Part 1 of the fix).
  private val wholeLogStart = 0L

  private var tempDir: java.io.File = _

  @BeforeEach
  def setUp(): Unit = {
    tempDir = Files.createTempDirectory("tier-state-rebuild-test").toFile
  }

  @AfterEach
  def tearDown(): Unit = {
    if (tempDir != null) {
      Option(tempDir.listFiles()).foreach(_.foreach(_.delete()))
      tempDir.delete()
    }
  }

  private def leaderEpochCheckpointStream(epochs: Seq[(Int, Long)]): ByteArrayInputStream = {
    // Checkpoint format: version line, entry-count line, then "epoch startOffset" per entry.
    val sb = new StringBuilder
    sb.append("0\n")
    sb.append(epochs.size).append("\n")
    epochs.foreach { case (epoch, startOffset) => sb.append(epoch).append(" ").append(startOffset).append("\n") }
    new ByteArrayInputStream(sb.toString().getBytes(StandardCharsets.UTF_8))
  }

  /**
   * Wires a real [[TierStateMachine]] over mocks. `earliestLocalEpoch` is the epoch the diskless
   * leader reports for the earliest local (diskless WAL start) offset: 0 for born-consolidated,
   * non-zero for switched topics. `remoteCheckpointEpochs` is what the remote segment's leader-epoch
   * index contains. Returns the machine plus the mocks needed for verification.
   */
  private def setup(
    earliestLocalEpoch: Int,
    remoteCheckpointEpochs: Seq[(Int, Long)]
  ): (TierStateMachine, LeaderEndPoint, RemoteLogManager, Partition, LeaderEpochFileCache) = {
    val leader = mock(classOf[LeaderEndPoint])
    val replicaManager = mock(classOf[ReplicaManager])
    val rlm = mock(classOf[RemoteLogManager])
    val rsm = mock(classOf[RemoteStorageManager])
    val unifiedLog = mock(classOf[UnifiedLog])
    val partition = mock(classOf[Partition])
    val epochCache = mock(classOf[LeaderEpochFileCache])
    val psm = mock(classOf[ProducerStateManager])
    val segmentMetadata = mock(classOf[RemoteLogSegmentMetadata])

    // Leader (diskless control plane) responses.
    when(leader.fetchEarliestLocalOffset(eqTo(topicPartition), eqTo(currentLeaderEpoch)))
      .thenReturn(new OffsetAndEpoch(disklessWalStart, earliestLocalEpoch))
    when(leader.fetchLatestOffset(eqTo(topicPartition), eqTo(currentLeaderEpoch)))
      .thenReturn(new OffsetAndEpoch(200L, earliestLocalEpoch))

    // remoteStorageEnabled=true so the buildRemoteLogAuxState* meters that start() marks are registered.
    when(replicaManager.brokerTopicStats).thenReturn(new BrokerTopicStats(true))
    when(replicaManager.localLogOrException(topicPartition)).thenReturn(unifiedLog)
    when(replicaManager.remoteLogManager).thenReturn(Some(rlm))
    when(replicaManager.getPartitionOrException(topicPartition)).thenReturn(partition)

    when(unifiedLog.remoteLogEnabled()).thenReturn(true)
    when(unifiedLog.dir()).thenReturn(tempDir)
    when(unifiedLog.leaderEpochCache()).thenReturn(epochCache)
    when(unifiedLog.producerStateManager()).thenReturn(psm)
    when(unifiedLog.latestEpoch()).thenReturn(Optional.of(Int.box(remoteCheckpointEpochs.last._1)))
    doReturn(new java.util.HashMap()).when(psm).activeProducers()

    when(rlm.isPartitionReady(topicPartition)).thenReturn(true)
    when(rlm.storageManager()).thenReturn(rsm)
    // The remote segment that contains (disklessWalStart - 1); its end offset is that last prefix offset.
    when(segmentMetadata.endOffset()).thenReturn(disklessWalStart - 1)
    when(rsm.fetchIndex(eqTo(segmentMetadata), eqTo(RemoteStorageManager.IndexType.LEADER_EPOCH)))
      .thenReturn(leaderEpochCheckpointStream(remoteCheckpointEpochs))
    when(rsm.fetchIndex(eqTo(segmentMetadata), eqTo(RemoteStorageManager.IndexType.PRODUCER_SNAPSHOT)))
      .thenReturn(new ByteArrayInputStream(Array.emptyByteArray))

    // The classic prefix epoch (epoch 0) ends exactly at the seal/WAL start, covering the whole prefix.
    when(leader.fetchEpochEndOffsets(any()))
      .thenReturn(java.util.Map.of(
        topicPartition,
        new EpochEndOffset()
          .setPartition(topicPartition.partition)
          .setErrorCode(Errors.NONE.code)
          .setLeaderEpoch(0)
          .setEndOffset(disklessWalStart)
      ))

    // Whichever target epoch is resolved, the segment lookup for (disklessWalStart - 1) returns our segment.
    when(rlm.fetchRemoteLogSegmentMetadata(eqTo(topicPartition), any[Int], eqTo(disklessWalStart - 1)))
      .thenReturn(Optional.of(segmentMetadata))

    val tierStateMachine = new TierStateMachine(leader, replicaManager, false)
    (tierStateMachine, leader, rlm, partition, epochCache)
  }

  private def fetchState(): PartitionFetchState =
    new PartitionFetchState(Optional.of(topicId), wholeLogStart, Optional.empty(), currentLeaderEpoch, ReplicaState.FETCHING, Optional.empty())

  private def fetchPartitionData(): FetchResponseData.PartitionData =
    new FetchResponseData.PartitionData()
      .setPartitionIndex(topicPartition.partition)
      // The diskless leader endpoint preserves the whole-log start here (Part 1 of the fix).
      .setLogStartOffset(wholeLogStart)

  @Test
  def testRebuildResolvesClassicEpochForSwitchedTopic(): Unit = {
    // Switched topic: the classic->diskless seal is at offset 100, so the classic prefix [0, 100) is
    // epoch 0 and the diskless interval begins at 100 under epoch 1 (= last_classic_epoch + 1). The
    // diskless leader therefore reports the earliest-local (WAL start) offset at the *diskless* epoch
    // 1, but the boundary offset 99 belongs to the classic segment written under epoch 0.
    val (tsm, leader, rlm, partition, epochCache) = setup(
      earliestLocalEpoch = 1,
      remoteCheckpointEpochs = Seq(0 -> 0L)
    )

    val newState = tsm.start(topicPartition, fetchState(), fetchPartitionData())

    // Because the earliest-local epoch is non-zero, the boundary offset must be resolved against the
    // previous (classic) epoch rather than blindly reusing the diskless epoch: start() asks the leader
    // for the end offset of the epoch before 1...
    val epochReq = ArgumentCaptor.forClass(classOf[java.util.Map[TopicPartition, OffsetForLeaderPartition]])
    verify(leader).fetchEpochEndOffsets(epochReq.capture())
    assertEquals(0, epochReq.getValue.get(topicPartition).leaderEpoch,
      "switched topic must look up the end offset of the epoch before the diskless epoch")

    // ...and, since that classic epoch (0) extends past the boundary offset, locate the remote segment
    // under the classic epoch 0 (NOT the diskless epoch 1) for offset disklessWalStart - 1.
    verify(rlm).fetchRemoteLogSegmentMetadata(eqTo(topicPartition), eqTo(0), eqTo(disklessWalStart - 1))

    // The rebuilt epoch cache reflects the classic prefix recovered from the remote segment.
    val assigned = ArgumentCaptor.forClass(classOf[java.util.List[EpochEntry]])
    verify(epochCache).assign(assigned.capture())
    assertEquals(List(new EpochEntry(0, 0L)), assigned.getValue.asScala.toList)

    assertCommonRecovery(newState, partition)
  }

  @Test
  def testRebuildShortCircuitsEpochZeroForBornConsolidatedTopic(): Unit = {
    // Born-consolidated topic: the whole prefix rides at epoch 0, so no earlier-epoch lookup is needed.
    val (tsm, leader, rlm, partition, epochCache) = setup(
      earliestLocalEpoch = 0,
      remoteCheckpointEpochs = Seq(0 -> 0L)
    )

    val newState = tsm.start(topicPartition, fetchState(), fetchPartitionData())

    // Epoch 0 short-circuits: the diskless control plane is never queried for an earlier epoch.
    verify(leader, never()).fetchEpochEndOffsets(any())
    verify(rlm).fetchRemoteLogSegmentMetadata(eqTo(topicPartition), eqTo(0), eqTo(disklessWalStart - 1))

    val assigned = ArgumentCaptor.forClass(classOf[java.util.List[EpochEntry]])
    verify(epochCache).assign(assigned.capture())
    assertEquals(List(new EpochEntry(0, 0L)), assigned.getValue.asScala.toList)

    assertCommonRecovery(newState, partition)
  }

  /** Recovery effects common to both topologies: resume at the WAL start, preserve logStartOffset=0. */
  private def assertCommonRecovery(newState: PartitionFetchState, partition: Partition): Unit = {
    // Fetching resumes at the diskless WAL start (one past the remote prefix's last offset).
    assertEquals(disklessWalStart, newState.fetchOffset)
    assertEquals(ReplicaState.FETCHING, newState.state)

    // The local log is seeded at the WAL start while the whole-log start is preserved at 0,
    // so reads below the WAL start route to the (now rebuilt) remote prefix.
    val lsoCaptor = ArgumentCaptor.forClass(classOf[Optional[java.lang.Long]])
    verify(partition).truncateFullyAndStartAt(eqTo(disklessWalStart), eqTo(false), lsoCaptor.capture())
    assertEquals(Optional.of(java.lang.Long.valueOf(wholeLogStart)), lsoCaptor.getValue)
  }
}
