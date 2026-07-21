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

import kafka.server.metadata.InklessMetadataView
import org.apache.kafka.clients.NetworkClient
import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsPartitionResult
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.{Node, TopicPartition}
import org.apache.kafka.metadata.{MetadataCache, PartitionRegistration}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{BeforeEach, Test}
import org.mockito.ArgumentMatchers.{any, anyInt, eq => eqTo}
import org.mockito.Mockito._

import java.util.Optional

class DisklessDeleteRecordsForwarderTest {

  private val selfBrokerId = 1
  private val interBrokerListener = new ListenerName("PLAINTEXT")

  private val config: KafkaConfig = mock(classOf[KafkaConfig])
  private val networkClient: NetworkClient = mock(classOf[NetworkClient])
  private val metadataCache: MetadataCache = mock(classOf[MetadataCache])
  private val metadataView: InklessMetadataView = mock(classOf[InklessMetadataView])

  private var forwarder: DisklessDeleteRecordsForwarder = _

  @BeforeEach
  def setUp(): Unit = {
    when(config.brokerId).thenReturn(selfBrokerId)
    when(config.requestTimeoutMs).thenReturn(30000)
    when(config.interBrokerListenerName).thenReturn(interBrokerListener)
    forwarder = new DisklessDeleteRecordsForwarder(config, networkClient, metadataCache, metadataView, time = org.apache.kafka.common.utils.Time.SYSTEM)
  }

  private def stubLeader(tp: TopicPartition, node: Optional[Node]): Unit = {
    when(metadataCache.getPartitionLeaderEndpoint(eqTo(tp.topic), eqTo(tp.partition), any(classOf[ListenerName])))
      .thenReturn(node)
  }

  private def otherBroker: Node = new Node(2, "other-host", 9092)

  @Test
  def classicTopicStaysLocal(): Unit = {
    val tp = new TopicPartition("classic", 0)
    when(metadataView.isDisklessTopic("classic")).thenReturn(false)

    val (local, forward) = forwarder.routeByLeader(Map(tp -> 10L))
    assertEquals(Map(tp -> 10L), local)
    assertTrue(forward.isEmpty)
  }

  @Test
  def pureDisklessTopicStaysLocal(): Unit = {
    val tp = new TopicPartition("diskless", 0)
    when(metadataView.isDisklessTopic("diskless")).thenReturn(true)
    when(metadataView.isConsolidatingDisklessTopic("diskless")).thenReturn(false)
    when(metadataView.getClassicToDisklessStartOffset(tp))
      .thenReturn(PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET)

    val (local, forward) = forwarder.routeByLeader(Map(tp -> 10L))
    assertEquals(Map(tp -> 10L), local)
    assertTrue(forward.isEmpty)
    verify(metadataCache, never()).getPartitionLeaderEndpoint(any(), anyInt(), any())
  }

  @Test
  def switchedTopicOnRemoteLeaderIsForwarded(): Unit = {
    val tp = new TopicPartition("switched", 0)
    when(metadataView.isDisklessTopic("switched")).thenReturn(true)
    when(metadataView.getClassicToDisklessStartOffset(tp)).thenReturn(100L)
    stubLeader(tp, Optional.of(otherBroker))

    val (local, forward) = forwarder.routeByLeader(Map(tp -> 150L))
    assertTrue(local.isEmpty)
    assertEquals(Map(otherBroker -> Map(tp -> 150L)), forward)
  }

  @Test
  def switchedTopicOnSelfStaysLocal(): Unit = {
    val tp = new TopicPartition("switched", 0)
    when(metadataView.isDisklessTopic("switched")).thenReturn(true)
    when(metadataView.getClassicToDisklessStartOffset(tp)).thenReturn(100L)
    stubLeader(tp, Optional.of(new Node(selfBrokerId, "self-host", 9092)))

    val (local, forward) = forwarder.routeByLeader(Map(tp -> 150L))
    assertEquals(Map(tp -> 150L), local)
    assertTrue(forward.isEmpty)
  }

  @Test
  def consolidatingBornTopicOnRemoteLeaderIsForwarded(): Unit = {
    val tp = new TopicPartition("consolidating", 0)
    when(metadataView.isDisklessTopic("consolidating")).thenReturn(true)
    when(metadataView.isConsolidatingDisklessTopic("consolidating")).thenReturn(true)
    when(metadataView.getClassicToDisklessStartOffset(tp))
      .thenReturn(PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET)
    stubLeader(tp, Optional.of(otherBroker))

    val (local, forward) = forwarder.routeByLeader(Map(tp -> 50L))
    assertTrue(local.isEmpty)
    assertEquals(Map(otherBroker -> Map(tp -> 50L)), forward)
  }

  @Test
  def switchPendingTopicOnRemoteLeaderIsForwarded(): Unit = {
    val tp = new TopicPartition("pending", 0)
    when(metadataView.isDisklessTopic("pending")).thenReturn(true)
    when(metadataView.getClassicToDisklessStartOffset(tp))
      .thenReturn(PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING)
    stubLeader(tp, Optional.of(otherBroker))

    val (local, forward) = forwarder.routeByLeader(Map(tp -> 5L))
    assertTrue(local.isEmpty)
    assertEquals(Map(otherBroker -> Map(tp -> 5L)), forward)
  }

  @Test
  def missingLeaderStaysLocal(): Unit = {
    val tp = new TopicPartition("switched", 0)
    when(metadataView.isDisklessTopic("switched")).thenReturn(true)
    when(metadataView.getClassicToDisklessStartOffset(tp)).thenReturn(100L)
    stubLeader(tp, Optional.empty())

    val (local, forward) = forwarder.routeByLeader(Map(tp -> 150L))
    assertEquals(Map(tp -> 150L), local)
    assertTrue(forward.isEmpty)
  }

  @Test
  def emptyLeaderNodeStaysLocal(): Unit = {
    val tp = new TopicPartition("switched", 0)
    when(metadataView.isDisklessTopic("switched")).thenReturn(true)
    when(metadataView.getClassicToDisklessStartOffset(tp)).thenReturn(100L)
    stubLeader(tp, Optional.of(Node.noNode()))

    val (local, forward) = forwarder.routeByLeader(Map(tp -> 150L))
    assertEquals(Map(tp -> 150L), local)
    assertTrue(forward.isEmpty)
  }

  @Test
  def mixedPartitionsAreSplitPerLeader(): Unit = {
    val localTp = new TopicPartition("diskless", 0)
    val remoteTp = new TopicPartition("switched", 0)
    when(metadataView.isDisklessTopic("diskless")).thenReturn(true)
    when(metadataView.isConsolidatingDisklessTopic("diskless")).thenReturn(false)
    when(metadataView.getClassicToDisklessStartOffset(localTp))
      .thenReturn(PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET)
    when(metadataView.isDisklessTopic("switched")).thenReturn(true)
    when(metadataView.getClassicToDisklessStartOffset(remoteTp)).thenReturn(100L)
    stubLeader(remoteTp, Optional.of(otherBroker))

    val (local, forward) = forwarder.routeByLeader(Map(localTp -> 1L, remoteTp -> 2L))
    assertEquals(Map(localTp -> 1L), local)
    assertEquals(Map(otherBroker -> Map(remoteTp -> 2L)), forward)
  }

  @Test
  def forwardToEmptyLeaderCompletesWithRetriableError(): Unit = {
    val tp = new TopicPartition("switched", 0)
    val future = forwarder.forward(Node.noNode(), Map(tp -> 10L), 1000)
    assertTrue(future.isDone)
    val result: Map[TopicPartition, DeleteRecordsPartitionResult] = future.get()
    assertEquals(Errors.NOT_LEADER_OR_FOLLOWER.code, result(tp).errorCode)
  }

  @Test
  def forwardToNullLeaderCompletesWithRetriableError(): Unit = {
    val tp = new TopicPartition("switched", 0)
    val future = forwarder.forward(null, Map(tp -> 10L), 1000)
    assertTrue(future.isDone)
    assertEquals(Errors.NOT_LEADER_OR_FOLLOWER.code, future.get()(tp).errorCode)
  }
}
