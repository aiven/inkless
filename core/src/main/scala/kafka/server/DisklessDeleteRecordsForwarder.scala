/*
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

import kafka.server.metadata.InklessMetadataView
import kafka.utils.Logging
import org.apache.kafka.clients.{ClientResponse, NetworkClient, RequestCompletionHandler}
import org.apache.kafka.common.message.DeleteRecordsRequestData
import org.apache.kafka.common.message.DeleteRecordsRequestData.{DeleteRecordsPartition, DeleteRecordsTopic}
import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsPartitionResult
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{DeleteRecordsRequest, DeleteRecordsResponse}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.{Node, TopicPartition}
import org.apache.kafka.metadata.{MetadataCache, PartitionRegistration}
import org.apache.kafka.server.util.{InterBrokerSendThread, RequestAndCompletionHandler}

import java.util
import java.util.concurrent.{CompletableFuture, ConcurrentLinkedQueue}
import scala.collection.mutable
import scala.jdk.CollectionConverters._

/**
 * Forwards the leader-only leg of a `DeleteRecords` request to a diskless partition's real KRaft
 * leader over the inter-broker listener.
 *
 * For a diskless topic that owns a classic/consolidated prefix (switched, switch-pending or
 * consolidating), `ReplicaManager.deleteRecords` splits the request into a broker-agnostic diskless
 * (control-plane) leg and a local-log leg. The local-log leg only succeeds on the real KRaft leader
 * (`Partition.deleteRecordsOnLeader` requires `leaderLogIfLocal`). Because
 * `InklessTopicMetadataTransformer` advertises a hash/AZ-selected replica as the client-facing
 * leader for locality-aware read routing, the AdminClient generally delivers `DeleteRecords` to a
 * follower, which rejects it with `NOT_LEADER_OR_FOLLOWER`.
 *
 * `KafkaApis` uses this forwarder to send the affected partitions to their real leader (resolved
 * from the raw KRaft metadata image, independent of the transformer). The receiving leader runs its
 * own `ReplicaManager.deleteRecords` and serves both legs authoritatively, so read AZ-routing is
 * left untouched.
 *
 * Modeled on `AddPartitionsToTxnManager` / `TransactionMarkerChannelManager`.
 */
class DisklessDeleteRecordsForwarder(
  config: KafkaConfig,
  networkClient: NetworkClient,
  metadataCache: MetadataCache,
  metadataView: InklessMetadataView,
  time: Time
) extends InterBrokerSendThread(
  "DisklessDeleteRecordsForwarder-" + config.brokerId,
  networkClient,
  config.requestTimeoutMs,
  time
) with Logging {

  this.logIdent = s"[DisklessDeleteRecordsForwarder broker=${config.brokerId}] "

  private val interBrokerListenerName = config.interBrokerListenerName

  private case class PendingForward(
    leader: Node,
    offsets: Map[TopicPartition, Long],
    timeoutMs: Int,
    future: CompletableFuture[Map[TopicPartition, DeleteRecordsPartitionResult]]
  )

  private val queue = new ConcurrentLinkedQueue[PendingForward]()

  /**
   * Split `offsets` into the partitions this broker handles locally (via
   * `ReplicaManager.deleteRecords`) and the partitions whose local-log leg must run on another
   * broker's real KRaft leader, grouped by that leader.
   *
   * A partition is forwarded iff it is a diskless topic that owns a local-log leg
   * (switched / switch-pending / consolidating) and its real leader is a different, reachable
   * broker. Pure-diskless partitions (control-plane only) and partitions this broker already leads
   * stay local, preserving existing behaviour.
   */
  def routeByLeader(
    offsets: Map[TopicPartition, Long]
  ): (Map[TopicPartition, Long], Map[Node, Map[TopicPartition, Long]]) = {
    val local = mutable.Map.empty[TopicPartition, Long]
    val forward = mutable.Map.empty[Node, mutable.Map[TopicPartition, Long]]
    offsets.foreach { case (topicPartition, offset) =>
      leaderToForwardTo(topicPartition) match {
        case Some(leader) => forward.getOrElseUpdate(leader, mutable.Map.empty) += (topicPartition -> offset)
        case None => local += (topicPartition -> offset)
      }
    }
    (local.toMap, forward.map { case (node, partitionOffsets) => node -> partitionOffsets.toMap }.toMap)
  }

  private def leaderToForwardTo(topicPartition: TopicPartition): Option[Node] = {
    if (!metadataView.isDisklessTopic(topicPartition.topic) || !hasLocalLeg(topicPartition)) {
      None
    } else {
      val leader = metadataCache.getPartitionLeaderEndpoint(
        topicPartition.topic, topicPartition.partition, interBrokerListenerName)
      if (leader.isPresent && !leader.get.isEmpty && leader.get.id != config.brokerId) Some(leader.get)
      else None
    }
  }

  /**
   * Whether the partition has a classic/consolidated prefix that lives in a local `UnifiedLog`,
   * i.e. a leader-only local-log leg of `DeleteRecords` that is not broker-agnostic. Derived purely
   * from KRaft metadata so it can be evaluated on any broker (a born consolidating partition may not
   * yet have a local log on the leader; the leader resolves that when it runs the split).
   */
  private def hasLocalLeg(topicPartition: TopicPartition): Boolean = {
    val start = metadataView.getClassicToDisklessStartOffset(topicPartition)
    start >= 0 ||
      start == PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING ||
      (start == PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET &&
        metadataView.isConsolidatingDisklessTopic(topicPartition.topic))
  }

  /**
   * Forward a `DeleteRecords` sub-request for `offsets` to `leader` and complete the returned future
   * with the per-partition results. On a routing/network failure the future completes with a
   * retriable `NOT_LEADER_OR_FOLLOWER` per partition so the AdminClient re-reads metadata and
   * retries once leadership settles.
   */
  def forward(
    leader: Node,
    offsets: Map[TopicPartition, Long],
    timeoutMs: Int
  ): CompletableFuture[Map[TopicPartition, DeleteRecordsPartitionResult]] = {
    val future = new CompletableFuture[Map[TopicPartition, DeleteRecordsPartitionResult]]()
    if (leader == null || leader.isEmpty) {
      future.complete(errorResults(offsets.keys, Errors.NOT_LEADER_OR_FOLLOWER))
    } else {
      queue.add(PendingForward(leader, offsets, timeoutMs, future))
      wakeup()
    }
    future
  }

  private def errorResults(
    partitions: Iterable[TopicPartition],
    error: Errors
  ): Map[TopicPartition, DeleteRecordsPartitionResult] = {
    partitions.map { tp =>
      tp -> new DeleteRecordsPartitionResult()
        .setPartitionIndex(tp.partition)
        .setLowWatermark(DeleteRecordsResponse.INVALID_LOW_WATERMARK)
        .setErrorCode(error.code)
    }.toMap
  }

  override def generateRequests(): util.Collection[RequestAndCompletionHandler] = {
    val requests = new util.ArrayList[RequestAndCompletionHandler]()
    val now = time.milliseconds()
    var pending = queue.poll()
    while (pending != null) {
      requests.add(buildRequest(pending, now))
      pending = queue.poll()
    }
    requests
  }

  private def buildRequest(pending: PendingForward, now: Long): RequestAndCompletionHandler = {
    val topics = new util.LinkedHashMap[String, DeleteRecordsTopic]()
    pending.offsets.foreach { case (tp, offset) =>
      val topic = topics.computeIfAbsent(tp.topic, name => new DeleteRecordsTopic().setName(name))
      topic.partitions().add(new DeleteRecordsPartition()
        .setPartitionIndex(tp.partition)
        .setOffset(offset))
    }
    val data = new DeleteRecordsRequestData()
      .setTopics(new util.ArrayList[DeleteRecordsTopic](topics.values()))
      .setTimeoutMs(pending.timeoutMs)
    new RequestAndCompletionHandler(
      now,
      pending.leader,
      new DeleteRecordsRequest.Builder(data),
      completionHandler(pending)
    )
  }

  private def completionHandler(pending: PendingForward): RequestCompletionHandler = new RequestCompletionHandler {
    override def onComplete(response: ClientResponse): Unit = {
      try {
        if (response.authenticationException != null) {
          error(s"DeleteRecords forwarding to ${pending.leader} failed with an authentication error", response.authenticationException)
          pending.future.complete(errorResults(pending.offsets.keys, Errors.forException(response.authenticationException)))
        } else if (response.versionMismatch != null) {
          error(s"DeleteRecords forwarding to ${pending.leader} failed with a version mismatch", response.versionMismatch)
          pending.future.complete(errorResults(pending.offsets.keys, Errors.UNKNOWN_SERVER_ERROR))
        } else if (response.wasDisconnected || response.wasTimedOut) {
          warn(s"DeleteRecords forwarding to ${pending.leader} failed with a network error " +
            s"(disconnected=${response.wasDisconnected}, timedOut=${response.wasTimedOut}); the client will retry")
          pending.future.complete(errorResults(pending.offsets.keys, Errors.NOT_LEADER_OR_FOLLOWER))
        } else {
          pending.future.complete(parseResponse(pending, response.responseBody.asInstanceOf[DeleteRecordsResponse]))
        }
      } catch {
        case e: Throwable =>
          error(s"Failed to handle the forwarded DeleteRecords response from ${pending.leader}", e)
          pending.future.complete(errorResults(pending.offsets.keys, Errors.UNKNOWN_SERVER_ERROR))
      } finally {
        wakeup()
      }
    }
  }

  private def parseResponse(
    pending: PendingForward,
    response: DeleteRecordsResponse
  ): Map[TopicPartition, DeleteRecordsPartitionResult] = {
    val results = mutable.Map.empty[TopicPartition, DeleteRecordsPartitionResult]
    for (topicResult <- response.data.topics.asScala; partitionResult <- topicResult.partitions.asScala) {
      val tp = new TopicPartition(topicResult.name, partitionResult.partitionIndex)
      results += tp -> new DeleteRecordsPartitionResult()
        .setPartitionIndex(partitionResult.partitionIndex)
        .setLowWatermark(partitionResult.lowWatermark)
        .setErrorCode(partitionResult.errorCode)
    }
    // Any partition the leader did not answer for is reported as retriable so the client retries.
    pending.offsets.keys.filterNot(results.contains).foreach { tp =>
      results += tp -> new DeleteRecordsPartitionResult()
        .setPartitionIndex(tp.partition)
        .setLowWatermark(DeleteRecordsResponse.INVALID_LOW_WATERMARK)
        .setErrorCode(Errors.NOT_LEADER_OR_FOLLOWER.code)
    }
    results.toMap
  }
}
