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

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.{FetchResponseData, OffsetForLeaderEpochRequestData, OffsetForLeaderEpochResponseData}
import org.apache.kafka.common.requests.FetchRequest
import org.apache.kafka.server.common.OffsetAndEpoch
import org.apache.kafka.server.network.BrokerEndPoint
import org.apache.kafka.server.{LeaderEndPoint, PartitionFetchState, ReplicaFetch, ResultWithPartitions}

import java.util
import java.util.Optional

class DisklessLeaderEndPoint(brokerEndPoint: BrokerEndPoint) extends LeaderEndPoint {

  /**
   * A boolean specifying if truncation when fetching from the leader is supported
   */
  override def isTruncationOnFetchSupported: Boolean = false

  /**
   * Initiate closing access to fetches from leader.
   */
  override def initiateClose(): Unit = {
  }

  /**
   * Closes access to fetches from leader.
   * `initiateClose` must be called prior to invoking `close`.
   */
  override def close(): Unit = {
  }

  /**
   * The specific broker (host:port) we want to connect to.
   */
  override def brokerEndPoint(): BrokerEndPoint = {
    brokerEndPoint
  }

  /**
   * Given a fetchRequest, carries out the expected request and returns
   * the results from fetching from the leader.
   *
   * @param fetchRequest The fetch request we want to carry out
   * @return A map of topic partition -> fetch data
   */
  override def fetch(fetchRequest: FetchRequest.Builder): util.Map[TopicPartition, FetchResponseData.PartitionData] =
    util.Map.of[TopicPartition, FetchResponseData.PartitionData]

  /**
   * Fetches the epoch and log start offset of the given topic partition from the leader.
   *
   * @param topicPartition     The topic partition that we want to fetch from
   * @param currentLeaderEpoch An int representing the current leader epoch of the requester
   * @return An OffsetAndEpoch object representing the earliest offset and epoch in the leader's topic partition.
   */
  override def fetchEarliestOffset(topicPartition: TopicPartition, currentLeaderEpoch: Int): OffsetAndEpoch = {
    return new OffsetAndEpoch(0, 0)
  }

  /**
   * Fetches the epoch and log end offset of the given topic partition from the leader.
   *
   * @param topicPartition     The topic partition that we want to fetch from
   * @param currentLeaderEpoch An int representing the current leader epoch of the requester
   * @return An OffsetAndEpoch object representing the latest offset and epoch in the leader's topic partition.
   */
  override def fetchLatestOffset(topicPartition: TopicPartition, currentLeaderEpoch: Int): OffsetAndEpoch = {
    return new OffsetAndEpoch(0, 0)
  }

  /**
   * Fetches offset for leader epoch from the leader for each given topic partition
   *
   * @param partitions A map of topic partition -> leader epoch of the replica
   * @return A map of topic partition -> end offset for a requested leader epoch
   */
  override def fetchEpochEndOffsets(partitions: util.Map[TopicPartition, OffsetForLeaderEpochRequestData.OffsetForLeaderPartition]): util.Map[TopicPartition, OffsetForLeaderEpochResponseData.EpochEndOffset] = {
    util.Map.of[TopicPartition, OffsetForLeaderEpochResponseData.EpochEndOffset]()
  }

  /**
   * Fetches the epoch and local log start offset from the leader for the given partition and the current leader-epoch
   *
   * @param topicPartition     The topic partition that we want to fetch from
   * @param currentLeaderEpoch An int representing the current leader epoch of the requester
   * @return An OffsetAndEpoch object representing the earliest local offset and epoch in the leader's topic partition.
   */
  override def fetchEarliestLocalOffset(topicPartition: TopicPartition, currentLeaderEpoch: Int): OffsetAndEpoch = {
    new OffsetAndEpoch(0, 0)
  }

  /**
   * Builds a fetch request, given a partition map.
   *
   * @param partitions A map of topic partitions to their respective partition fetch state
   * @return A ResultWithPartitions, used to create the fetchRequest for fetch.
   */
  override def buildFetch(partitions: util.Map[TopicPartition, PartitionFetchState]): ResultWithPartitions[Optional[ReplicaFetch]] = {
    new ResultWithPartitions[Optional[ReplicaFetch]](Optional.empty(), util.Set.of())
  }
}
