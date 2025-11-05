/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server.mirror

import kafka.server._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.server.{LeaderEndPoint, PartitionFetchState}

import scala.collection.{Map, Set}

/**
 * Specialized fetcher thread for cross-cluster mirroring.
 *
 * This class extends ReplicaFetcherThread to handle partitions that replicate from remote clusters.
 * The key difference is that when the remote leader changes, this thread uses the
 * MirrorFetcherManager instead of the regular ReplicaFetcherManager to recreate fetchers.
 *
 * This ensures that when a leader election occurs in the source cluster, the mirror fetcher
 * is properly recreated with the new leader's connection details.
 *
 * @param name The name of the thread
 * @param leader The remote leader endpoint to fetch from (RemoteLeaderEndPoint)
 * @param brokerConfig Broker configuration
 * @param failedPartitions Tracker for partitions with errors
 * @param replicaMgr The local ReplicaManager
 * @param quota Replication quota for throttling
 * @param logPrefix Log prefix for this thread
 * @param mirrorName The name of the cluster mirror configuration
 */
class MirrorFetcherThread(name: String,
                          leader: LeaderEndPoint,
                          brokerConfig: KafkaConfig,
                          failedPartitions: FailedPartitions,
                          replicaMgr: ReplicaManager,
                          quota: ReplicaQuota,
                          logPrefix: String,
                          mirrorName: String)
  extends ReplicaFetcherThread(name, leader, brokerConfig, failedPartitions, replicaMgr, quota, logPrefix, mirrorName) {
  /**
   * Uses MirrorFetcherManager instead of regular ReplicaFetcherManager.
   * This ensures proper handling of cross-cluster fetcher lifecycle when leader changes occur.
   */
  override protected def removeFetcherForPartitions(partitions: Set[TopicPartition]): Map[TopicPartition, PartitionFetchState] = {
    replicaMgr.mirrorFetcherManager.removeFetcherForPartitions(partitions)
  }

  /**
   * Uses MirrorFetcherManager instead of regular ReplicaFetcherManager.
   * When the remote leader changes, this method triggers the recreation logic in
   * MirrorFetcherManager.addFetcherForPartitions, which compares the new
   * leader's BrokerEndPoint with the existing fetcher's leader and recreates if different.
   */
  override protected def addFetcherForPartitions(partitionAndOffsets: Map[TopicPartition, InitialFetchState]): Unit = {
    replicaMgr.mirrorFetcherManager.addFetcherForPartitions(partitionAndOffsets)
  }
}
