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

import org.apache.kafka.clients.FetchSessionHandler
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.{LogContext, Time}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.metadata.MetadataCache
import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.server.network.BrokerEndPoint
import org.apache.kafka.server.LeaderEndPoint
import org.apache.kafka.server.PartitionFetchState
import org.apache.kafka.server.config.ClusterLinkConfigs

import scala.collection.{Map, mutable}

/**
 * Dedicated fetcher manager for remote cluster link replication.
 * Handles fetcher threads that connect to remote clusters using cluster link authentication.
 */
class RemoteReplicaFetcherManager(brokerConfig: KafkaConfig,
                                  protected val replicaManager: ReplicaManager,
                                  metrics: Metrics,
                                  time: Time,
                                  quotaManager: ReplicationQuotaManager,
                                  metadataVersionSupplier: () => MetadataVersion,
                                  brokerEpochSupplier: () => Long,
                                  metadataCache: MetadataCache)
    extends AbstractFetcherManager[ReplicaFetcherThread](
      name = "RemoteReplicaFetcherManager on broker " + brokerConfig.brokerId,
      clientId = "RemoteReplica",
      numFetchers = brokerConfig.numRemoteReplicaFetchers) {
  private val remoteFetcherThreadMap = new mutable.HashMap[BrokerAndFetcherIdWithClusterLink, ReplicaFetcherThread]

  override def createFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint): ReplicaFetcherThread = {
    throw new UnsupportedOperationException("Use createClusterLinkFetcherThread for remote fetchers")
  }

  override def addFetcherForPartitions(partitionAndOffsets: Map[TopicPartition, InitialFetchState]): Unit = {
    // Ensures partitions with different cluster links get separate fetcher threads.
    // This is crucial because different cluster links may require different authentication credentials.
    val partitionsPerFetcher = partitionAndOffsets.groupBy { case (topicPartition, brokerAndInitialFetchOffset) =>
      BrokerAndFetcherIdWithClusterLink(
        brokerAndInitialFetchOffset.leader,
        getFetcherId(topicPartition),
        brokerAndInitialFetchOffset.clusterLinkName
      )
    }

    this.synchronized {
      def addAndStartFetcherThread(remoteFetcherKey: BrokerAndFetcherIdWithClusterLink): ReplicaFetcherThread = {
        val fetcherThread = createClusterLinkFetcherThread(remoteFetcherKey.fetcherId, remoteFetcherKey.sourceBroker,
          remoteFetcherKey.clusterLinkName)
        remoteFetcherThreadMap.put(remoteFetcherKey, fetcherThread)
        fetcherThread.start()
        fetcherThread
      }

      for ((remoteFetcherKey, initialFetchOffsets) <- partitionsPerFetcher) {
        val fetcherThread = remoteFetcherThreadMap.get(remoteFetcherKey) match {
          case Some(currentFetcherThread) if currentFetcherThread.leader.brokerEndPoint() == remoteFetcherKey.sourceBroker =>
            // reuse the fetcher thread
            currentFetcherThread
          case Some(f) =>
            f.shutdown()
            addAndStartFetcherThread(remoteFetcherKey)
          case None =>
            addAndStartFetcherThread(remoteFetcherKey)
        }
        // failed partitions are removed when added partitions to thread
        addPartitionsToFetcherThread(fetcherThread, initialFetchOffsets)
      }
    }
  }

  def createClusterLinkFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint, clusterLinkName: String): ReplicaFetcherThread = {
    info(s"!!! createClusterLinkFetcherThread: sourceBroker = $sourceBroker fetcherId = $fetcherId clusterLinkName = $clusterLinkName")
    val threadName = s"ReplicaFetcherThread-$fetcherId-${sourceBroker.id}-$clusterLinkName"
    val logContext = new LogContext(s"[ReplicaFetcher replicaId=${brokerConfig.brokerId}, leaderId=${sourceBroker.id}, " +
      s"fetcherId=$fetcherId, clusterLinkName=$clusterLinkName]")

    val endpoint = if (clusterLinkName.nonEmpty) {
      val clusterLinkProperties = metadataCache.config(new ConfigResource(ConfigResource.Type.CLUSTER_LINK, clusterLinkName))
      info(s"Using cluster link properties for $clusterLinkName: ${clusterLinkProperties.keySet()}")
      val clusterLinkConfigs = ClusterLinkConfigs.fromProperties(clusterLinkProperties)
      new RemoteBrokerBlockingSender(sourceBroker, clusterLinkConfigs, metrics, time, fetcherId,
        s"broker-${brokerConfig.brokerId}-fetcher-$fetcherId-link-$clusterLinkName", logContext)
    } else {
      throw new IllegalArgumentException("Cluster link name must be provided for remote fetchers")
    }
    val fetchSessionHandler = new FetchSessionHandler(logContext, sourceBroker.id)
    val leader: LeaderEndPoint = new RemoteLeaderEndPoint(logContext.logPrefix, endpoint, fetchSessionHandler, brokerConfig,
      replicaManager, quotaManager, metadataVersionSupplier, brokerEpochSupplier)
    new ReplicaFetcherThread(threadName, leader, brokerConfig, failedPartitions, replicaManager,
      quotaManager, logContext.logPrefix, clusterLinkName)
  }

  override def removeFetcherForPartitions(partitions: scala.collection.Set[TopicPartition]): scala.collection.Map[TopicPartition, PartitionFetchState] = {
    val fetchStates = mutable.Map.empty[TopicPartition, PartitionFetchState]
    this.synchronized {
      for (fetcher <- remoteFetcherThreadMap.values)
        fetchStates ++= fetcher.removePartitions(partitions)
      failedPartitions.removeAll(partitions)
    }
    if (partitions.nonEmpty)
      info(s"Removed remote fetcher for partitions $partitions")
    fetchStates
  }

  override def shutdownIdleFetcherThreads(): Unit = {
    this.synchronized {
      val keysToBeRemoved = new mutable.HashSet[BrokerAndFetcherIdWithClusterLink]
      for ((key, fetcher) <- remoteFetcherThreadMap) {
        if (fetcher.partitionCount <= 0) {
          fetcher.shutdown()
          keysToBeRemoved += key
        }
      }
      remoteFetcherThreadMap --= keysToBeRemoved
    }
  }

  override def closeAllFetchers(): Unit = {
    this.synchronized {
      for ((_, fetcher) <- remoteFetcherThreadMap) {
        fetcher.initiateShutdown()
      }

      for ((_, fetcher) <- remoteFetcherThreadMap) {
        fetcher.shutdown()
      }
      remoteFetcherThreadMap.clear()
    }
  }

  def shutdown(): Unit = {
    info("Shutting down RemoteReplicaFetcherManager")
    closeAllFetchers()
    info("RemoteReplicaFetcherManager shutdown completed")
  }
}

/**
 * Three-dimensional key for grouping fetcher threads.
 *
 * Let's say you have:
 * - 2 remote cluster links: cluster-A and cluster-B
 * - 2 brokers per cluster: broker-1, broker-2
 * - num.remote.replica.fetchers = 2
 *
 * The fetchers would be grouped like this:
 *
 * | Fetcher Thread                     | Cluster Link | Source Broker | Fetcher ID | Partitions           |
 * |------------------------------------|--------------|---------------|------------|----------------------|
 * | ReplicaFetcherThread-0-1-cluster-A | cluster-A    | broker-1      | 0          | topic1-p0, topic2-p2 |
 * | ReplicaFetcherThread-1-1-cluster-A | cluster-A    | broker-1      | 1          | topic1-p1, topic2-p3 |
 * | ReplicaFetcherThread-0-2-cluster-A | cluster-A    | broker-2      | 0          | topic3-p0, topic4-p2 |
 * | ReplicaFetcherThread-0-1-cluster-B | cluster-B    | broker-1      | 0          | topic5-p0, topic6-p1 |
 *
 * The grouping ensures that:
 * - Partitions from different cluster links never share fetcher threads
 * - Load is balanced across the configured number of fetchers per broker
 * - Each fetcher thread has its own authentication context and configuration
 */
case class BrokerAndFetcherIdWithClusterLink(sourceBroker: BrokerEndPoint, fetcherId: Int, clusterLinkName: String)
