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
import org.apache.kafka.server.config.MirrorConfig

import scala.collection.{Map, mutable}

/**
 * Cluster Mirror component that manages replica fetcher threads for cluster mirroring scenarios
 * where the local Kafka cluster needs to replicate data from multiple remote Kafka clusters.
 *
 * This manager extends the standard AbstractFetcherManager with specialized logic for handling
 * multiple cluster mirrors simultaneously. The key architectural decision is to ensure that
 * partitions from different cluster mirrors are assigned to separate fetcher threads, providing:
 *
 * 1. Authentication Isolation: Each cluster mirror can have its own authentication
 *    credentials and security configuration, preventing credential conflicts.
 *
 * 2. Configuration Isolation: Different cluster mirrors can have distinct connection
 *    properties, timeouts, and other networking configurations.
 *
 * 3. Load Balancing: Within each cluster mirror, partitions are distributed across
 *    multiple fetcher threads based on the num.cluster.mirror.replica.fetchers configuration.
 *
 * The manager uses a three-dimensional key (source broker endpoint + fetcher ID + mirror name)
 * to organize fetcher threads, ensuring proper isolation while maintaining efficient resource
 * utilization. This design allows a single Kafka cluster to simultaneously mirror data from
 * multiple remote clusters without interference between their respective configurations.
 */
class MirrorReplicaFetcherManager(brokerConfig: KafkaConfig,
                                  protected val replicaManager: ReplicaManager,
                                  metrics: Metrics,
                                  time: Time,
                                  quotaManager: ReplicationQuotaManager,
                                  metadataVersionSupplier: () => MetadataVersion,
                                  brokerEpochSupplier: () => Long,
                                  metadataCache: MetadataCache)
    extends AbstractFetcherManager[ReplicaFetcherThread](
      name = "MirrorReplicaFetcherManager on broker " + brokerConfig.brokerId,
      clientId = "MirrorReplica",
      numFetchers = brokerConfig.numMirrorReplicaFetchers) {
  private val remoteFetcherThreadMap = new mutable.HashMap[BrokerAndFetcherIdWithMirror, ReplicaFetcherThread]

  override def createFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint): ReplicaFetcherThread = {
    throw new UnsupportedOperationException("Use createMirrorFetcherThread for remote fetchers")
  }

  override def addFetcherForPartitions(partitionAndOffsets: Map[TopicPartition, InitialFetchState]): Unit = {
    // Ensures partitions with different cluster mirrors get separate fetcher threads.
    // This is crucial because different cluster mirrors may require different authentication credentials.
    val partitionsPerFetcher = partitionAndOffsets.groupBy { case (topicPartition, brokerAndInitialFetchOffset) =>
      BrokerAndFetcherIdWithMirror(
        brokerAndInitialFetchOffset.leader,
        getFetcherId(topicPartition),
        brokerAndInitialFetchOffset.mirrorName
      )
    }

    this.synchronized {
      def addAndStartFetcherThread(remoteFetcherKey: BrokerAndFetcherIdWithMirror): ReplicaFetcherThread = {
        val fetcherThread = createMirrorFetcherThread(remoteFetcherKey.fetcherId, remoteFetcherKey.sourceBroker,
          remoteFetcherKey.mirrorName)
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

  def createMirrorFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint, mirrorName: String): ReplicaFetcherThread = {
    info(s"!!! createMirrorFetcherThread: sourceBroker = $sourceBroker fetcherId = $fetcherId mirrorName = $mirrorName")
    val threadName = s"ReplicaFetcherThread-$fetcherId-${sourceBroker.id}-$mirrorName"
    val logContext = new LogContext(s"[ReplicaFetcher replicaId=${brokerConfig.brokerId}, leaderId=${sourceBroker.id}, " +
      s"fetcherId=$fetcherId, mirrorName=$mirrorName]")

    val endpoint = if (mirrorName.nonEmpty) {
      val mirrorProperties = metadataCache.config(new ConfigResource(ConfigResource.Type.MIRROR, mirrorName))
      info(s"Using mirror properties for $mirrorName: ${mirrorProperties.keySet()}")
      val mirrorConfig = MirrorConfig.fromProperties(mirrorProperties)
      new MirrorBrokerBlockingSender(sourceBroker, mirrorConfig, metrics, time, fetcherId,
        s"broker-${brokerConfig.brokerId}-fetcher-$fetcherId-mirror-$mirrorName", logContext)
    } else {
      throw new IllegalArgumentException("Mirror name must be provided for remote fetchers")
    }
    val fetchSessionHandler = new FetchSessionHandler(logContext, sourceBroker.id)
    val leader: LeaderEndPoint = new RemoteLeaderEndPoint(logContext.logPrefix, endpoint, fetchSessionHandler, brokerConfig,
      replicaManager, quotaManager, metadataVersionSupplier, brokerEpochSupplier)
    new ReplicaFetcherThread(threadName, leader, brokerConfig, failedPartitions, replicaManager,
      quotaManager, logContext.logPrefix, mirrorName)
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
      val keysToBeRemoved = new mutable.HashSet[BrokerAndFetcherIdWithMirror]
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
    info("Shutting down MirrorReplicaFetcherManager")
    closeAllFetchers()
    info("MirrorReplicaFetcherManager shutdown completed")
  }
}

/**
 * Three-dimensional key for grouping fetcher threads.
 *
 * Let's say you have:
 * - 2 remote cluster mirrors: cluster-A and cluster-B
 * - 2 brokers per cluster: broker-1, broker-2
 * - num.remote.replica.fetchers = 2
 *
 * The fetchers would be grouped like this:
 *
 * | Fetcher Thread                     | Cluster Mirror | Source Broker | Fetcher ID | Partitions           |
 * |------------------------------------|----------------|---------------|------------|----------------------|
 * | ReplicaFetcherThread-0-1-cluster-A | cluster-A      | broker-1      | 0          | topic1-p0, topic2-p2 |
 * | ReplicaFetcherThread-1-1-cluster-A | cluster-A      | broker-1      | 1          | topic1-p1, topic2-p3 |
 * | ReplicaFetcherThread-0-2-cluster-A | cluster-A      | broker-2      | 0          | topic3-p0, topic4-p2 |
 * | ReplicaFetcherThread-0-1-cluster-B | cluster-B      | broker-1      | 0          | topic5-p0, topic6-p1 |
 *
 * The grouping ensures that:
 * - Partitions from different cluster mirrors never share fetcher threads
 * - Load is balanced across the configured number of fetchers per broker
 * - Each fetcher thread has its own authentication context and configuration
 */
case class BrokerAndFetcherIdWithMirror(sourceBroker: BrokerEndPoint, fetcherId: Int, mirrorName: String)
