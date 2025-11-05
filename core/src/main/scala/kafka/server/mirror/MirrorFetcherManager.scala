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

package kafka.server.mirror

import kafka.server._
import org.apache.kafka.clients.FetchSessionHandler
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.{LogContext, Time}
import org.apache.kafka.metadata.MetadataCache
import org.apache.kafka.server.{LeaderEndPoint, PartitionFetchState}
import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.server.config.MirrorConfig
import org.apache.kafka.server.network.BrokerEndPoint

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
      name = "MirrorFetcherManager on broker " + brokerConfig.brokerId,
      clientId = "MirrorReplica",
      numFetchers = brokerConfig.numMirrorReplicaFetchers) {
  private val mirrorFetcherThreadMap = new mutable.HashMap[BrokerAndFetcherIdWithMirror, ReplicaFetcherThread]

  override def createFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint): ReplicaFetcherThread = {
    throw new UnsupportedOperationException("Use createMirrorFetcherThread for mirror fetchers")
  }

  override def addFetcherForPartitions(partitionAndOffsets: Map[TopicPartition, InitialFetchState]): Unit = {
    logger.info("#### mirrorFetcherThreadMap: " + mirrorFetcherThreadMap.keys)
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
        mirrorFetcherThreadMap.put(remoteFetcherKey, fetcherThread)
        fetcherThread.start()
        fetcherThread
      }

      for ((remoteFetcherKey, initialFetchOffsets) <- partitionsPerFetcher) {
        val fetcherThread = mirrorFetcherThreadMap.get(remoteFetcherKey) match {
          case Some(currentFetcherThread) if currentFetcherThread.leader.brokerEndPoint() == remoteFetcherKey.sourceBroker =>
            // reuse the fetcher thread
            logger.info("#### Reusing mirror fetcher")
            currentFetcherThread
          case Some(f) =>
            logger.info("#### Recreating mirror fetcher")
            f.shutdown()
            addAndStartFetcherThread(remoteFetcherKey)
          case None =>
            logger.info("#### Creating new mirror fetcher")
            addAndStartFetcherThread(remoteFetcherKey)
        }
        // failed partitions are removed when added partitions to thread
        addPartitionsToFetcherThread(fetcherThread, initialFetchOffsets)
      }
    }
  }

  private def createMirrorFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint, mirrorName: String): MirrorFetcherThread = {
    info(s"Creating mirror fetcher thread: fetcherId = $fetcherId, sourceBroker = $sourceBroker, mirrorName = $mirrorName")
    val threadName = s"MirrorFetcherThread-${sourceBroker.id}-$fetcherId-$mirrorName"
    val logContext = new LogContext(s"[MirrorFetcher fetcherId=$fetcherId, mirrorName=$mirrorName, " +
      s"sourceBroker=${sourceBroker.id}, destBroker=${brokerConfig.brokerId}] ")

    val endpoint = if (mirrorName.nonEmpty) {
      val mirrorProperties = metadataCache.config(new ConfigResource(ConfigResource.Type.MIRROR, mirrorName))
      info(s"Using mirror properties for $mirrorName: ${mirrorProperties.keySet()}")
      val mirrorConfig = MirrorConfig.fromProperties(mirrorProperties)
      new MirrorBlockingSender(sourceBroker, mirrorConfig, metrics, time, fetcherId,
        s"broker-${brokerConfig.brokerId}-fetcher-$fetcherId-mirror-$mirrorName", logContext)
    } else {
      throw new IllegalArgumentException("Mirror name must be provided for remote fetchers")
    }
    val fetchSessionHandler = new FetchSessionHandler(logContext, sourceBroker.id)
    val leader: LeaderEndPoint = new RemoteLeaderEndPoint(logContext.logPrefix, endpoint, fetchSessionHandler, brokerConfig,
      replicaManager, quotaManager, metadataVersionSupplier, brokerEpochSupplier)
    new MirrorFetcherThread(threadName, leader, brokerConfig, failedPartitions, replicaManager,
      quotaManager, logContext.logPrefix, mirrorName)
  }

  override def removeFetcherForPartitions(partitions: scala.collection.Set[TopicPartition]): scala.collection.Map[TopicPartition, PartitionFetchState] = {
    val fetchStates = mutable.Map.empty[TopicPartition, PartitionFetchState]
    this.synchronized {
      for (fetcher <- mirrorFetcherThreadMap.values)
        fetchStates ++= fetcher.removePartitions(partitions)
      failedPartitions.removeAll(partitions)
    }
    if (partitions.nonEmpty)
      info(s"#### Removed mirror fetcher for partitions $partitions")
    fetchStates
  }

  override def shutdownIdleFetcherThreads(): Unit = {
    this.synchronized {
      val keysToBeRemoved = new mutable.HashSet[BrokerAndFetcherIdWithMirror]
      for ((key, fetcher) <- mirrorFetcherThreadMap) {
        if (fetcher.partitionCount <= 0) {
          fetcher.shutdown()
          keysToBeRemoved += key
        }
      }
      mirrorFetcherThreadMap --= keysToBeRemoved
    }
  }

  override def closeAllFetchers(): Unit = {
    this.synchronized {
      for ((_, fetcher) <- mirrorFetcherThreadMap) {
        fetcher.initiateShutdown()
      }

      for ((_, fetcher) <- mirrorFetcherThreadMap) {
        fetcher.shutdown()
      }
      mirrorFetcherThreadMap.clear()
    }
  }

  def shutdown(): Unit = {
    info("Shutting down MirrorFetcherManager")
    closeAllFetchers()
    info("MirrorFetcherManager shutdown completed")
  }
}

/**
 * Three-dimensional key for grouping mirror fetcher threads.
 *
 * Multiple partitions share the same fetcher thread when they have identical keys
 * (same source broker, fetcher ID, and mirror name). This key determines thread reuse.
 *
 * Example with num.mirror.replica.fetchers = 2:
 *
 * | Partition  | Source Leader | Fetcher ID | Mirror Name | Key                      | Thread Reused? |
 * |------------|---------------|------------|-------------|--------------------------|----------------|
 * | topic1-p0  | broker-1      | 0          | cluster-A   | (broker-1, 0, cluster-A) | New thread     |
 * | topic1-p1  | broker-1      | 1          | cluster-A   | (broker-1, 1, cluster-A) | New thread     |
 * | topic2-p0  | broker-1      | 0          | cluster-A   | (broker-1, 0, cluster-A) | Reuse          |
 * | topic2-p1  | broker-1      | 1          | cluster-A   | (broker-1, 1, cluster-A) | Reuse          |
 * | topic3-p0  | broker-2      | 0          | cluster-A   | (broker-2, 0, cluster-A) | New thread     |
 * | topic4-p0  | broker-1      | 0          | cluster-B   | (broker-1, 0, cluster-B) | New thread     |
 *
 * Result: 4 fetcher threads created, each serving multiple partitions:
 * - MirrorFetcherThread-0-1-cluster-A: [topic1-p0, topic2-p0] --- Same key
 * - MirrorFetcherThread-1-1-cluster-A: [topic1-p1, topic2-p1] --- Same key
 * - MirrorFetcherThread-0-2-cluster-A: [topic3-p0]
 * - MirrorFetcherThread-0-1-cluster-B: [topic4-p0]
 *
 * Thread creation rules:
 * - Same source broker + same fetcher ID + same mirror -> REUSE thread
 * - Different source broker -> NEW thread (source leader changed)
 * - Different mirror name -> NEW thread (different auth credentials)
 * - Different fetcher ID -> NEW thread (load balancing)
 */
case class BrokerAndFetcherIdWithMirror(sourceBroker: BrokerEndPoint, fetcherId: Int, mirrorName: String)
