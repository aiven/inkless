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
 * Manages replica fetcher threads for cluster mirroring, assigning partitions from different
 * mirrors to separate threads for authentication, configuration, and load balancing isolation.
 */
class MirrorFetcherManager(brokerConfig: KafkaConfig,
                           protected val replicaManager: ReplicaManager,
                           metrics: Metrics,
                           time: Time,
                           quotaManager: ReplicationQuotaManager,
                           metadataVersionSupplier: () => MetadataVersion,
                           brokerEpochSupplier: () => Long,
                           metadataCache: MetadataCache)
    extends AbstractFetcherManager[MirrorFetcherThread](
      name = "MirrorFetcherManager on broker " + brokerConfig.brokerId,
      clientId = "MirrorReplica",
      numFetchers = brokerConfig.mirrorConfig.numReplicaFetchers) {
  private val mirrorFetcherThreadMap = new mutable.HashMap[FetcherThreadKey, MirrorFetcherThread]
  private val lagInfo = new mutable.HashMap[PartitionLagKey, LagInfo]

  override def deadThreadCount: Int = lock synchronized { mirrorFetcherThreadMap.values.count(_.isThreadFailed) }

  override def minFetchRate: Double = {
    // current min fetch rate across all fetchers/topics/partitions
    val headRate = mirrorFetcherThreadMap.values.headOption.map(_.fetcherStats.requestRate.oneMinuteRate).getOrElse(0.0)
    mirrorFetcherThreadMap.values.foldLeft(headRate)((curMinAll, fetcherThread) =>
      math.min(curMinAll, fetcherThread.fetcherStats.requestRate.oneMinuteRate))
  }

  override def maxLag: Long = {
    // current max lag across all fetchers/topics/partitions
    mirrorFetcherThreadMap.values.foldLeft(0L) { (curMaxLagAll, fetcherThread) =>
      val maxLagThread = fetcherThread.fetcherLagStats.stats.values.stream().mapToLong(v => v.lag).max().orElse(0L)
      math.max(curMaxLagAll, maxLagThread)
    }
  }

  override def createFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint): MirrorFetcherThread = {
    throw new UnsupportedOperationException("Use createMirrorFetcherThread for mirror fetchers")
  }

  override def addFetcherForPartitions(partitionAndOffsets: Map[TopicPartition, InitialFetchState]): Unit = {
    logger.debug("Adding fetcher for partitions, existing fetchers: {}", mirrorFetcherThreadMap.keys)
    // Ensures partitions with different cluster mirrors get separate fetcher threads.
    // This is crucial because different cluster mirrors may require different authentication credentials.
    val partitionsPerFetcher = partitionAndOffsets.groupBy { case (topicPartition, brokerAndInitialFetchOffset) =>
      FetcherThreadKey(
        brokerAndInitialFetchOffset.leader,
        getFetcherId(topicPartition),
        brokerAndInitialFetchOffset.mirrorName
      )
    }

    this.synchronized {
      def addAndStartFetcherThread(fetcherKey: FetcherThreadKey): MirrorFetcherThread = {
        val fetcherThread = createMirrorFetcherThread(fetcherKey.fetcherId, fetcherKey.sourceBroker,
          fetcherKey.mirrorName)
        mirrorFetcherThreadMap.put(fetcherKey, fetcherThread)
        fetcherThread.start()
        fetcherThread
      }

      for ((remoteFetcherKey, initialFetchOffsets) <- partitionsPerFetcher) {
        val fetcherThread = mirrorFetcherThreadMap.get(remoteFetcherKey) match {
          case Some(currentFetcherThread) if currentFetcherThread.leader.brokerEndPoint() == remoteFetcherKey.sourceBroker =>
            // reuse the fetcher thread
            logger.debug("Reusing mirror fetcher for {}", remoteFetcherKey)
            currentFetcherThread
          case Some(f) =>
            logger.debug("Recreating mirror fetcher for {}", remoteFetcherKey)
            f.shutdown()
            addAndStartFetcherThread(remoteFetcherKey)
          case None =>
            logger.debug("Creating new mirror fetcher for {}", remoteFetcherKey)
            addAndStartFetcherThread(remoteFetcherKey)
        }
        // failed partitions are removed when added partitions to thread
        addPartitionsToFetcherThread(fetcherThread, initialFetchOffsets)

        // Initialize lag information for newly added partitions
        initialFetchOffsets.foreach { case (topicPartition, initialState) =>
          val lagKey = PartitionLagKey(remoteFetcherKey.mirrorName, topicPartition)
          // Initialize with 0 values until first fetch updates it
          val destinationOffset = replicaManager.getPartition(topicPartition) match {
            case HostedPartition.Online(partition) =>
              partition.log.map(_.highWatermark).getOrElse(0L)
            case _ => 0L
          }
          lagInfo.put(lagKey, LagInfo(destinationOffset, destinationOffset, 0, time.milliseconds()))
        }
      }
    }
  }

  private def createMirrorFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint, mirrorName: String): MirrorFetcherThread = {
    info(s"Creating mirror fetcher thread: fetcherId = $fetcherId, sourceBroker = $sourceBroker, mirrorName = $mirrorName")
    val threadName = s"MirrorFetcherThread-${sourceBroker.id}-$fetcherId-$mirrorName"
    val logContext = new LogContext(s"[MirrorFetcher fetcherId=$fetcherId, mirrorName=$mirrorName, " +
      s"srcBroker=${sourceBroker.id}, dstBroker=${brokerConfig.brokerId}] ")

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
      replicaManager, quotaManager, metadataVersionSupplier, brokerEpochSupplier, isCrossClusterMirror = true)
    new MirrorFetcherThread(threadName, leader, brokerConfig, failedPartitions, replicaManager,
      quotaManager, logContext.logPrefix, mirrorName)
  }

  override def removeFetcherForPartitions(partitions: scala.collection.Set[TopicPartition]): scala.collection.Map[TopicPartition, PartitionFetchState] = {
    val fetchStates = mutable.Map.empty[TopicPartition, PartitionFetchState]
    this.synchronized {
      for ((key, fetcher) <- mirrorFetcherThreadMap) {
        val removed = fetcher.removePartitions(partitions)
        fetchStates ++= removed
        // Remove lag cache entries for partitions that were actually removed
        for (partition <- removed.keys) {
          val lagKey = PartitionLagKey(key.mirrorName, partition)
          lagInfo.remove(lagKey)
        }
      }
      failedPartitions.removeAll(partitions)
    }
    // Only log if we actually removed mirror partitions (not regular partitions)
    if (fetchStates.nonEmpty)
      info(s"Removed mirror fetcher for partitions ${fetchStates.keySet}")
    fetchStates
  }

  override def shutdownIdleFetcherThreads(): Unit = {
    this.synchronized {
      val keysToBeRemoved = new mutable.HashSet[FetcherThreadKey]
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

  def updatePartitionLag(mirrorName: String, topicPartition: TopicPartition, sourceOffset: Long, destinationOffset: Long): Unit = {
    this.synchronized {
      val key = PartitionLagKey(mirrorName, topicPartition)
      val lag = Math.max(0, sourceOffset - destinationOffset)
      lagInfo.put(key, LagInfo(sourceOffset, destinationOffset, lag, time.milliseconds()))
    }
  }

  def getMirrorLagInfo(mirrorName: String): Map[TopicPartition, LagInfo] = {
    this.synchronized {
      lagInfo.collect {
        case (key, lagInfo) if key.mirrorName == mirrorName => key.topicPartition -> lagInfo
      }.toMap
    }
  }

  def shutdown(): Unit = {
    info("Shutting down MirrorFetcherManager")
    closeAllFetchers()
    lagInfo.clear()
    info("MirrorFetcherManager shutdown completed")
  }
}

/**
 * Three-dimensional key for grouping mirror fetcher threads.
 *
 * Multiple partitions share the same fetcher thread when they have identical keys
 * (source broker, fetcher ID, and mirror name). This key determines thread reuse.
 *
 * Thread creation rules:
 * - Same source broker + same fetcher ID + same mirror -> REUSE thread
 * - Different source broker -> NEW thread (source leader changed)
 * - Different mirror name -> NEW thread (different auth credentials)
 * - Different fetcher ID -> NEW thread (load balancing)
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
 */
case class FetcherThreadKey(sourceBroker: BrokerEndPoint, fetcherId: Int, mirrorName: String)

case class PartitionLagKey(mirrorName: String, topicPartition: TopicPartition)

case class LagInfo(sourceOffset: Long, destinationOffset: Long, lag: Long, lastUpdateMs: Long)
