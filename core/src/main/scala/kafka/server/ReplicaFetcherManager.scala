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
import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.server.network.BrokerEndPoint
import org.apache.kafka.server.LeaderEndPoint
import org.apache.kafka.server.config.ClusterLinkConfigs

import java.util.Properties
import scala.collection.Map

case class BrokerAndFetcherIdWithClusterLink(broker: BrokerEndPoint, fetcherId: Int, readOnly: Boolean = false, clusterLinkName: String = "")

class ReplicaFetcherManager(brokerConfig: KafkaConfig,
                            protected val replicaManager: ReplicaManager,
                            metrics: Metrics,
                            time: Time,
                            quotaManager: ReplicationQuotaManager,
                            metadataVersionSupplier: () => MetadataVersion,
                            brokerEpochSupplier: () => Long,
                            metadataCache: org.apache.kafka.metadata.MetadataCache)
      extends AbstractFetcherManager[ReplicaFetcherThread](
        name = "ReplicaFetcherManager on broker " + brokerConfig.brokerId,
        clientId = "Replica",
        numFetchers = brokerConfig.numReplicaFetchers) {

  override def createFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint, readOnly: Boolean): ReplicaFetcherThread = {
    info("!!! createFetcherThread: sourceBroker = " + sourceBroker + " fetcherId = " + fetcherId + " readOnly = " + readOnly)
    val threadName = s"ReplicaFetcherThread-$fetcherId-${sourceBroker.id}"
    val logContext = new LogContext(s"[ReplicaFetcher replicaId=${brokerConfig.brokerId}, leaderId=${sourceBroker.id}, " +
      s"fetcherId=$fetcherId, readOnly=$readOnly] ")

    val endpoint = if (readOnly) {
      // For connecting to remote clusters, we need to get authentication config from cluster link properties.
      // Create a default ClusterLinkConfigs with empty properties for now.
      // It will be updated when partitions are added in createClusterLinkFetcherThread.
      new RemoteBrokerBlockingSender(sourceBroker, ClusterLinkConfigs.fromProperties(new Properties()), metrics, time, fetcherId,
        s"broker-${brokerConfig.brokerId}-remote-fetcher-$fetcherId", logContext)
    } else {
      new BrokerBlockingSender(sourceBroker, brokerConfig, metrics, time, fetcherId,
      s"broker-${brokerConfig.brokerId}-fetcher-$fetcherId", logContext)
    }
    val fetchSessionHandler = new FetchSessionHandler(logContext, sourceBroker.id)
    val leader: LeaderEndPoint = new RemoteLeaderEndPoint(logContext.logPrefix, endpoint, fetchSessionHandler, brokerConfig,
      replicaManager, quotaManager, metadataVersionSupplier, brokerEpochSupplier)
    new ReplicaFetcherThread(threadName, leader, brokerConfig, failedPartitions, replicaManager,
      quotaManager, logContext.logPrefix)
  }

  override def addFetcherForPartitions(partitionAndOffsets: Map[TopicPartition, InitialFetchState]): Unit = {
    // Check if we have any partitions that need cluster link authentication
    val partitionsWithClusterLink = partitionAndOffsets.filter(_._2.clusterLinkName.nonEmpty)
    val partitionsWithoutClusterLink = partitionAndOffsets.filter(_._2.clusterLinkName.isEmpty)

    // Handle regular partitions delegating to parent
    if (partitionsWithoutClusterLink.nonEmpty) {
      super.addFetcherForPartitions(partitionsWithoutClusterLink)
    }

    // Handle partitions with cluster link
    if (partitionsWithClusterLink.nonEmpty) {
      // Ensures partitions with different cluster links get separate fetcher threads.
      // This is crucial because different cluster links may require different authentication credentials.
      val partitionsPerFetcher = partitionsWithClusterLink.groupBy { case (topicPartition, brokerAndInitialFetchOffset) =>
        BrokerAndFetcherIdWithClusterLink(
          brokerAndInitialFetchOffset.leader,
          getFetcherId(topicPartition),
          brokerAndInitialFetchOffset.readOnly,
          brokerAndInitialFetchOffset.clusterLinkName
        )
      }

      this.synchronized {
        def addAndStartFetcherThread(brokerAndFetcherId: BrokerAndFetcherIdWithClusterLink,
                                     brokerIdAndFetcherId: BrokerIdAndFetcherId): ReplicaFetcherThread = {
          val fetcherThread = createClusterLinkFetcherThread(brokerAndFetcherId.fetcherId, brokerAndFetcherId.broker,
            brokerAndFetcherId.readOnly, brokerAndFetcherId.clusterLinkName)
          fetcherThreadMap.put(brokerIdAndFetcherId, fetcherThread)
          fetcherThread.start()
          fetcherThread
        }

        for ((brokerAndFetcherId, initialFetchOffsets) <- partitionsPerFetcher) {
          val brokerIdAndFetcherId = BrokerIdAndFetcherId(brokerAndFetcherId.broker.id, brokerAndFetcherId.fetcherId)
          val fetcherThread = fetcherThreadMap.get(brokerIdAndFetcherId) match {
            case Some(currentFetcherThread) if currentFetcherThread.leader.brokerEndPoint() == brokerAndFetcherId.broker =>
              info(s"!!! Recreating remote fetcher thread for cluster link: ${brokerAndFetcherId.clusterLinkName}")
              currentFetcherThread.shutdown()
              addAndStartFetcherThread(brokerAndFetcherId, brokerIdAndFetcherId)
            case Some(f) =>
              f.shutdown()
              addAndStartFetcherThread(brokerAndFetcherId, brokerIdAndFetcherId)
            case None =>
              addAndStartFetcherThread(brokerAndFetcherId, brokerIdAndFetcherId)
          }
          // failed partitions are removed when added partitions to thread
          addPartitionsToFetcherThread(fetcherThread, initialFetchOffsets)
        }
      }
    }
  }

  def createClusterLinkFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint, readOnly: Boolean, clusterLinkName: String): ReplicaFetcherThread = {
    info(s"!!! createClusterLinkFetcherThread: sourceBroker = $sourceBroker fetcherId = $fetcherId readOnly = $readOnly clusterLinkName = $clusterLinkName")
    val threadName = s"ReplicaFetcherThread-$fetcherId-${sourceBroker.id}-$clusterLinkName"
    val logContext = new LogContext(s"[ReplicaFetcher replicaId=${brokerConfig.brokerId}, leaderId=${sourceBroker.id}, " +
      s"fetcherId=$fetcherId, readOnly=$readOnly, clusterLinkName=$clusterLinkName] ")

    val endpoint = if (readOnly && clusterLinkName.nonEmpty) {
      val clusterLinkProperties = metadataCache.config(new ConfigResource(ConfigResource.Type.CLUSTER_LINK, clusterLinkName))
      info(s"!!! Using cluster link properties for $clusterLinkName: ${clusterLinkProperties.keySet()}")
      val clusterLinkConfigs = ClusterLinkConfigs.fromProperties(clusterLinkProperties)
      new RemoteBrokerBlockingSender(sourceBroker, clusterLinkConfigs, metrics, time, fetcherId,
        s"broker-${brokerConfig.brokerId}-remote-fetcher-$fetcherId", logContext)
    } else {
      new BrokerBlockingSender(sourceBroker, brokerConfig, metrics, time, fetcherId,
      s"broker-${brokerConfig.brokerId}-fetcher-$fetcherId", logContext)
    }
    val fetchSessionHandler = new FetchSessionHandler(logContext, sourceBroker.id)
    val leader: LeaderEndPoint = new RemoteLeaderEndPoint(logContext.logPrefix, endpoint, fetchSessionHandler, brokerConfig,
      replicaManager, quotaManager, metadataVersionSupplier, brokerEpochSupplier)
    new ReplicaFetcherThread(threadName, leader, brokerConfig, failedPartitions, replicaManager,
      quotaManager, logContext.logPrefix)
  }

  def shutdown(): Unit = {
    info("shutting down")
    closeAllFetchers()
    info("shutdown completed")
  }
}
