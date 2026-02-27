/*
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

import kafka.utils.Logging
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.server.PartitionFetchState
import org.apache.kafka.server.network.BrokerEndPoint

import scala.collection.mutable

/**
 * Manager for WAL unification fetcher threads (Option A): does not extend AbstractFetcherManager.
 * Holds a fixed pool of N worker threads, assigns partitions by the same hash as replica fetchers,
 * and exposes the same public API for ReplicaManager: addFetcherForPartitions, removeFetcherForPartitions,
 * shutdown, shutdownIdleFetcherThreads, syntheticBrokerEndPoint.
 */
class WalUnificationFetcherManager(
  brokerConfig: KafkaConfig,
  replicaMgr: ReplicaManager,
  walFetchHelper: WalUnificationFetchHelper
) extends Logging {

  private val numFetchers = brokerConfig.disklessTsUnificationFetchers
  private val fetcherThreadMap = mutable.Map.empty[Int, WalUnificationFetcherThread]
  private val lock = new Object
  val failedPartitions = new FailedPartitions

  /** Synthetic broker used only for constructing InitialFetchState when adding partitions. */
  val syntheticBrokerEndPoint: BrokerEndPoint = new BrokerEndPoint(-1, "wal-unification", -1)

  this.logIdent = s"[WalUnificationFetcherManager broker=${brokerConfig.brokerId}] "

  private def getFetcherId(topicPartition: TopicPartition): Int = {
    Utils.abs(31 * topicPartition.topic.hashCode() + topicPartition.partition) % numFetchers
  }

  def addFetcherForPartitions(tp: TopicPartition, fetchState: InitialFetchState): Unit = {
    lock.synchronized {
      val fetcherId = getFetcherId(tp)
      val thread = fetcherThreadMap.getOrElseUpdate(fetcherId, {
        val name = s"WalUnificationFetcherThread-$fetcherId"
        val t = new WalUnificationFetcherThread(
          name,
          fetcherId,
          walFetchHelper,
          brokerConfig,
          failedPartitions,
          replicaMgr
        )
        t.start()
        t
      })
      thread.addPartitions(Map(tp -> fetchState))
      info(s"Added WAL unification fetcher for partition $tp")
    }
  }

  def removeFetcherForPartitions(partitions: Set[TopicPartition]): Map[TopicPartition, PartitionFetchState] = {
    val fetchStates = mutable.Map.empty[TopicPartition, PartitionFetchState]
    val partitionsImmutable = partitions.toSet
    lock.synchronized {
      fetcherThreadMap.values.foreach { thread =>
        fetchStates ++= thread.removePartitions(partitionsImmutable)
      }
      failedPartitions.removeAll(partitionsImmutable)
    }
    if (partitions.nonEmpty)
      info(s"Removed WAL unification fetcher for partitions $partitions")
    fetchStates.toMap
  }

  def addFailedPartition(topicPartition: TopicPartition): Unit = {
    lock.synchronized {
      failedPartitions.add(topicPartition)
    }
  }

  def shutdownIdleFetcherThreads(): Unit = {
    lock.synchronized {
      val toRemove = fetcherThreadMap.filter { case (_, thread) => thread.partitionCount <= 0 }
      toRemove.foreach { case (id, thread) =>
        thread.shutdown()
        fetcherThreadMap.remove(id)
      }
    }
  }

  def shutdown(): Unit = {
    info("Shutting down WAL unification fetcher manager")
    lock.synchronized {
      fetcherThreadMap.values.foreach { t => t.initiateShutdown() }
      fetcherThreadMap.values.foreach { t => t.shutdown() }
      fetcherThreadMap.clear()
    }
    walFetchHelper.close()
    info("WAL unification fetcher manager shutdown complete")
  }
}
