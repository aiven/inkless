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

import java.util.{Collections, Properties}
import kafka.server.QuotaFactory.QuotaManagers
import kafka.utils.Logging
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.server.config.QuotaConfig
import org.apache.kafka.common.metrics.Quota._
import org.apache.kafka.coordinator.group.GroupCoordinator
import org.apache.kafka.server.ClientMetricsManager
import org.apache.kafka.server.common.StopPartition
import org.apache.kafka.server.log.remote.TopicPartitionLog
import org.apache.kafka.storage.internals.log.{LogStartOffsetIncrementReason, ThrottledReplicaListValidator, UnifiedLog}

import scala.jdk.CollectionConverters._
import scala.collection.Seq

/**
  * The ConfigHandler is used to process broker configuration change notifications.
  */
trait ConfigHandler {
  def processConfigChanges(entityName: String, value: Properties): Unit
}

/**
  * The TopicConfigHandler will process topic config changes from the metadata log.
  * The callback provides the topic name and the full properties set.
  *
  * @param replicaManager The ReplicaManager to access logs and partitions
  * @param kafkaConfig The broker configuration
  * @param quotas The quota managers for throttled replicas
  * @param disklessMigrationHandler Optional handler for diskless migration (Delos-style chain sealing)
  */
class TopicConfigHandler(private val replicaManager: ReplicaManager,
                         kafkaConfig: KafkaConfig,
                         val quotas: QuotaManagers,
                         val disklessMigrationHandler: Option[DisklessMigrationHandler] = None) extends ConfigHandler with Logging  {

  private def updateLogConfig(topic: String,
                              topicConfig: Properties): Unit = {
    val logManager = replicaManager.logManager

    val logs = logManager.logsByTopic(topic)
    val wasRemoteLogEnabled = logs.exists(_.remoteLogEnabled())
    val wasCopyDisabled = logs.exists(_.config.remoteLogCopyDisable())
    // Check if diskless was previously enabled (before the config update)
    val wasDisklessEnabled = logs.exists(_.config.disklessEnable())

    logManager.updateTopicConfig(topic, topicConfig, kafkaConfig.remoteLogManagerConfig.isRemoteStorageSystemEnabled,
      wasRemoteLogEnabled)
    maybeUpdateRemoteLogComponents(topic, logs, wasRemoteLogEnabled, wasCopyDisabled)

    // Handle diskless migration: seal the chain when transitioning from classic to diskless
    maybeHandleDisklessMigration(topic, wasDisklessEnabled, topicConfig)
  }

  /**
   * Handles the migration from classic (diskful) to diskless storage.
   *
   * When diskless.enable is switched from false to true, this method:
   * 1. Seals all leader partitions (via the DisklessMigrationHandler)
   * 2. Initializes the diskless log in the control plane with the captured B0 offset
   *
   * This implements the Delos-style chain sealing protocol for safe online migration.
   *
   * @param topic The topic being configured
   * @param wasDisklessEnabled Whether diskless was enabled before this config change
   * @param topicConfig The new topic configuration
   */
  private def maybeHandleDisklessMigration(topic: String, wasDisklessEnabled: Boolean, topicConfig: Properties): Unit = {
    val isDisklessEnabled = topicConfig.getProperty(TopicConfig.DISKLESS_ENABLE_CONFIG, "false").toBoolean

    if (!wasDisklessEnabled && isDisklessEnabled) {
      info(s"Detected diskless migration for topic $topic: diskless.enable changed from false to true")

      disklessMigrationHandler match {
        case Some(handler) =>
          try {
            val results = handler.migrateTopicToDiskless(topic)
            val successCount = results.values.count(_.isInstanceOf[MigrationSuccess])
            val failedCount = results.values.count(_.isInstanceOf[MigrationFailed])

            if (failedCount > 0) {
              warn(s"Diskless migration for topic $topic had $failedCount failures. " +
                s"Partitions are sealed but control plane initialization may have failed for some. " +
                s"Results: $results")
            } else {
              info(s"Diskless migration for topic $topic completed successfully. " +
                s"$successCount partitions sealed and initialized.")
            }
          } catch {
            case e: Exception =>
              error(s"Failed to perform diskless migration for topic $topic", e)
              // Note: We don't rethrow here because:
              // 1. The config has already been updated in KRaft metadata
              // 2. The partitions may be partially sealed
              // 3. The migration can be retried by triggering another config update
              // The error will be logged and operators should monitor for this condition
          }

        case None =>
          warn(s"Diskless migration triggered for topic $topic but no DisklessMigrationHandler is configured. " +
            s"This may indicate a configuration issue - diskless storage system should be enabled when " +
            s"diskless topics are allowed.")
      }
    }
  }

  private[server] def maybeUpdateRemoteLogComponents(topic: String,
                                                     logs: Seq[UnifiedLog],
                                                     wasRemoteLogEnabled: Boolean,
                                                     wasCopyDisabled: Boolean): Unit = {
    val isRemoteLogEnabled = logs.exists(_.remoteLogEnabled())
    val isCopyDisabled = logs.exists(_.config.remoteLogCopyDisable())
    val isDeleteOnDisable = logs.exists(_.config.remoteLogDeleteOnDisable())

    val (leaderPartitions, followerPartitions) =
      logs.flatMap(log => replicaManager.onlinePartition(log.topicPartition)).partition(_.isLeader)

    // Topic configs gets updated incrementally. This check is added to prevent redundant updates.
    // When remote log is enabled, or remote copy is enabled, we should create RLM tasks accordingly via `onLeadershipChange`.
    if (isRemoteLogEnabled && (!wasRemoteLogEnabled || (wasCopyDisabled && !isCopyDisabled))) {
      val topicIds = Collections.singletonMap(topic, replicaManager.metadataCache.getTopicId(topic))
      replicaManager.remoteLogManager.foreach(rlm =>
        rlm.onLeadershipChange((leaderPartitions.toSet: Set[TopicPartitionLog]).asJava, (followerPartitions.toSet: Set[TopicPartitionLog]).asJava, topicIds))
    }

    // When copy disabled, we should stop leaderCopyRLMTask, but keep expirationTask
    if (isRemoteLogEnabled && !wasCopyDisabled && isCopyDisabled) {
      replicaManager.remoteLogManager.foreach(rlm => {
        rlm.stopLeaderCopyRLMTasks((leaderPartitions.toSet: Set[TopicPartitionLog] ).asJava)
      })
    }

    // Disabling remote log storage on this topic
    if (wasRemoteLogEnabled && !isRemoteLogEnabled && isDeleteOnDisable) {
      val stopPartitions: java.util.HashSet[StopPartition] = new java.util.HashSet[StopPartition]()
      leaderPartitions.foreach(partition => {
        // delete remote logs and stop RemoteLogMetadataManager
        stopPartitions.add(new StopPartition(partition.topicPartition, false, true, true))
      })

      followerPartitions.foreach(partition => {
        // we need to cancel follower tasks and stop RemoteLogMetadataManager
        stopPartitions.add(new StopPartition(partition.topicPartition, false, false, true))
      })

      // update the log start offset to local log start offset for the leader replicas
      logs.filter(log => leaderPartitions.exists(p => p.topicPartition.equals(log.topicPartition)))
        .foreach(log => log.maybeIncrementLogStartOffset(log.localLogStartOffset(), LogStartOffsetIncrementReason.SegmentDeletion))

      replicaManager.remoteLogManager.foreach(rlm => rlm.stopPartitions(stopPartitions, (_, _) => {}))
    }
  }

  def processConfigChanges(topic: String, topicConfig: Properties): Unit = {
    updateLogConfig(topic, topicConfig)

    def updateThrottledList(prop: String, quotaManager: ReplicationQuotaManager): Unit = {
      if (topicConfig.containsKey(prop) && topicConfig.getProperty(prop).nonEmpty) {
        val partitions = parseThrottledPartitions(topicConfig, kafkaConfig.brokerId, prop)
        quotaManager.markThrottled(topic, partitions.map(Integer.valueOf).asJava)
        debug(s"Setting $prop on broker ${kafkaConfig.brokerId} for topic: $topic and partitions $partitions")
      } else {
        quotaManager.removeThrottle(topic)
        debug(s"Removing $prop from broker ${kafkaConfig.brokerId} for topic $topic")
      }
    }
    updateThrottledList(QuotaConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG, quotas.leader)
    updateThrottledList(QuotaConfig.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG, quotas.follower)
  }

  def parseThrottledPartitions(topicConfig: Properties, brokerId: Int, prop: String): Seq[Int] = {
    val configValue = topicConfig.get(prop).toString.trim
    ThrottledReplicaListValidator.ensureValidString(prop, configValue)
    configValue match {
      case "" => Seq()
      case "*" => ReplicationQuotaManager.ALL_REPLICAS.asScala.map(_.toInt).toSeq
      case _ => configValue.trim
        .split(",")
        .map(_.split(":"))
        .filter(_ (1).toInt == brokerId) //Filter this replica
        .map(_ (0).toInt).toSeq //convert to list of partition ids
    }
  }
}

/**
  * The BrokerConfigHandler will process individual broker config changes in ZK.
  * The callback provides the brokerId and the full properties set read from ZK.
  * This implementation reports the overrides to the respective ReplicationQuotaManager objects
  */
class BrokerConfigHandler(private val brokerConfig: KafkaConfig,
                          private val quotaManagers: QuotaManagers) extends ConfigHandler with Logging {
  def processConfigChanges(brokerId: String, properties: Properties): Unit = {
    if (brokerId.isEmpty)
      brokerConfig.dynamicConfig.updateDefaultConfig(properties)
    else if (brokerConfig.brokerId == brokerId.trim.toInt) {
      brokerConfig.dynamicConfig.updateBrokerConfig(brokerConfig.brokerId, properties)
    }
    val updatedDynamicBrokerConfigs = brokerConfig.dynamicConfig.currentDynamicBrokerConfigs
    val updatedDynamicDefaultConfigs = brokerConfig.dynamicConfig.currentDynamicDefaultConfigs

    def getOrDefault(prop: String): Long = updatedDynamicBrokerConfigs get prop match {
      case Some(value) => value.toLong
      case None => updatedDynamicDefaultConfigs get prop match {
        case Some(defaultValue) => defaultValue.toLong
        case None => QuotaConfig.QUOTA_BYTES_PER_SECOND_DEFAULT
      }
    }
    quotaManagers.leader.updateQuota(upperBound(getOrDefault(QuotaConfig.LEADER_REPLICATION_THROTTLED_RATE_CONFIG).toDouble))
    quotaManagers.follower.updateQuota(upperBound(getOrDefault(QuotaConfig.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG).toDouble))
    quotaManagers.alterLogDirs.updateQuota(upperBound(getOrDefault(QuotaConfig.REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG).toDouble))
  }
}

/**
 * The ClientMetricsConfigHandler will process individual client metrics subscription changes.
 */
class ClientMetricsConfigHandler(private val clientMetricsManager: ClientMetricsManager) extends ConfigHandler with Logging {
  def processConfigChanges(subscriptionGroupId: String, properties: Properties): Unit = {
    clientMetricsManager.updateSubscription(subscriptionGroupId, properties)
  }
}

/**
 * The GroupConfigHandler will process individual group config changes.
 */
class GroupConfigHandler(private val groupCoordinator: GroupCoordinator) extends ConfigHandler with Logging {
  override def processConfigChanges(groupId: String, properties: Properties): Unit = {
    groupCoordinator.updateGroupConfig(groupId, properties)
  }
}
