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

package org.apache.kafka.controller

import kafka.server.{ControllerConfigurationValidator, KafkaConfig}
import kafka.utils.TestUtils
import org.apache.kafka.clients.admin.AlterConfigOp
import org.apache.kafka.clients.admin.AlterConfigOp.OpType.{DELETE, SET}
import org.apache.kafka.common.config.{ConfigDef, ConfigResource, TopicConfig}
import org.apache.kafka.common.metadata.ConfigRecord
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.ApiError
import org.apache.kafka.metadata.KafkaConfigSchema
import org.apache.kafka.server.config.{ServerConfigs, ServerTopicConfigSynonyms}
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig
import org.apache.kafka.storage.internals.log.LogConfig
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

import java.util
import java.util.AbstractMap.SimpleImmutableEntry

import scala.jdk.CollectionConverters._

class DisklessRemoteStorageDeleteConfigTest {
  private val resource = new ConfigResource(ConfigResource.Type.TOPIC, "diskless-topic")
  private val requiresRemoteStorageError =
    "Diskless topics must have remote storage enabled. " +
      "Cannot set remote.storage.enable=false when diskless is enabled."

  @Test
  def testIncrementalDeleteRemoteStorageRejected(): Unit = {
    val manager = configurationControlManager()
    replayConsolidatedDisklessTopic(manager)

    val deleteRemoteStorage: util.Map.Entry[AlterConfigOp.OpType, String] =
      new SimpleImmutableEntry(DELETE, null)
    val enableDeleteOnDisable: util.Map.Entry[AlterConfigOp.OpType, String] =
      new SimpleImmutableEntry(SET, "true")
    val changes = util.Map.of(
      TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG,
      deleteRemoteStorage,
      TopicConfig.REMOTE_LOG_DELETE_ON_DISABLE_CONFIG,
      enableDeleteOnDisable
    )
    val result = manager.incrementalAlterConfigs(util.Map.of(resource, changes), false)

    assertRejectedWithoutRecords(result)
    assertConsolidatedConfigsUnchanged(manager)
  }

  @Test
  def testLegacyOmissionOfRemoteStorageRejected(): Unit = {
    val manager = configurationControlManager()
    replayConsolidatedDisklessTopic(manager)

    val result = manager.legacyAlterConfigs(
      util.Map.of(resource, util.Map.of(
        TopicConfig.DISKLESS_ENABLE_CONFIG, "true",
        TopicConfig.REMOTE_LOG_DELETE_ON_DISABLE_CONFIG, "true"
      )),
      false
    )

    assertRejectedWithoutRecords(result)
    assertConsolidatedConfigsUnchanged(manager)
  }

  private def configurationControlManager(): ConfigurationControlManager = {
    val props = TestUtils.createDummyBrokerConfig()
    props.put(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, "true")
    props.put(ServerConfigs.DISKLESS_STORAGE_SYSTEM_ENABLE_CONFIG, "true")
    props.put(ServerConfigs.DISKLESS_MANAGED_REPLICAS_ENABLE_CONFIG, "true")
    props.put(ServerConfigs.DISKLESS_ALLOW_FROM_CLASSIC_ENABLE_CONFIG, "true")
    props.put(ServerConfigs.DISKLESS_REMOTE_STORAGE_CONSOLIDATION_ENABLE_CONFIG, "true")
    val kafkaConfig = KafkaConfig.fromProps(props)
    val configSchema = new KafkaConfigSchema(
      Map(
        ConfigResource.Type.BROKER -> new ConfigDef(KafkaConfig.configDef),
        ConfigResource.Type.TOPIC -> LogConfig.configDefCopy
      ).asJava,
      ServerTopicConfigSynonyms.ALL_TOPIC_CONFIG_SYNONYMS
    )
    new ConfigurationControlManager.Builder()
      .setKafkaConfigSchema(configSchema)
      .setValidator(new ControllerConfigurationValidator(kafkaConfig))
      .build()
  }

  private def replayConsolidatedDisklessTopic(manager: ConfigurationControlManager): Unit = {
    manager.replay(configRecord(TopicConfig.DISKLESS_ENABLE_CONFIG, "true"))
    manager.replay(configRecord(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true"))
  }

  private def configRecord(name: String, value: String): ConfigRecord = {
    new ConfigRecord()
      .setResourceType(ConfigResource.Type.TOPIC.id())
      .setResourceName(resource.name())
      .setName(name)
      .setValue(value)
  }

  private def assertRejectedWithoutRecords(
    result: ControllerResult[util.Map[ConfigResource, ApiError]]
  ): Unit = {
    val error = result.response().get(resource)
    assertEquals(Errors.INVALID_CONFIG, error.error())
    assertEquals(requiresRemoteStorageError, error.message())
    assertTrue(result.records().isEmpty)
  }

  private def assertConsolidatedConfigsUnchanged(manager: ConfigurationControlManager): Unit = {
    assertEquals(
      util.Map.of(
        TopicConfig.DISKLESS_ENABLE_CONFIG, "true",
        TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true"
      ),
      manager.getConfigs(resource)
    )
  }
}
