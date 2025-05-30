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
package kafka.api

import kafka.server.{KafkaBroker, KafkaConfig}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.junit.jupiter.api.{BeforeEach, TestInfo}

import java.util.Properties

abstract class RebootstrapTest extends AbstractConsumerTest {
  override def brokerCount: Int = 2

  def server0: KafkaBroker = serverForId(0).get
  def server1: KafkaBroker = serverForId(1).get

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.doSetup(testInfo, createOffsetsTopic = true)

    // Enable unclean leader election for the test topic
    val topicProps = new Properties
    topicProps.put(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "true")

    // create the test topic with all the brokers as replicas
    createTopic(topic, 2, brokerCount, adminClientConfig = this.adminClientConfig, topicConfig = topicProps)
  }

  override def generateConfigs: Seq[KafkaConfig] = {
    val overridingProps = new Properties()
    overridingProps.put(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, brokerCount.toString)

    // In this test, fixed ports are necessary, because brokers must have the
    // same port after the restart.
    FixedPortTestUtils.createBrokerConfigs(brokerCount, enableControlledShutdown = false, inklessMode = inklessMode)
      .map(KafkaConfig.fromProps(_, overridingProps))
  }

  def clientOverrides(useRebootstrapTriggerMs: Boolean): Properties = {
    val overrides = new Properties()
    if (useRebootstrapTriggerMs) {
      overrides.put(CommonClientConfigs.METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_CONFIG, "5000")
    } else {
      overrides.put(CommonClientConfigs.METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_CONFIG, "3600000")
      overrides.put(CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG, "5000")
      overrides.put(CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG, "5000")
      overrides.put(CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG, "1000")
      overrides.put(CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG, "1000")
    }
    overrides.put(CommonClientConfigs.METADATA_RECOVERY_STRATEGY_CONFIG, "rebootstrap")
    overrides
  }
}
