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

package kafka.server.metadata

import kafka.server.{ConfigHandler, KafkaConfig}
import kafka.utils.TestUtils
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.metadata.ConfigRecord
import org.apache.kafka.image.{MetadataDelta, MetadataImage, MetadataProvenance}
import org.apache.kafka.server.config.ConfigType
import org.apache.kafka.server.fault.MockFaultHandler
import org.junit.jupiter.api.Assertions.{assertFalse, assertTrue}
import org.junit.jupiter.api.Test

import java.util.Properties

/**
 * `initialPublishFuture` is what lets a caller such as `SharedServer.startInklessControlPlane()`
 * know when it is safe to read a persisted dynamic value out of `KafkaConfig`: only after the
 * corresponding `ConfigHandler` has run at least once, including when there was nothing to apply.
 */
class DynamicConfigPublisherTest {
  /** Mirrors only the default-resource branch of the real `BrokerConfigHandler`, to avoid needing
   * a `QuotaManagers` here: this test cares about `initialPublishFuture`, not quota updates. */
  private class DefaultResourceConfigHandler(config: KafkaConfig) extends ConfigHandler {
    override def processConfigChanges(resourceName: String, properties: Properties): Unit = {
      if (resourceName.isEmpty) config.dynamicConfig.updateDefaultConfig(properties)
    }
  }

  private def newPublisher(config: KafkaConfig, faultHandler: MockFaultHandler): DynamicConfigPublisher =
    new DynamicConfigPublisher(
      config,
      faultHandler,
      Map[ConfigType, ConfigHandler](ConfigType.BROKER -> new DefaultResourceConfigHandler(config)),
      "test")

  @Test
  def testInitialPublishFutureCompletesWithoutAnyConfigOverride(): Unit = {
    val config = new KafkaConfig(TestUtils.createBrokerConfig(0))
    config.dynamicConfig.initialize(None)
    val faultHandler = new MockFaultHandler("test")
    val publisher = newPublisher(config, faultHandler)
    assertFalse(publisher.initialPublishFuture.isDone, "must not complete before the first update")

    val delta = new MetadataDelta(MetadataImage.EMPTY)
    val image = delta.apply(MetadataProvenance.EMPTY)
    publisher.onMetadataUpdate(delta, image)

    assertTrue(publisher.initialPublishFuture.isDone,
      "the no-override case must still complete the future, or a waiting caller hangs forever")
    assertFalse(publisher.initialPublishFuture.isCompletedExceptionally)
    faultHandler.maybeRethrowFirstException()
  }

  @Test
  def testInitialPublishFutureCompletesWithAPersistedOverride(): Unit = {
    val config = new KafkaConfig(TestUtils.createBrokerConfig(0))
    config.dynamicConfig.initialize(None)
    val faultHandler = new MockFaultHandler("test")
    val publisher = newPublisher(config, faultHandler)

    val delta = new MetadataDelta(MetadataImage.EMPTY)
    delta.replay(new ConfigRecord()
      .setResourceType(ConfigResource.Type.BROKER.id())
      .setResourceName("")
      .setName("inkless.control.plane.connection.string")
      .setValue("jdbc:postgresql://persisted/db"))
    val image = delta.apply(MetadataProvenance.EMPTY)
    publisher.onMetadataUpdate(delta, image)

    assertTrue(publisher.initialPublishFuture.isDone)
    faultHandler.maybeRethrowFirstException()
    assertTrue(config.currentInklessConfig.controlPlaneConfig.get("connection.string") ==
      "jdbc:postgresql://persisted/db",
      "the persisted value must have reached KafkaConfig by the time the future completes")
  }

  @Test
  def testInitialPublishFutureCompletesExactlyOnceAcrossRepeatedUpdates(): Unit = {
    val config = new KafkaConfig(TestUtils.createBrokerConfig(0))
    config.dynamicConfig.initialize(None)
    val faultHandler = new MockFaultHandler("test")
    val publisher = newPublisher(config, faultHandler)

    val delta = new MetadataDelta(MetadataImage.EMPTY)
    val image = delta.apply(MetadataProvenance.EMPTY)
    publisher.onMetadataUpdate(delta, image)
    val completedAfterFirst = publisher.initialPublishFuture

    publisher.onMetadataUpdate(new MetadataDelta(image), image)

    assertTrue(completedAfterFirst.isDone)
    faultHandler.maybeRethrowFirstException()
  }
}
