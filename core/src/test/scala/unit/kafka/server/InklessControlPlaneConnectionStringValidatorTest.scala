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

import kafka.utils.TestUtils
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.ConfigResource.Type.BROKER
import org.apache.kafka.common.errors.InvalidConfigurationException
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}
import org.junit.jupiter.api.Test

import java.util
import java.util.Collections.emptyMap

/**
 * Exercises the embedded-credentials rejection through `ControllerConfigurationValidator`, the
 * entry point the controller actually calls on the `AlterConfigs`/`IncrementalAlterConfigs` path,
 * rather than calling `InklessControlPlaneConnectionStringValidator` directly.
 */
class InklessControlPlaneConnectionStringValidatorTest {
  val config = new KafkaConfig(TestUtils.createDummyBrokerConfig())
  val validator = new ControllerConfigurationValidator(config)

  @Test
  def testValidInklessControlPlaneConnectionStringConfig(): Unit = {
    val config = new util.TreeMap[String, String]()
    config.put("inkless.control.plane.connection.string", "jdbc:postgresql://host/db")
    validator.validate(new ConfigResource(BROKER, ""), config, emptyMap())
  }

  @Test
  def testInklessControlPlaneConnectionStringRejectsEmbeddedCredentials(): Unit = {
    val config = new util.TreeMap[String, String]()
    config.put("inkless.control.plane.connection.string", "jdbc:postgresql://host/db?user=admin&password=secret")
    assertEquals("inkless.control.plane.connection.string must not embed credentials in the " +
      "connection string; configure username/password separately",
      assertThrows(classOf[InvalidConfigurationException], () => validator.validate(
        new ConfigResource(BROKER, ""), config, emptyMap())).getMessage)
  }

  @Test
  def testInklessControlPlaneReadConnectionStringRejectsEmbeddedCredentials(): Unit = {
    val config = new util.TreeMap[String, String]()
    config.put("inkless.control.plane.read.connection.string", "jdbc:postgresql://host/db?password=secret")
    assertEquals("inkless.control.plane.read.connection.string must not embed credentials in the " +
      "connection string; configure username/password separately",
      assertThrows(classOf[InvalidConfigurationException], () => validator.validate(
        new ConfigResource(BROKER, ""), config, emptyMap())).getMessage)
  }
}
