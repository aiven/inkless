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

package kafka.integration

import io.aiven.inkless.test_utils.{PostgreSQLTestContainer, S3TestContainer}
import org.junit.jupiter.api.{BeforeEach, TestInfo}
import kafka.integration.InklessServerTestHarness.{minioContainer, pgContainer}
import kafka.server.KafkaConfig
import org.apache.kafka.common.network.ListenerName

import java.util.Properties
import scala.collection.Seq


abstract class InklessServerTestHarness extends KafkaServerTestHarness {

  def generateConfigs(props: Properties): Seq[KafkaConfig]

  override def generateConfigs: Seq[KafkaConfig] = generateConfigs(inklessProps())

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    pgContainer.createDatabase(testInfo)
    minioContainer.createBucket(testInfo)

    super.setUp(testInfo)
  }

  override def baseProps(): Properties = inklessProps()

  override def configs: Seq[KafkaConfig] = {
    if (instanceConfigs == null) {
      instanceConfigs = generateConfigs
    }
    instanceConfigs
  }

  private def inklessProps() = {
    val props = new java.util.Properties()
    // Control plane: Postgres
    props.put("inkless.control.plane.class", "io.aiven.inkless.control_plane.postgres.PostgresControlPlane")
    props.put("inkless.control.plane.connection.string", pgContainer.getUserJdbcUrl)
    props.put("inkless.control.plane.username", pgContainer.getUsername)
    props.put("inkless.control.plane.password", pgContainer.getPassword)
    // Storage: S3
    props.put("inkless.storage.backend.class", "io.aiven.inkless.storage_backend.s3.S3Storage")
    props.put("inkless.storage.s3.bucket.name", minioContainer.getBucketName)
    props.put("inkless.storage.s3.region", minioContainer.getRegion)
    props.put("inkless.storage.s3.endpoint.url", minioContainer.getEndpoint)
    props.put("inkless.storage.s3.path.style.access.enabled", "true")
    props.put("inkless.storage.aws.access.key.id", minioContainer.getAccessKey)
    props.put("inkless.storage.aws.secret.access.key", minioContainer.getSecretKey)

    props
  }

  override def createTopic(topic: String,
                           numPartitions: Int,
                           replicationFactor: Int,
                           topicConfig: Properties,
                           listenerName: ListenerName,
                           adminClientConfig: Properties): Map[Int, Int] = {
    if (!topicConfig.getOrDefault("cleanup.policy", "").equals("compact")) {
      topicConfig.put("inkless.enable", "true")
    }

    super.createTopic(topic, numPartitions, replicationFactor, topicConfig, listenerName, adminClientConfig)
  }
}

object InklessServerTestHarness {
  val pgContainer = {
    val container = PostgreSQLTestContainer.container()
    container.start()
    container
  }
  val minioContainer = {
    val container = S3TestContainer.minio()
    container.start()
    container
  }
}