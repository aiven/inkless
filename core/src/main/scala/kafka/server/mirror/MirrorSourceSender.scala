/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.server.mirror

import java.net.SocketTimeoutException
import kafka.server.BlockingSend
import org.apache.kafka.clients._
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network._
import org.apache.kafka.common.requests.AbstractRequest
import org.apache.kafka.common.security.JaasContext
import org.apache.kafka.common.utils.{LogContext, Time}
import org.apache.kafka.clients.{ApiVersions, ClientResponse, ManualMetadataUpdater, NetworkClient}
import org.apache.kafka.common.Node
import org.apache.kafka.common.requests.AbstractRequest.Builder
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.server.config.ClusterMirrorConfig
import org.apache.kafka.server.network.BrokerEndPoint
import kafka.server.KafkaConfig

import scala.jdk.CollectionConverters._

/**
 * BlockingSend implementation for cross-cluster mirroring. Creates a dedicated NetworkClient
 * configured with cluster-specific security settings (SASL/SSL) from ClusterMirrorConfig.
 */
class MirrorSourceSender(sourceBroker: BrokerEndPoint,
                         mirrorConfig: ClusterMirrorConfig,
                         brokerConfig: KafkaConfig,
                         metrics: Metrics,
                         time: Time,
                         fetcherId: Int,
                         clientId: String,
                         logContext: LogContext) extends BlockingSend {
  private val sourceNode = new Node(sourceBroker.id, sourceBroker.host, sourceBroker.port)
  private val socketTimeout: Int = brokerConfig.replicaSocketTimeoutMs

  private val networkClient = {
    val channelBuilder = ChannelBuilders.clientChannelBuilder(
      SecurityProtocol.forName(mirrorConfig.securityProtocol()),
      JaasContext.Type.CLIENT,
      mirrorConfig.getConfig(),
      null,
      mirrorConfig.saslMechanism(),
      time,
      logContext
    )
    val selector = new Selector(
      NetworkReceive.UNLIMITED,
      brokerConfig.connectionsMaxIdleMs,
      metrics,
      time,
      "mirror-" + clientId,
      Map("broker-id" -> sourceBroker.id.toString, "fetcher-id" -> fetcherId.toString).asJava,
      false,
      channelBuilder,
      logContext)
    new NetworkClient(
      selector,
      new ManualMetadataUpdater(),
      clientId,
      1,
      0,
      0,
      Selectable.USE_DEFAULT_BUFFER_SIZE,
      brokerConfig.replicaSocketReceiveBufferBytes,
      brokerConfig.requestTimeoutMs,
      brokerConfig.connectionSetupTimeoutMs,
      brokerConfig.connectionSetupTimeoutMaxMs,
      time,
      true,
      new ApiVersions,
      logContext,
      MetadataRecoveryStrategy.NONE
    )
  }

  override def brokerEndPoint(): BrokerEndPoint = sourceBroker

  override def sendRequest(requestBuilder: Builder[_ <: AbstractRequest]): ClientResponse = {
    try {
      if (!NetworkClientUtils.awaitReady(networkClient, sourceNode, time, socketTimeout))
        throw new SocketTimeoutException(s"Failed to connect within $socketTimeout ms")
      else {
        val clientRequest = networkClient.newClientRequest(sourceBroker.id.toString, requestBuilder,
          time.milliseconds(), true)
        NetworkClientUtils.sendAndReceive(networkClient, clientRequest, time)
      }
    }
    catch {
      case e: Throwable =>
        networkClient.close(sourceBroker.id.toString)
        throw e
    }
  }

  override def initiateClose(): Unit = {
    networkClient.initiateClose()
  }

  def close(): Unit = {
    networkClient.close()
  }

  override def toString: String = {
    s"MirrorBlockingSender(sourceBroker=$sourceBroker, fetcherId=$fetcherId)"
  }
}
