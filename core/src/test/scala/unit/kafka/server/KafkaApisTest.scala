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

package kafka.server

import io.aiven.inkless.common.SharedState
import io.aiven.inkless.control_plane.MetadataView
import kafka.cluster.Partition
import kafka.coordinator.transaction.{InitProducerIdResult, TransactionCoordinator}
import kafka.network.RequestChannel
import kafka.server.QuotaFactory.QuotaManagers
import kafka.server.metadata.KRaftMetadataCache
import kafka.server.share.SharePartitionManager
import kafka.utils.{CoreUtils, Logging, LoggingController, TestUtils}
import org.apache.kafka.clients.admin.AlterConfigOp.OpType
import org.apache.kafka.clients.admin.{AlterConfigOp, ConfigEntry}
import org.apache.kafka.common._
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.ConfigResource.Type.{BROKER, BROKER_LOGGER}
import org.apache.kafka.common.errors.{ClusterAuthorizationException, UnsupportedVersionException}
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData.{AddPartitionsToTxnTopic, AddPartitionsToTxnTopicCollection, AddPartitionsToTxnTransaction, AddPartitionsToTxnTransactionCollection}
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData.AddPartitionsToTxnResult
import org.apache.kafka.common.message.AlterConfigsRequestData.{AlterConfigsResource => LAlterConfigsResource, AlterConfigsResourceCollection => LAlterConfigsResourceCollection, AlterableConfig => LAlterableConfig, AlterableConfigCollection => LAlterableConfigCollection}
import org.apache.kafka.common.message.AlterConfigsResponseData.{AlterConfigsResourceResponse => LAlterConfigsResourceResponse}
import org.apache.kafka.common.message.ApiMessageType.ListenerType
import org.apache.kafka.common.message.ConsumerGroupDescribeResponseData.{DescribedGroup, TopicPartitions}
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult
import org.apache.kafka.common.message.DescribeShareGroupOffsetsRequestData.{DescribeShareGroupOffsetsRequestGroup, DescribeShareGroupOffsetsRequestTopic}
import org.apache.kafka.common.message.DescribeShareGroupOffsetsResponseData.{DescribeShareGroupOffsetsResponseGroup, DescribeShareGroupOffsetsResponsePartition, DescribeShareGroupOffsetsResponseTopic}
import org.apache.kafka.common.message.IncrementalAlterConfigsRequestData.{AlterConfigsResource => IAlterConfigsResource, AlterConfigsResourceCollection => IAlterConfigsResourceCollection, AlterableConfig => IAlterableConfig, AlterableConfigCollection => IAlterableConfigCollection}
import org.apache.kafka.common.message.IncrementalAlterConfigsResponseData.{AlterConfigsResourceResponse => IAlterConfigsResourceResponse}
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity
import org.apache.kafka.common.message.ListClientMetricsResourcesResponseData.ClientMetricsResource
import org.apache.kafka.common.message.ListOffsetsRequestData.{ListOffsetsPartition, ListOffsetsTopic}
import org.apache.kafka.common.message.ListOffsetsResponseData.{ListOffsetsPartitionResponse, ListOffsetsTopicResponse}
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic
import org.apache.kafka.common.message.OffsetDeleteRequestData.{OffsetDeleteRequestPartition, OffsetDeleteRequestTopic, OffsetDeleteRequestTopicCollection}
import org.apache.kafka.common.message.OffsetDeleteResponseData.{OffsetDeleteResponsePartition, OffsetDeleteResponsePartitionCollection, OffsetDeleteResponseTopic, OffsetDeleteResponseTopicCollection}
import org.apache.kafka.common.message.ShareFetchRequestData.{AcknowledgementBatch, ForgottenTopic}
import org.apache.kafka.common.message.ShareFetchResponseData.{AcquiredRecords, PartitionData, ShareFetchableTopicResponse}
import org.apache.kafka.common.metadata.{FeatureLevelRecord, PartitionRecord, RegisterBrokerRecord, TopicRecord}
import org.apache.kafka.common.metadata.RegisterBrokerRecord.{BrokerEndpoint, BrokerEndpointCollection}
import org.apache.kafka.common.protocol.ApiMessage
import org.apache.kafka.common.message._
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.{ClientInformation, ListenerName}
import org.apache.kafka.common.protocol.{ApiKeys, Errors, MessageUtil}
import org.apache.kafka.common.record._
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType
import org.apache.kafka.common.requests.MetadataResponse.TopicMetadata
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.requests.WriteTxnMarkersRequest.TxnMarkerEntry
import org.apache.kafka.common.requests.{FetchMetadata => JFetchMetadata, _}
import org.apache.kafka.common.resource.{PatternType, Resource, ResourcePattern, ResourceType}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, KafkaPrincipalSerde, SecurityProtocol}
import org.apache.kafka.common.utils.annotation.ApiKeyVersionsSource
import org.apache.kafka.common.utils.{ImplicitLinkedHashCollection, ProducerIdAndEpoch, SecurityUtils, Utils}
import org.apache.kafka.coordinator.group.GroupConfig.{CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG, CONSUMER_SESSION_TIMEOUT_MS_CONFIG, SHARE_AUTO_OFFSET_RESET_CONFIG, SHARE_HEARTBEAT_INTERVAL_MS_CONFIG, SHARE_RECORD_LOCK_DURATION_MS_CONFIG, SHARE_SESSION_TIMEOUT_MS_CONFIG}
import org.apache.kafka.coordinator.group.modern.share.ShareGroupConfig
import org.apache.kafka.coordinator.group.{GroupConfig, GroupCoordinator, GroupCoordinatorConfig}
import org.apache.kafka.coordinator.group.streams.StreamsGroupHeartbeatResult
import org.apache.kafka.coordinator.share.{ShareCoordinator, ShareCoordinatorTestConfig}
import org.apache.kafka.coordinator.transaction.TransactionLogConfig
import org.apache.kafka.image.{MetadataDelta, MetadataImage, MetadataProvenance}
import org.apache.kafka.metadata.{ConfigRepository, MockConfigRepository}
import org.apache.kafka.network.metrics.{RequestChannelMetrics, RequestMetrics}
import org.apache.kafka.raft.QuorumConfig
import org.apache.kafka.security.authorizer.AclEntry
import org.apache.kafka.server.{BrokerFeatures, ClientMetricsManager}
import org.apache.kafka.server.authorizer.{Action, AuthorizationResult, Authorizer}
import org.apache.kafka.server.common.{FeatureVersion, FinalizedFeatures, GroupVersion, KRaftVersion, MetadataVersion, RequestLocal, TransactionVersion}
import org.apache.kafka.server.config.{KRaftConfigs, ReplicationConfigs, ServerConfigs, ServerLogConfigs}
import org.apache.kafka.server.metrics.ClientMetricsTestUtils
import org.apache.kafka.server.share.{CachedSharePartition, ErroneousAndValidPartitionData}
import org.apache.kafka.server.quota.ThrottleCallback
import org.apache.kafka.server.share.acknowledge.ShareAcknowledgementBatch
import org.apache.kafka.server.share.context.{FinalContext, ShareSessionContext}
import org.apache.kafka.server.share.session.{ShareSession, ShareSessionKey}
import org.apache.kafka.server.storage.log.{FetchParams, FetchPartitionData}
import org.apache.kafka.server.util.{FutureUtils, MockTime}
import org.apache.kafka.storage.internals.log.{AppendOrigin, LogConfig, UnifiedLog}
import org.apache.kafka.storage.log.metrics.BrokerTopicStats
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{CsvSource, EnumSource, ValueSource}
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.mockito.{ArgumentCaptor, ArgumentMatchers, Mockito}

import java.lang.{Byte => JByte}
import java.net.InetAddress
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util
import java.util.Arrays.asList
import java.util.concurrent.{CompletableFuture, TimeUnit}
import java.util.{Collections, Comparator, Optional, OptionalInt, OptionalLong, Properties}
import scala.collection.{Map, Seq, mutable}
import scala.jdk.CollectionConverters._

class KafkaApisTest extends Logging {
  private val requestChannel: RequestChannel = mock(classOf[RequestChannel])
  private val requestChannelMetrics: RequestChannelMetrics = mock(classOf[RequestChannelMetrics])
  private val replicaManager: ReplicaManager = mock(classOf[ReplicaManager])
  private val groupCoordinator: GroupCoordinator = mock(classOf[GroupCoordinator])
  private val shareCoordinator: ShareCoordinator = mock(classOf[ShareCoordinator])
  private val txnCoordinator: TransactionCoordinator = mock(classOf[TransactionCoordinator])
  private val forwardingManager: ForwardingManager = mock(classOf[ForwardingManager])
  private val autoTopicCreationManager: AutoTopicCreationManager = mock(classOf[AutoTopicCreationManager])

  private val kafkaPrincipalSerde = new KafkaPrincipalSerde {
    override def serialize(principal: KafkaPrincipal): Array[Byte] = Utils.utf8(principal.toString)
    override def deserialize(bytes: Array[Byte]): KafkaPrincipal = SecurityUtils.parseKafkaPrincipal(Utils.utf8(bytes))
  }
  private val metrics = new Metrics()
  private val brokerId = 1
  private var metadataCache: MetadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
  private val clientQuotaManager: ClientQuotaManager = mock(classOf[ClientQuotaManager])
  private val clientRequestQuotaManager: ClientRequestQuotaManager = mock(classOf[ClientRequestQuotaManager])
  private val clientControllerQuotaManager: ControllerMutationQuotaManager = mock(classOf[ControllerMutationQuotaManager])
  private val replicaQuotaManager: ReplicationQuotaManager = mock(classOf[ReplicationQuotaManager])
  private val quotas = new QuotaManagers(clientQuotaManager, clientQuotaManager, clientRequestQuotaManager,
    clientControllerQuotaManager, replicaQuotaManager, replicaQuotaManager, replicaQuotaManager, util.Optional.empty())
  private val fetchManager: FetchManager = mock(classOf[FetchManager])
  private val sharePartitionManager: SharePartitionManager = mock(classOf[SharePartitionManager])
  private val clientMetricsManager: ClientMetricsManager = mock(classOf[ClientMetricsManager])
  private val brokerTopicStats = new BrokerTopicStats
  private val clusterId = "clusterId"
  private val time = new MockTime
  private val clientId = ""
  private var kafkaApis: KafkaApis = _

  @AfterEach
  def tearDown(): Unit = {
    CoreUtils.swallow(quotas.shutdown(), this)
    if (kafkaApis != null)
      CoreUtils.swallow(kafkaApis.close(), this)
    TestUtils.clearYammerMetrics()
    metrics.close()
  }

  def createKafkaApis(
    authorizer: Option[Authorizer] = None,
    configRepository: ConfigRepository = new MockConfigRepository(),
    overrideProperties: Map[String, String] = Map.empty,
    featureVersions: Seq[FeatureVersion] = Seq.empty,
    inklessSharedState: Option[SharedState] = None
  ): KafkaApis = {

    val properties = TestUtils.createBrokerConfig(brokerId)
    properties.put(KRaftConfigs.NODE_ID_CONFIG, brokerId.toString)
    properties.put(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker")
    val voterId = brokerId + 1
    properties.put(QuorumConfig.QUORUM_VOTERS_CONFIG, s"$voterId@localhost:9093")

    overrideProperties.foreach( p => properties.put(p._1, p._2))
    val config = new KafkaConfig(properties)

    val listenerType = ListenerType.BROKER
    val enabledApis = ApiKeys.apisForListener(listenerType).asScala

    val apiVersionManager = new SimpleApiVersionManager(
      listenerType,
      enabledApis,
      BrokerFeatures.defaultSupportedFeatures(true),
      true,
      () => new FinalizedFeatures(MetadataVersion.latestTesting(), Collections.emptyMap[String, java.lang.Short], 0))

    when(groupCoordinator.isNewGroupCoordinator).thenReturn(config.isNewGroupCoordinatorEnabled)
    setupFeatures(featureVersions)

    new KafkaApis(
      requestChannel = requestChannel,
      forwardingManager = forwardingManager,
      replicaManager = replicaManager,
      groupCoordinator = groupCoordinator,
      txnCoordinator = txnCoordinator,
      shareCoordinator = Some(shareCoordinator),
      autoTopicCreationManager = autoTopicCreationManager,
      brokerId = brokerId,
      config = config,
      configRepository = configRepository,
      metadataCache = metadataCache,
      metrics = metrics,
      authorizer = authorizer,
      quotas = quotas,
      fetchManager = fetchManager,
      sharePartitionManager = sharePartitionManager,
      brokerTopicStats = brokerTopicStats,
      clusterId = clusterId,
      time = time,
      tokenManager = null,
      apiVersionManager = apiVersionManager,
      clientMetricsManager = clientMetricsManager,
      inklessSharedState = inklessSharedState)
  }

  private def setupFeatures(featureVersions: Seq[FeatureVersion]): Unit = {
    if (featureVersions.isEmpty) return

    when(metadataCache.features()).thenReturn {
      new FinalizedFeatures(
        MetadataVersion.latestTesting,
        featureVersions.map { featureVersion =>
          featureVersion.featureName -> featureVersion.featureLevel.asInstanceOf[java.lang.Short]
        }.toMap.asJava,
        0)
    }
  }

  @Test
  def testDescribeConfigsWithAuthorizer(): Unit = {
    val authorizer: Authorizer = mock(classOf[Authorizer])

    val operation = AclOperation.DESCRIBE_CONFIGS
    val resourceType = ResourceType.TOPIC
    val resourceName = "topic-1"
    val requestHeader = new RequestHeader(ApiKeys.DESCRIBE_CONFIGS, ApiKeys.DESCRIBE_CONFIGS.latestVersion,
      clientId, 0)

    val expectedActions = Seq(
      new Action(operation, new ResourcePattern(resourceType, resourceName, PatternType.LITERAL),
        1, true, true)
    )

    // Verify that authorize is only called once
    when(authorizer.authorize(any[RequestContext], ArgumentMatchers.eq(expectedActions.asJava)))
      .thenReturn(Seq(AuthorizationResult.ALLOWED).asJava)

    val configRepository: ConfigRepository = mock(classOf[ConfigRepository])
    val topicConfigs = new Properties()
    val propName = "min.insync.replicas"
    val propValue = "3"
    topicConfigs.put(propName, propValue)
    when(configRepository.topicConfig(resourceName)).thenReturn(topicConfigs)

    metadataCache = mock(classOf[KRaftMetadataCache])
    when(metadataCache.contains(resourceName)).thenReturn(true)

    val describeConfigsRequest = new DescribeConfigsRequest.Builder(new DescribeConfigsRequestData()
      .setIncludeSynonyms(true)
      .setResources(List(new DescribeConfigsRequestData.DescribeConfigsResource()
        .setResourceName(resourceName)
        .setResourceType(ConfigResource.Type.TOPIC.id)).asJava))
      .build(requestHeader.apiVersion)
    val request = buildRequest(describeConfigsRequest, requestHeader = Option(requestHeader))

    kafkaApis = createKafkaApis(authorizer = Some(authorizer), configRepository = configRepository)
    kafkaApis.handleDescribeConfigsRequest(request)

    verify(authorizer).authorize(any(), ArgumentMatchers.eq(expectedActions.asJava))
    val response = verifyNoThrottling[DescribeConfigsResponse](request)
    val results = response.data.results
    assertEquals(1, results.size)
    val describeConfigsResult = results.get(0)
    assertEquals(ConfigResource.Type.TOPIC.id, describeConfigsResult.resourceType)
    assertEquals(resourceName, describeConfigsResult.resourceName)
    val configs = describeConfigsResult.configs.asScala.filter(_.name == propName)
    assertEquals(1, configs.length)
    val describeConfigsResponseData = configs.head
    assertEquals(propName, describeConfigsResponseData.name)
    assertEquals(propValue, describeConfigsResponseData.value)
  }

  @Test
  def testElectLeadersForwarding(): Unit = {
    val requestBuilder = new ElectLeadersRequest.Builder(ElectionType.PREFERRED, null, 30000)
    testKraftForwarding(ApiKeys.ELECT_LEADERS, requestBuilder)
  }

  @Test
  def testIncrementalConsumerGroupAlterConfigs(): Unit = {
    val authorizer: Authorizer = mock(classOf[Authorizer])

    val consumerGroupId = "consumer_group_1"
    val resource = new ConfigResource(ConfigResource.Type.GROUP, consumerGroupId)

    authorizeResource(authorizer, AclOperation.ALTER_CONFIGS, ResourceType.GROUP,
      consumerGroupId, AuthorizationResult.ALLOWED)

    val requestHeader = new RequestHeader(ApiKeys.INCREMENTAL_ALTER_CONFIGS,
      ApiKeys.INCREMENTAL_ALTER_CONFIGS.latestVersion, clientId, 0)

    val incrementalAlterConfigsRequest = getIncrementalAlterConfigRequestBuilder(
      Seq(resource), "consumer.session.timeout.ms", "45000").build(requestHeader.apiVersion)
    val request = buildRequest(incrementalAlterConfigsRequest, requestHeader = Option(requestHeader))

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    createKafkaApis(authorizer = Some(authorizer)).handleIncrementalAlterConfigsRequest(request)
    verify(forwardingManager, times(1)).forwardRequest(
      any(),
      any(),
      any()
    )
  }

  @Test
  def testDescribeConfigsConsumerGroup(): Unit = {
    val authorizer: Authorizer = mock(classOf[Authorizer])
    val operation = AclOperation.DESCRIBE_CONFIGS
    val resourceType = ResourceType.GROUP
    val consumerGroupId = "consumer_group_1"
    val requestHeader =
      new RequestHeader(ApiKeys.DESCRIBE_CONFIGS, ApiKeys.DESCRIBE_CONFIGS.latestVersion, clientId, 0)
    val expectedActions = Seq(
      new Action(operation, new ResourcePattern(resourceType, consumerGroupId, PatternType.LITERAL),
        1, true, true)
    )

    when(authorizer.authorize(any[RequestContext], ArgumentMatchers.eq(expectedActions.asJava)))
      .thenReturn(Seq(AuthorizationResult.ALLOWED).asJava)

    val configRepository: ConfigRepository = mock(classOf[ConfigRepository])
    val cgConfigs = new Properties()
    cgConfigs.put(CONSUMER_SESSION_TIMEOUT_MS_CONFIG, GroupCoordinatorConfig.CONSUMER_GROUP_SESSION_TIMEOUT_MS_DEFAULT.toString)
    cgConfigs.put(CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG, GroupCoordinatorConfig.CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_DEFAULT.toString)
    cgConfigs.put(SHARE_SESSION_TIMEOUT_MS_CONFIG, GroupCoordinatorConfig.SHARE_GROUP_SESSION_TIMEOUT_MS_DEFAULT.toString)
    cgConfigs.put(SHARE_HEARTBEAT_INTERVAL_MS_CONFIG, GroupCoordinatorConfig.SHARE_GROUP_HEARTBEAT_INTERVAL_MS_DEFAULT.toString)
    cgConfigs.put(SHARE_RECORD_LOCK_DURATION_MS_CONFIG, ShareGroupConfig.SHARE_GROUP_RECORD_LOCK_DURATION_MS_DEFAULT.toString)
    cgConfigs.put(SHARE_AUTO_OFFSET_RESET_CONFIG, GroupConfig.SHARE_AUTO_OFFSET_RESET_DEFAULT)
    when(configRepository.groupConfig(consumerGroupId)).thenReturn(cgConfigs)

    val describeConfigsRequest = new DescribeConfigsRequest.Builder(new DescribeConfigsRequestData()
      .setIncludeSynonyms(true)
      .setResources(List(new DescribeConfigsRequestData.DescribeConfigsResource()
        .setResourceName(consumerGroupId)
        .setResourceType(ConfigResource.Type.GROUP.id)).asJava))
      .build(requestHeader.apiVersion)
    val request = buildRequest(describeConfigsRequest,
      requestHeader = Option(requestHeader))
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    createKafkaApis(authorizer = Some(authorizer), configRepository = configRepository)
      .handleDescribeConfigsRequest(request)

    val response = verifyNoThrottling[DescribeConfigsResponse](request)
    // Verify that authorize is only called once
    verify(authorizer, times(1)).authorize(any(), any())
    val results = response.data.results
    assertEquals(1, results.size)
    val describeConfigsResult = results.get(0)

    assertEquals(ConfigResource.Type.GROUP.id, describeConfigsResult.resourceType)
    assertEquals(consumerGroupId, describeConfigsResult.resourceName)
    val configs = describeConfigsResult.configs
    assertEquals(cgConfigs.size, configs.size)
  }

  @Test
  def testAlterConfigsClientMetrics(): Unit = {
    val subscriptionName = "client_metric_subscription_1"
    val authorizedResource = new ConfigResource(ConfigResource.Type.CLIENT_METRICS, subscriptionName)

    val props = ClientMetricsTestUtils.defaultProperties
    val configEntries = new util.ArrayList[AlterConfigsRequest.ConfigEntry]()
    props.forEach((x, y) =>
      configEntries.add(new AlterConfigsRequest.ConfigEntry(x.asInstanceOf[String], y.asInstanceOf[String])))

    val configs = Map(authorizedResource -> new AlterConfigsRequest.Config(configEntries))

    val requestHeader = new RequestHeader(ApiKeys.ALTER_CONFIGS, ApiKeys.ALTER_CONFIGS.latestVersion, clientId, 0)
    val apiRequest = new AlterConfigsRequest.Builder(configs.asJava, false).build(requestHeader.apiVersion)
    val request = buildRequest(apiRequest)

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    kafkaApis = createKafkaApis()
    kafkaApis.handleAlterConfigsRequest(request)
    verify(forwardingManager, times(1)).forwardRequest(
      any(),
      any(),
      any()
    )
  }

  @Test
  def testIncrementalClientMetricAlterConfigs(): Unit = {
    val subscriptionName = "client_metric_subscription_1"
    val resource = new ConfigResource(ConfigResource.Type.CLIENT_METRICS, subscriptionName)

    val requestHeader = new RequestHeader(ApiKeys.INCREMENTAL_ALTER_CONFIGS,
      ApiKeys.INCREMENTAL_ALTER_CONFIGS.latestVersion, clientId, 0)

    val incrementalAlterConfigsRequest = getIncrementalAlterConfigRequestBuilder(
      Seq(resource), "metrics", "foo.bar").build(requestHeader.apiVersion)
    val request = buildRequest(incrementalAlterConfigsRequest, requestHeader = Option(requestHeader))

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    kafkaApis = createKafkaApis()
    kafkaApis.handleIncrementalAlterConfigsRequest(request)
    verify(forwardingManager, times(1)).forwardRequest(
      any(),
      any(),
      any()
    )
  }

  private def getIncrementalAlterConfigRequestBuilder(configResources: Seq[ConfigResource],
                                                      configName: String,
                                                      configValue: String): IncrementalAlterConfigsRequest.Builder = {
    val resourceMap = configResources.map(configResource => {
      val entryToBeModified = new ConfigEntry(configName, configValue)
      configResource -> Set(new AlterConfigOp(entryToBeModified, OpType.SET)).asJavaCollection
    }).toMap.asJava
    new IncrementalAlterConfigsRequest.Builder(resourceMap, false)
  }

  @Test
  def testDescribeConfigsClientMetrics(): Unit = {
    val authorizer: Authorizer = mock(classOf[Authorizer])
    val operation = AclOperation.DESCRIBE_CONFIGS
    val resourceType = ResourceType.CLUSTER
    val subscriptionName = "client_metric_subscription_1"
    val requestHeader =
      new RequestHeader(ApiKeys.DESCRIBE_CONFIGS, ApiKeys.DESCRIBE_CONFIGS.latestVersion, clientId, 0)
    val expectedActions = Seq(
      new Action(operation, new ResourcePattern(resourceType, Resource.CLUSTER_NAME, PatternType.LITERAL),
        1, true, true)
    )

    when(authorizer.authorize(any[RequestContext], ArgumentMatchers.eq(expectedActions.asJava)))
      .thenReturn(Seq(AuthorizationResult.ALLOWED).asJava)

    val resource = new ConfigResource(ConfigResource.Type.CLIENT_METRICS, subscriptionName)
    val configRepository: ConfigRepository = mock(classOf[ConfigRepository])
    val cmConfigs = ClientMetricsTestUtils.defaultProperties
    when(configRepository.config(resource)).thenReturn(cmConfigs)

    metadataCache = mock(classOf[KRaftMetadataCache])
    when(metadataCache.contains(subscriptionName)).thenReturn(true)

    val describeConfigsRequest = new DescribeConfigsRequest.Builder(new DescribeConfigsRequestData()
      .setIncludeSynonyms(true)
      .setResources(List(new DescribeConfigsRequestData.DescribeConfigsResource()
        .setResourceName(subscriptionName)
        .setResourceType(ConfigResource.Type.CLIENT_METRICS.id)).asJava))
      .build(requestHeader.apiVersion)
    val request = buildRequest(describeConfigsRequest,
      requestHeader = Option(requestHeader))

    kafkaApis = createKafkaApis(authorizer = Some(authorizer), configRepository = configRepository)
    kafkaApis.handleDescribeConfigsRequest(request)

    val response = verifyNoThrottling[DescribeConfigsResponse](request)
    // Verify that authorize is only called once
    verify(authorizer, times(1)).authorize(any(), any())
    val results = response.data.results
    assertEquals(1, results.size)
    val describeConfigsResult = results.get(0)

    assertEquals(ConfigResource.Type.CLIENT_METRICS.id, describeConfigsResult.resourceType)
    assertEquals(subscriptionName, describeConfigsResult.resourceName)
    val configs = describeConfigsResult.configs
    assertEquals(cmConfigs.size, configs.size)
  }

  @Test
  def testDescribeQuorumForwardedForKRaftClusters(): Unit = {
    val requestData = DescribeQuorumRequest.singletonRequest(KafkaRaftServer.MetadataPartition)
    val requestBuilder = new DescribeQuorumRequest.Builder(requestData)
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    kafkaApis = createKafkaApis()
    testForwardableApi(kafkaApis = kafkaApis,
      ApiKeys.DESCRIBE_QUORUM,
      requestBuilder
    )
  }

  private def testKraftForwarding(
    apiKey: ApiKeys,
    requestBuilder: AbstractRequest.Builder[_ <: AbstractRequest]
  ): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    kafkaApis = createKafkaApis()
    testForwardableApi(kafkaApis = kafkaApis,
      apiKey,
      requestBuilder
    )
  }

  private def testForwardableApi(
    kafkaApis: KafkaApis,
    apiKey: ApiKeys,
    requestBuilder: AbstractRequest.Builder[_ <: AbstractRequest]
  ): Unit = {
    val topicHeader = new RequestHeader(apiKey, apiKey.latestVersion,
      clientId, 0)

    val apiRequest = requestBuilder.build(topicHeader.apiVersion)
    val request = buildRequest(apiRequest)

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    val forwardCallback: ArgumentCaptor[Option[AbstractResponse] => Unit] = ArgumentCaptor.forClass(classOf[Option[AbstractResponse] => Unit])

    kafkaApis.handle(request, RequestLocal.withThreadConfinedCaching)
    verify(forwardingManager).forwardRequest(
      ArgumentMatchers.eq(request),
      forwardCallback.capture()
    )
    assertNotNull(request.buffer, "The buffer was unexpectedly deallocated after " +
      s"`handle` returned (is $apiKey marked as forwardable in `ApiKeys`?)")

    val expectedResponse = apiRequest.getErrorResponse(Errors.NOT_CONTROLLER.exception)
    forwardCallback.getValue.apply(Some(expectedResponse))

    val capturedResponse = verifyNoThrottling[AbstractResponse](request)
    assertEquals(expectedResponse.data, capturedResponse.data)
  }

  private def authorizeResource(authorizer: Authorizer,
                                operation: AclOperation,
                                resourceType: ResourceType,
                                resourceName: String,
                                result: AuthorizationResult,
                                logIfAllowed: Boolean = true,
                                logIfDenied: Boolean = true): Unit = {
    val expectedAuthorizedAction = if (operation == AclOperation.CLUSTER_ACTION)
      new Action(operation,
        new ResourcePattern(ResourceType.CLUSTER, Resource.CLUSTER_NAME, PatternType.LITERAL),
        1, logIfAllowed, logIfDenied)
    else
      new Action(operation,
        new ResourcePattern(resourceType, resourceName, PatternType.LITERAL),
        1, logIfAllowed, logIfDenied)

    when(authorizer.authorize(any[RequestContext], ArgumentMatchers.eq(Seq(expectedAuthorizedAction).asJava)))
      .thenReturn(Seq(result).asJava)
  }

  @Test
  def testIncrementalAlterConfigsWithAuthorizer(): Unit = {
    val authorizer: Authorizer = mock(classOf[Authorizer])

    val localResource = new ConfigResource(ConfigResource.Type.BROKER_LOGGER, "localResource")
    val forwardedResource = new ConfigResource(ConfigResource.Type.GROUP, "forwardedResource")

    val requestHeader = new RequestHeader(ApiKeys.INCREMENTAL_ALTER_CONFIGS, ApiKeys.INCREMENTAL_ALTER_CONFIGS.latestVersion, clientId, 0)

    val incrementalAlterConfigsRequest = getIncrementalAlterConfigRequestBuilder(Seq(localResource, forwardedResource))
      .build(requestHeader.apiVersion)
    val request = buildRequest(incrementalAlterConfigsRequest, requestHeader = Option(requestHeader))

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.LATEST_PRODUCTION)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleIncrementalAlterConfigsRequest(request)

    verify(authorizer, times(1)).authorize(any(), any())
    verify(forwardingManager, times(1)).forwardRequest(
      any(),
      any(),
      any()
    )
  }

  private def getIncrementalAlterConfigRequestBuilder(configResources: Seq[ConfigResource]): IncrementalAlterConfigsRequest.Builder = {
    val resourceMap = configResources.map(configResource => {
      configResource -> Set(
        new AlterConfigOp(new ConfigEntry("foo", "bar"),
        OpType.SET)).asJavaCollection
    }).toMap.asJava

    new IncrementalAlterConfigsRequest.Builder(resourceMap, false)
  }

  @ParameterizedTest
  @CsvSource(value = Array("0,1500", "1500,0", "3000,1000"))
  def testKRaftControllerThrottleTimeEnforced(
    controllerThrottleTimeMs: Int,
    requestThrottleTimeMs: Int
  ): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)

    val topicToCreate = new CreatableTopic()
      .setName("topic")
      .setNumPartitions(1)
      .setReplicationFactor(1.toShort)

    val requestData = new CreateTopicsRequestData()
    requestData.topics().add(topicToCreate)

    val requestBuilder = new CreateTopicsRequest.Builder(requestData).build()
    val request = buildRequest(requestBuilder)

    kafkaApis = createKafkaApis()
    val forwardCallback: ArgumentCaptor[Option[AbstractResponse] => Unit] =
      ArgumentCaptor.forClass(classOf[Option[AbstractResponse] => Unit])

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(request, time.milliseconds()))
      .thenReturn(requestThrottleTimeMs)

    kafkaApis.handle(request, RequestLocal.withThreadConfinedCaching)

    verify(forwardingManager).forwardRequest(
      ArgumentMatchers.eq(request),
      forwardCallback.capture()
    )

    val responseData = new CreateTopicsResponseData()
      .setThrottleTimeMs(controllerThrottleTimeMs)
    responseData.topics().add(new CreatableTopicResult()
      .setErrorCode(Errors.THROTTLING_QUOTA_EXCEEDED.code))

    forwardCallback.getValue.apply(Some(new CreateTopicsResponse(responseData)))

    val expectedThrottleTimeMs = math.max(controllerThrottleTimeMs, requestThrottleTimeMs)

    verify(clientRequestQuotaManager).throttle(
      ArgumentMatchers.eq(request),
      any[ThrottleCallback](),
      ArgumentMatchers.eq(expectedThrottleTimeMs)
    )

    assertEquals(expectedThrottleTimeMs, responseData.throttleTimeMs)
  }

  @Test
  def testFindCoordinatorAutoTopicCreationForOffsetTopic(): Unit = {
    testFindCoordinatorWithTopicCreation(CoordinatorType.GROUP)
  }

  @Test
  def testFindCoordinatorAutoTopicCreationForTxnTopic(): Unit = {
    testFindCoordinatorWithTopicCreation(CoordinatorType.TRANSACTION)
  }

  @Test
  def testFindCoordinatorNotEnoughBrokersForOffsetTopic(): Unit = {
    testFindCoordinatorWithTopicCreation(CoordinatorType.GROUP, hasEnoughLiveBrokers = false)
  }

  @Test
  def testFindCoordinatorNotEnoughBrokersForTxnTopic(): Unit = {
    testFindCoordinatorWithTopicCreation(CoordinatorType.TRANSACTION, hasEnoughLiveBrokers = false)
  }

  @Test
  def testOldFindCoordinatorAutoTopicCreationForOffsetTopic(): Unit = {
    testFindCoordinatorWithTopicCreation(CoordinatorType.GROUP, version = 3)
  }

  @Test
  def testOldFindCoordinatorAutoTopicCreationForTxnTopic(): Unit = {
    testFindCoordinatorWithTopicCreation(CoordinatorType.TRANSACTION, version = 3)
  }

  @Test
  def testOldFindCoordinatorNotEnoughBrokersForOffsetTopic(): Unit = {
    testFindCoordinatorWithTopicCreation(CoordinatorType.GROUP, hasEnoughLiveBrokers = false, version = 3)
  }

  @Test
  def testOldFindCoordinatorNotEnoughBrokersForTxnTopic(): Unit = {
    testFindCoordinatorWithTopicCreation(CoordinatorType.TRANSACTION, hasEnoughLiveBrokers = false, version = 3)
  }

  @Test
  def testFindCoordinatorTooOldForShareStateTopic(): Unit = {
    testFindCoordinatorWithTopicCreation(CoordinatorType.SHARE, checkAutoCreateTopic = false, version = 5)
  }

  @Test
  def testFindCoordinatorNoShareCoordinatorForShareStateTopic(): Unit = {
    testFindCoordinatorWithTopicCreation(CoordinatorType.SHARE, checkAutoCreateTopic = false)
  }

  private def testFindCoordinatorWithTopicCreation(coordinatorType: CoordinatorType,
                                                   hasEnoughLiveBrokers: Boolean = true,
                                                   checkAutoCreateTopic: Boolean = true,
                                                   version: Short = ApiKeys.FIND_COORDINATOR.latestVersion): Unit = {
    val authorizer: Authorizer = mock(classOf[Authorizer])

    val requestHeader = new RequestHeader(ApiKeys.FIND_COORDINATOR, version, clientId, 0)

    val numBrokersNeeded = 3

    setupBrokerMetadata(hasEnoughLiveBrokers, numBrokersNeeded)

    val requestTimeout = 10
    val topicConfigOverride = mutable.Map.empty[String, String]
    topicConfigOverride.put(ServerConfigs.REQUEST_TIMEOUT_MS_CONFIG, requestTimeout.toString)

    val groupId = "group"
    val topicId = Uuid.randomUuid
    val partition = 0
    var key:String = groupId

    val topicName =
      coordinatorType match {
        case CoordinatorType.GROUP =>
          topicConfigOverride.put(GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, numBrokersNeeded.toString)
          topicConfigOverride.put(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, numBrokersNeeded.toString)
          when(groupCoordinator.groupMetadataTopicConfigs).thenReturn(new Properties)
          authorizeResource(authorizer, AclOperation.DESCRIBE, ResourceType.GROUP,
            groupId, AuthorizationResult.ALLOWED)
          Topic.GROUP_METADATA_TOPIC_NAME
        case CoordinatorType.TRANSACTION =>
          topicConfigOverride.put(TransactionLogConfig.TRANSACTIONS_TOPIC_PARTITIONS_CONFIG, numBrokersNeeded.toString)
          topicConfigOverride.put(TransactionLogConfig.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG, numBrokersNeeded.toString)
          when(txnCoordinator.transactionTopicConfigs).thenReturn(new Properties)
          authorizeResource(authorizer, AclOperation.DESCRIBE, ResourceType.TRANSACTIONAL_ID,
            groupId, AuthorizationResult.ALLOWED)
          Topic.TRANSACTION_STATE_TOPIC_NAME
        case CoordinatorType.SHARE =>
          authorizeResource(authorizer, AclOperation.CLUSTER_ACTION, ResourceType.CLUSTER,
            Resource.CLUSTER_NAME, AuthorizationResult.ALLOWED)
          key = "%s:%s:%d" format(groupId, topicId, partition)
          Topic.SHARE_GROUP_STATE_TOPIC_NAME
        case _ =>
          throw new IllegalStateException(s"Unknown coordinator type $coordinatorType")
      }

    val findCoordinatorRequestBuilder = if (version >= 4) {
      new FindCoordinatorRequest.Builder(
        new FindCoordinatorRequestData()
          .setKeyType(coordinatorType.id())
          .setCoordinatorKeys(asList(key)))
    } else {
      new FindCoordinatorRequest.Builder(
        new FindCoordinatorRequestData()
          .setKeyType(coordinatorType.id())
          .setKey(key))
    }
    val request = buildRequest(findCoordinatorRequestBuilder.build(requestHeader.apiVersion))
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    val capturedRequest = verifyTopicCreation(topicName, true, true, request)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer),
      overrideProperties = topicConfigOverride)
    kafkaApis.handleFindCoordinatorRequest(request)

    val response = verifyNoThrottling[FindCoordinatorResponse](request)
    if (coordinatorType == CoordinatorType.SHARE && version < 6) {
      assertEquals(Errors.INVALID_REQUEST.code, response.data.coordinators.get(0).errorCode)
    } else if (version >= 4) {
      assertEquals(Errors.COORDINATOR_NOT_AVAILABLE.code, response.data.coordinators.get(0).errorCode)
      assertEquals(key, response.data.coordinators.get(0).key)
    } else {
      assertEquals(Errors.COORDINATOR_NOT_AVAILABLE.code, response.data.errorCode)
      assertTrue(capturedRequest.getValue.isEmpty)
    }
    if (checkAutoCreateTopic) {
      assertTrue(capturedRequest.getValue.isEmpty)
    }
  }

  @Test
  def testMetadataAutoTopicCreationForOffsetTopic(): Unit = {
    testMetadataAutoTopicCreation(Topic.GROUP_METADATA_TOPIC_NAME, enableAutoTopicCreation = true,
      expectedError = Errors.UNKNOWN_TOPIC_OR_PARTITION)
  }

  @Test
  def testMetadataAutoTopicCreationForTxnTopic(): Unit = {
    testMetadataAutoTopicCreation(Topic.TRANSACTION_STATE_TOPIC_NAME, enableAutoTopicCreation = true,
      expectedError = Errors.UNKNOWN_TOPIC_OR_PARTITION)
  }

  @Test
  def testMetadataAutoTopicCreationForNonInternalTopic(): Unit = {
    testMetadataAutoTopicCreation("topic", enableAutoTopicCreation = true,
      expectedError = Errors.UNKNOWN_TOPIC_OR_PARTITION)
  }

  @Test
  def testMetadataAutoTopicCreationDisabledForOffsetTopic(): Unit = {
    testMetadataAutoTopicCreation(Topic.GROUP_METADATA_TOPIC_NAME, enableAutoTopicCreation = false,
      expectedError = Errors.UNKNOWN_TOPIC_OR_PARTITION)
  }

  @Test
  def testMetadataAutoTopicCreationDisabledForTxnTopic(): Unit = {
    testMetadataAutoTopicCreation(Topic.TRANSACTION_STATE_TOPIC_NAME, enableAutoTopicCreation = false,
      expectedError = Errors.UNKNOWN_TOPIC_OR_PARTITION)
  }

  @Test
  def testMetadataAutoTopicCreationDisabledForNonInternalTopic(): Unit = {
    testMetadataAutoTopicCreation("topic", enableAutoTopicCreation = false,
      expectedError = Errors.UNKNOWN_TOPIC_OR_PARTITION)
  }

  @Test
  def testMetadataAutoCreationDisabledForNonInternal(): Unit = {
    testMetadataAutoTopicCreation("topic", enableAutoTopicCreation = true,
      expectedError = Errors.UNKNOWN_TOPIC_OR_PARTITION)
  }

  private def testMetadataAutoTopicCreation(topicName: String,
                                            enableAutoTopicCreation: Boolean,
                                            expectedError: Errors): Unit = {
    val authorizer: Authorizer = mock(classOf[Authorizer])

    val requestHeader = new RequestHeader(ApiKeys.METADATA, ApiKeys.METADATA.latestVersion,
      clientId, 0)

    val numBrokersNeeded = 3
    addTopicToMetadataCache("some-topic", 1, 3)

    authorizeResource(authorizer, AclOperation.DESCRIBE, ResourceType.TOPIC,
      topicName, AuthorizationResult.ALLOWED)

    if (enableAutoTopicCreation)
      authorizeResource(authorizer, AclOperation.CREATE, ResourceType.CLUSTER,
        Resource.CLUSTER_NAME, AuthorizationResult.ALLOWED, logIfDenied = false)

    val topicConfigOverride = mutable.Map.empty[String, String]
    val isInternal =
      topicName match {
        case Topic.GROUP_METADATA_TOPIC_NAME =>
          topicConfigOverride.put(GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, numBrokersNeeded.toString)
          topicConfigOverride.put(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, numBrokersNeeded.toString)
          when(groupCoordinator.groupMetadataTopicConfigs).thenReturn(new Properties)
          true

        case Topic.TRANSACTION_STATE_TOPIC_NAME =>
          topicConfigOverride.put(TransactionLogConfig.TRANSACTIONS_TOPIC_PARTITIONS_CONFIG, numBrokersNeeded.toString)
          topicConfigOverride.put(TransactionLogConfig.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG, numBrokersNeeded.toString)
          when(txnCoordinator.transactionTopicConfigs).thenReturn(new Properties)
          true
        case _ =>
          topicConfigOverride.put(ServerLogConfigs.NUM_PARTITIONS_CONFIG, numBrokersNeeded.toString)
          topicConfigOverride.put(ReplicationConfigs.DEFAULT_REPLICATION_FACTOR_CONFIG, numBrokersNeeded.toString)
          false
      }

    val metadataRequest = new MetadataRequest.Builder(
      List(topicName).asJava, enableAutoTopicCreation
    ).build(requestHeader.apiVersion)
    val request = buildRequest(metadataRequest)

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    val capturedRequest = verifyTopicCreation(topicName, enableAutoTopicCreation, isInternal, request)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer), overrideProperties = topicConfigOverride)
    kafkaApis.handleTopicMetadataRequest(request)

    val response = verifyNoThrottling[MetadataResponse](request)
    val expectedMetadataResponse = util.Collections.singletonList(new TopicMetadata(
      expectedError,
      topicName,
      isInternal,
      util.Collections.emptyList()
    ))

    assertEquals(expectedMetadataResponse, response.topicMetadata())

    if (enableAutoTopicCreation) {
      assertTrue(capturedRequest.getValue.isDefined)
      assertEquals(request.context, capturedRequest.getValue.get)
    }
  }

  private def verifyTopicCreation(topicName: String,
                                  enableAutoTopicCreation: Boolean,
                                  isInternal: Boolean,
                                  request: RequestChannel.Request): ArgumentCaptor[Option[RequestContext]] = {
    val capturedRequest: ArgumentCaptor[Option[RequestContext]] = ArgumentCaptor.forClass(classOf[Option[RequestContext]])
    if (enableAutoTopicCreation) {
      when(clientControllerQuotaManager.newPermissiveQuotaFor(ArgumentMatchers.eq(request)))
        .thenReturn(UnboundedControllerMutationQuota)

      when(autoTopicCreationManager.createTopics(
        ArgumentMatchers.eq(Set(topicName)),
        ArgumentMatchers.eq(UnboundedControllerMutationQuota),
        capturedRequest.capture())).thenReturn(
        Seq(new MetadataResponseTopic()
        .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
        .setIsInternal(isInternal)
        .setName(topicName))
      )
    }
    capturedRequest
  }

  private def setupBrokerMetadata(hasEnoughLiveBrokers: Boolean, numBrokersNeeded: Int): Unit = {
    addTopicToMetadataCache("some-topic", 1,
      if (hasEnoughLiveBrokers)
        numBrokersNeeded
      else
        numBrokersNeeded - 1)
  }

  @Test
  def testInvalidMetadataRequestReturnsError(): Unit = {
    // Construct invalid MetadataRequestTopics. We will try each one separately and ensure the error is thrown.
    val topics = List(new MetadataRequestData.MetadataRequestTopic().setName(null).setTopicId(Uuid.randomUuid()),
      new MetadataRequestData.MetadataRequestTopic().setName(null),
      new MetadataRequestData.MetadataRequestTopic().setTopicId(Uuid.randomUuid()),
      new MetadataRequestData.MetadataRequestTopic().setName("topic1").setTopicId(Uuid.randomUuid()))

    // if version is 10 or 11, the invalid topic metadata should return an error
    val invalidVersions = Set(10, 11)
    invalidVersions.foreach( version =>
      topics.foreach(topic => {
        val metadataRequestData = new MetadataRequestData().setTopics(Collections.singletonList(topic))
        val request = buildRequest(new MetadataRequest(metadataRequestData, version.toShort))
        val kafkaApis = createKafkaApis()
        try {
          val capturedResponse: ArgumentCaptor[AbstractResponse] = ArgumentCaptor.forClass(classOf[AbstractResponse])
          kafkaApis.handle(request, RequestLocal.withThreadConfinedCaching)
          verify(requestChannel).sendResponse(
            ArgumentMatchers.eq(request),
            capturedResponse.capture(),
            any()
          )
          val response = capturedResponse.getValue.asInstanceOf[MetadataResponse]
          assertEquals(1, response.topicMetadata.size)
          assertEquals(1, response.errorCounts.get(Errors.INVALID_REQUEST))
          response.data.topics.forEach(topic => assertNotEquals(null, topic.name))
          reset(requestChannel)
        } finally {
          kafkaApis.close()
        }
      })
    )
  }

  @Test
  def testHandleOffsetCommitRequest(): Unit = {
    addTopicToMetadataCache("foo", numPartitions = 1)

    val offsetCommitRequest = new OffsetCommitRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setTopics(List(
        new OffsetCommitRequestData.OffsetCommitRequestTopic()
          .setName("foo")
          .setPartitions(List(
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(10)).asJava)).asJava)

    val requestChannelRequest = buildRequest(new OffsetCommitRequest.Builder(offsetCommitRequest).build())

    val future = new CompletableFuture[OffsetCommitResponseData]()
    when(groupCoordinator.commitOffsets(
      requestChannelRequest.context,
      offsetCommitRequest,
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handle(
      requestChannelRequest,
      RequestLocal.noCaching
    )

    // This is the response returned by the group coordinator.
    val offsetCommitResponse = new OffsetCommitResponseData()
      .setTopics(List(
        new OffsetCommitResponseData.OffsetCommitResponseTopic()
          .setName("foo")
          .setPartitions(List(
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code)).asJava)).asJava)

    future.complete(offsetCommitResponse)
    val response = verifyNoThrottling[OffsetCommitResponse](requestChannelRequest)
    assertEquals(offsetCommitResponse, response.data)
  }

  @Test
  def testHandleOffsetCommitRequestFutureFailed(): Unit = {
    addTopicToMetadataCache("foo", numPartitions = 1)

    val offsetCommitRequest = new OffsetCommitRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setTopics(List(
        new OffsetCommitRequestData.OffsetCommitRequestTopic()
          .setName("foo")
          .setPartitions(List(
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(10)).asJava)).asJava)

    val requestChannelRequest = buildRequest(new OffsetCommitRequest.Builder(offsetCommitRequest).build())

    val future = new CompletableFuture[OffsetCommitResponseData]()
    when(groupCoordinator.commitOffsets(
      requestChannelRequest.context,
      offsetCommitRequest,
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)

    kafkaApis = createKafkaApis()
    kafkaApis.handle(
      requestChannelRequest,
      RequestLocal.noCaching
    )

    val expectedOffsetCommitResponse = new OffsetCommitResponseData()
      .setTopics(List(
        new OffsetCommitResponseData.OffsetCommitResponseTopic()
          .setName("foo")
          .setPartitions(List(
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NOT_COORDINATOR.code)).asJava)).asJava)

    future.completeExceptionally(Errors.NOT_COORDINATOR.exception)
    val response = verifyNoThrottling[OffsetCommitResponse](requestChannelRequest)
    assertEquals(expectedOffsetCommitResponse, response.data)
  }

  @Test
  def testHandleOffsetCommitRequestTopicsAndPartitionsValidation(): Unit = {
    addTopicToMetadataCache("foo", numPartitions = 2)
    addTopicToMetadataCache("bar", numPartitions = 2)

    val offsetCommitRequest = new OffsetCommitRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setTopics(List(
        // foo exists but only has 2 partitions.
        new OffsetCommitRequestData.OffsetCommitRequestTopic()
          .setName("foo")
          .setPartitions(List(
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(10),
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(1)
              .setCommittedOffset(20),
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(2)
              .setCommittedOffset(30)).asJava),
        // bar exists.
        new OffsetCommitRequestData.OffsetCommitRequestTopic()
          .setName("bar")
          .setPartitions(List(
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(40),
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(1)
              .setCommittedOffset(50)).asJava),
        // zar does not exist.
        new OffsetCommitRequestData.OffsetCommitRequestTopic()
          .setName("zar")
          .setPartitions(List(
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(60),
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(1)
              .setCommittedOffset(70)).asJava)).asJava)

    val requestChannelRequest = buildRequest(new OffsetCommitRequest.Builder(offsetCommitRequest).build())

    // This is the request expected by the group coordinator.
    val expectedOffsetCommitRequest = new OffsetCommitRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setTopics(List(
        // foo exists but only has 2 partitions.
        new OffsetCommitRequestData.OffsetCommitRequestTopic()
          .setName("foo")
          .setPartitions(List(
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(10),
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(1)
              .setCommittedOffset(20)).asJava),
        new OffsetCommitRequestData.OffsetCommitRequestTopic()
          .setName("bar")
          .setPartitions(List(
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(40),
            new OffsetCommitRequestData.OffsetCommitRequestPartition()
              .setPartitionIndex(1)
              .setCommittedOffset(50)).asJava)).asJava)

    val future = new CompletableFuture[OffsetCommitResponseData]()
    when(groupCoordinator.commitOffsets(
      requestChannelRequest.context,
      expectedOffsetCommitRequest,
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handle(
      requestChannelRequest,
      RequestLocal.noCaching
    )

    // This is the response returned by the group coordinator.
    val offsetCommitResponse = new OffsetCommitResponseData()
      .setTopics(List(
        new OffsetCommitResponseData.OffsetCommitResponseTopic()
          .setName("foo")
          .setPartitions(List(
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code),
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.NONE.code)).asJava),
        new OffsetCommitResponseData.OffsetCommitResponseTopic()
          .setName("bar")
          .setPartitions(List(
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code),
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.NONE.code)).asJava)).asJava)

    val expectedOffsetCommitResponse = new OffsetCommitResponseData()
      .setTopics(List(
        new OffsetCommitResponseData.OffsetCommitResponseTopic()
          .setName("foo")
          .setPartitions(List(
            // foo-2 is first because partitions failing the validation
            // are put in the response first.
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(2)
              .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code),
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code),
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.NONE.code)).asJava),
        // zar is before bar because topics failing the validation are
        // put in the response first.
        new OffsetCommitResponseData.OffsetCommitResponseTopic()
          .setName("zar")
          .setPartitions(List(
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code),
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code)).asJava),
        new OffsetCommitResponseData.OffsetCommitResponseTopic()
          .setName("bar")
          .setPartitions(List(
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code),
            new OffsetCommitResponseData.OffsetCommitResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.NONE.code)).asJava)).asJava)

    future.complete(offsetCommitResponse)
    val response = verifyNoThrottling[OffsetCommitResponse](requestChannelRequest)
    assertEquals(expectedOffsetCommitResponse, response.data)
  }

  @Test
  def testOffsetCommitWithInvalidPartition(): Unit = {
    val topic = "topic"
    addTopicToMetadataCache(topic, numPartitions = 1)

    def checkInvalidPartition(invalidPartitionId: Int): Unit = {
      reset(replicaManager, clientRequestQuotaManager, requestChannel)

      val offsetCommitRequest = new OffsetCommitRequest.Builder(
        new OffsetCommitRequestData()
          .setGroupId("groupId")
          .setTopics(Collections.singletonList(
            new OffsetCommitRequestData.OffsetCommitRequestTopic()
              .setName(topic)
              .setPartitions(Collections.singletonList(
                new OffsetCommitRequestData.OffsetCommitRequestPartition()
                  .setPartitionIndex(invalidPartitionId)
                  .setCommittedOffset(15)
                  .setCommittedLeaderEpoch(RecordBatch.NO_PARTITION_LEADER_EPOCH)
                  .setCommittedMetadata(""))
              )
          ))).build()

      val request = buildRequest(offsetCommitRequest)
      when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
        any[Long])).thenReturn(0)
      val kafkaApis = createKafkaApis()
      try {
        kafkaApis.handleOffsetCommitRequest(request, RequestLocal.withThreadConfinedCaching)

        val response = verifyNoThrottling[OffsetCommitResponse](request)
        assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION,
          Errors.forCode(response.data.topics().get(0).partitions().get(0).errorCode))
      } finally {
        kafkaApis.close()
      }
    }

    checkInvalidPartition(-1)
    checkInvalidPartition(1) // topic has only one partition
  }

  @Test
  def testTxnOffsetCommitWithInvalidPartition(): Unit = {
    val topic = "topic"
    addTopicToMetadataCache(topic, numPartitions = 1)

    def checkInvalidPartition(invalidPartitionId: Int): Unit = {
      reset(replicaManager, clientRequestQuotaManager, requestChannel)

      val invalidTopicPartition = new TopicPartition(topic, invalidPartitionId)
      val partitionOffsetCommitData = new TxnOffsetCommitRequest.CommittedOffset(15L, "", Optional.empty())
      val offsetCommitRequest = new TxnOffsetCommitRequest.Builder(
        "txnId",
        "groupId",
        15L,
        0.toShort,
        Map(invalidTopicPartition -> partitionOffsetCommitData).asJava,
        true
      ).build()
      val request = buildRequest(offsetCommitRequest)
      when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
        any[Long])).thenReturn(0)
      val kafkaApis = createKafkaApis()
      try {
        kafkaApis.handleTxnOffsetCommitRequest(request, RequestLocal.withThreadConfinedCaching)

        val response = verifyNoThrottling[TxnOffsetCommitResponse](request)
        assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, response.errors().get(invalidTopicPartition))
      } finally {
        kafkaApis.close()
      }
    }

    checkInvalidPartition(-1)
    checkInvalidPartition(1) // topic has only one partition
  }

  @Test
  def testTxnOffsetCommitWithInklessTopic(): Unit = {
    val topic = "topic"
    addTopicToMetadataCache(topic, numPartitions = 1)

    reset(replicaManager, clientRequestQuotaManager, requestChannel)

    val topicPartition = new TopicPartition(topic, 0)
    val partitionOffsetCommitData = new TxnOffsetCommitRequest.CommittedOffset(15L, "", Optional.empty())
    val offsetCommitRequest = new TxnOffsetCommitRequest.Builder(
      "txnId",
      "groupId",
      15L,
      0.toShort,
      Map(topicPartition -> partitionOffsetCommitData).asJava,
      true
    ).build()
    val request = buildRequest(offsetCommitRequest)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    val kafkaApis = createKafkaApis(inklessSharedState = Some(createInklessSharedStateWithTopic(topic)))
    try {
      kafkaApis.handleTxnOffsetCommitRequest(request, RequestLocal.withThreadConfinedCaching)

      val response = verifyNoThrottling[TxnOffsetCommitResponse](request)
      assertEquals(Errors.INVALID_TOPIC_EXCEPTION, response.errors().get(topicPartition))
    } finally {
      kafkaApis.close()
    }
  }

  @Test
  def testHandleTxnOffsetCommitRequest(): Unit = {
    addTopicToMetadataCache("foo", numPartitions = 1)

    val txnOffsetCommitRequest = new TxnOffsetCommitRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setGenerationId(10)
      .setProducerId(20)
      .setProducerEpoch(30)
      .setGroupInstanceId("instance-id")
      .setTransactionalId("transactional-id")
      .setTopics(List(
        new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
          .setName("foo")
          .setPartitions(List(
            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(10)).asJava)).asJava)

    val requestChannelRequest = buildRequest(new TxnOffsetCommitRequest.Builder(txnOffsetCommitRequest).build())

    val future = new CompletableFuture[TxnOffsetCommitResponseData]()
    when(txnCoordinator.partitionFor(txnOffsetCommitRequest.transactionalId)).thenReturn(0)
    when(groupCoordinator.commitTransactionalOffsets(
      requestChannelRequest.context,
      txnOffsetCommitRequest,
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handle(
      requestChannelRequest,
      RequestLocal.noCaching
    )

    // This is the response returned by the group coordinator.
    val txnOffsetCommitResponse = new TxnOffsetCommitResponseData()
      .setTopics(List(
        new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
          .setName("foo")
          .setPartitions(List(
            new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code)).asJava)).asJava)

    future.complete(txnOffsetCommitResponse)
    val response = verifyNoThrottling[TxnOffsetCommitResponse](requestChannelRequest)
    assertEquals(txnOffsetCommitResponse, response.data)
  }

  @Test
  def testHandleTxnOffsetCommitRequestFutureFailed(): Unit = {
    addTopicToMetadataCache("foo", numPartitions = 1)

    val txnOffsetCommitRequest = new TxnOffsetCommitRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setTopics(List(
        new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
          .setName("foo")
          .setPartitions(List(
            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(10)).asJava)).asJava)

    val requestChannelRequest = buildRequest(new TxnOffsetCommitRequest.Builder(txnOffsetCommitRequest).build())

    val future = new CompletableFuture[TxnOffsetCommitResponseData]()
    when(txnCoordinator.partitionFor(txnOffsetCommitRequest.transactionalId)).thenReturn(0)
    when(groupCoordinator.commitTransactionalOffsets(
      requestChannelRequest.context,
      txnOffsetCommitRequest,
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handle(
      requestChannelRequest,
      RequestLocal.noCaching
    )

    val expectedTxnOffsetCommitResponse = new TxnOffsetCommitResponseData()
      .setTopics(List(
        new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
          .setName("foo")
          .setPartitions(List(
            new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NOT_COORDINATOR.code)).asJava)).asJava)

    future.completeExceptionally(Errors.NOT_COORDINATOR.exception)
    val response = verifyNoThrottling[TxnOffsetCommitResponse](requestChannelRequest)
    assertEquals(expectedTxnOffsetCommitResponse, response.data)
  }

  @Test
  def testHandleTxnOffsetCommitRequestTopicsAndPartitionsValidation(): Unit = {
    addTopicToMetadataCache("foo", numPartitions = 2)
    addTopicToMetadataCache("bar", numPartitions = 2)

    val txnOffsetCommitRequest = new TxnOffsetCommitRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setTopics(List(
        // foo exists but only has 2 partitions.
        new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
          .setName("foo")
          .setPartitions(List(
            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(10),
            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
              .setPartitionIndex(1)
              .setCommittedOffset(20),
            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
              .setPartitionIndex(2)
              .setCommittedOffset(30)).asJava),
        // bar exists.
        new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
          .setName("bar")
          .setPartitions(List(
            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(40),
            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
              .setPartitionIndex(1)
              .setCommittedOffset(50)).asJava),
        // zar does not exist.
        new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
          .setName("zar")
          .setPartitions(List(
            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(60),
            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
              .setPartitionIndex(1)
              .setCommittedOffset(70)).asJava)).asJava)

    val requestChannelRequest = buildRequest(new TxnOffsetCommitRequest.Builder(txnOffsetCommitRequest).build())

    // This is the request expected by the group coordinator.
    val expectedTxnOffsetCommitRequest = new TxnOffsetCommitRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setTopics(List(
        // foo exists but only has 2 partitions.
        new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
          .setName("foo")
          .setPartitions(List(
            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(10),
            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
              .setPartitionIndex(1)
              .setCommittedOffset(20)).asJava),
        new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
          .setName("bar")
          .setPartitions(List(
            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
              .setPartitionIndex(0)
              .setCommittedOffset(40),
            new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
              .setPartitionIndex(1)
              .setCommittedOffset(50)).asJava)).asJava)

    val future = new CompletableFuture[TxnOffsetCommitResponseData]()
    when(txnCoordinator.partitionFor(expectedTxnOffsetCommitRequest.transactionalId)).thenReturn(0)
    when(groupCoordinator.commitTransactionalOffsets(
      requestChannelRequest.context,
      expectedTxnOffsetCommitRequest,
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handle(
      requestChannelRequest,
      RequestLocal.noCaching
    )

    // This is the response returned by the group coordinator.
    val txnOffsetCommitResponse = new TxnOffsetCommitResponseData()
      .setTopics(List(
        new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
          .setName("foo")
          .setPartitions(List(
            new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code),
            new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.NONE.code)).asJava),
        new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
          .setName("bar")
          .setPartitions(List(
            new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code),
            new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.NONE.code)).asJava)).asJava)

    val expectedTxnOffsetCommitResponse = new TxnOffsetCommitResponseData()
      .setTopics(List(
        new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
          .setName("foo")
          .setPartitions(List(
            // foo-2 is first because partitions failing the validation
            // are put in the response first.
            new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
              .setPartitionIndex(2)
              .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code),
            new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code),
            new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.NONE.code)).asJava),
        // zar is before bar because topics failing the validation are
        // put in the response first.
        new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
          .setName("zar")
          .setPartitions(List(
            new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code),
            new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code)).asJava),
        new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
          .setName("bar")
          .setPartitions(List(
            new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code),
            new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.NONE.code)).asJava)).asJava)

    future.complete(txnOffsetCommitResponse)
    val response = verifyNoThrottling[TxnOffsetCommitResponse](requestChannelRequest)
    assertEquals(expectedTxnOffsetCommitResponse, response.data)
  }

  @ParameterizedTest
  @ApiKeyVersionsSource(apiKey = ApiKeys.TXN_OFFSET_COMMIT)
  def shouldReplaceCoordinatorNotAvailableWithLoadInProcessInTxnOffsetCommitWithOlderClient(version: Short): Unit = {
    val topic = "topic"
    addTopicToMetadataCache(topic, numPartitions = 2)

    val topicPartition = new TopicPartition(topic, 1)
    val capturedResponse: ArgumentCaptor[TxnOffsetCommitResponse] = ArgumentCaptor.forClass(classOf[TxnOffsetCommitResponse])

    val partitionOffsetCommitData = new TxnOffsetCommitRequest.CommittedOffset(15L, "", Optional.empty())
    val groupId = "groupId"

    val producerId = 15L
    val epoch = 0.toShort

    val offsetCommitRequest = new TxnOffsetCommitRequest.Builder(
      "txnId",
      groupId,
      producerId,
      epoch,
      Map(topicPartition -> partitionOffsetCommitData).asJava,
      version >= TxnOffsetCommitRequest.LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2
    ).build(version)
    val request = buildRequest(offsetCommitRequest)

    val requestLocal = RequestLocal.withThreadConfinedCaching
    val future = new CompletableFuture[TxnOffsetCommitResponseData]()
    when(txnCoordinator.partitionFor(offsetCommitRequest.data.transactionalId)).thenReturn(0)
    when(groupCoordinator.commitTransactionalOffsets(
      request.context,
      offsetCommitRequest.data,
      requestLocal.bufferSupplier
    )).thenReturn(future)

    future.complete(new TxnOffsetCommitResponseData()
      .setTopics(List(
        new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic()
          .setName(topicPartition.topic)
          .setPartitions(List(
            new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition()
              .setPartitionIndex(topicPartition.partition)
              .setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code)
          ).asJava)
      ).asJava))
    kafkaApis = createKafkaApis()
    kafkaApis.handleTxnOffsetCommitRequest(request, requestLocal)

    verify(requestChannel).sendResponse(
      ArgumentMatchers.eq(request),
      capturedResponse.capture(),
      ArgumentMatchers.eq(None)
    )
    val response = capturedResponse.getValue

    if (version < 2) {
      assertEquals(Errors.COORDINATOR_NOT_AVAILABLE, response.errors().get(topicPartition))
    } else {
      assertEquals(Errors.COORDINATOR_LOAD_IN_PROGRESS, response.errors().get(topicPartition))
    }
  }

  @Test
  def shouldReplaceProducerFencedWithInvalidProducerEpochInInitProducerIdWithOlderClient(): Unit = {
    val topic = "topic"
    addTopicToMetadataCache(topic, numPartitions = 2)

    for (version <- ApiKeys.INIT_PRODUCER_ID.oldestVersion to ApiKeys.INIT_PRODUCER_ID.latestVersion) {

      reset(replicaManager, clientRequestQuotaManager, requestChannel, txnCoordinator)

      val capturedResponse: ArgumentCaptor[InitProducerIdResponse] = ArgumentCaptor.forClass(classOf[InitProducerIdResponse])
      val responseCallback: ArgumentCaptor[InitProducerIdResult => Unit] = ArgumentCaptor.forClass(classOf[InitProducerIdResult => Unit])

      val transactionalId = "txnId"
      val producerId = if (version < 3)
        RecordBatch.NO_PRODUCER_ID
      else
        15

      val epoch = if (version < 3)
        RecordBatch.NO_PRODUCER_EPOCH
      else
        0.toShort

      val txnTimeoutMs = TimeUnit.MINUTES.toMillis(15).toInt

      val initProducerIdRequest = new InitProducerIdRequest.Builder(
        new InitProducerIdRequestData()
          .setTransactionalId(transactionalId)
          .setTransactionTimeoutMs(txnTimeoutMs)
          .setProducerId(producerId)
          .setProducerEpoch(epoch)
      ).build(version.toShort)

      val request = buildRequest(initProducerIdRequest)

      val expectedProducerIdAndEpoch = if (version < 3)
        Option.empty
      else
        Option(new ProducerIdAndEpoch(producerId, epoch))

      val requestLocal = RequestLocal.withThreadConfinedCaching
      when(txnCoordinator.handleInitProducerId(
        ArgumentMatchers.eq(transactionalId),
        ArgumentMatchers.eq(txnTimeoutMs),
        ArgumentMatchers.eq(expectedProducerIdAndEpoch),
        responseCallback.capture(),
        ArgumentMatchers.eq(requestLocal)
      )).thenAnswer(_ => responseCallback.getValue.apply(InitProducerIdResult(producerId, epoch, Errors.PRODUCER_FENCED)))
      val kafkaApis = createKafkaApis()
      try {
        kafkaApis.handleInitProducerIdRequest(request, requestLocal)

        verify(requestChannel).sendResponse(
          ArgumentMatchers.eq(request),
          capturedResponse.capture(),
          ArgumentMatchers.eq(None)
        )
        val response = capturedResponse.getValue

        if (version < 4) {
          assertEquals(Errors.INVALID_PRODUCER_EPOCH.code, response.data.errorCode)
        } else {
          assertEquals(Errors.PRODUCER_FENCED.code, response.data.errorCode)
        }
      } finally {
        kafkaApis.close()
      }
    }
  }

  @Test
  def shouldReplaceProducerFencedWithInvalidProducerEpochInAddOffsetToTxnWithOlderClient(): Unit = {
    val topic = "topic"
    addTopicToMetadataCache(topic, numPartitions = 2)

    for (version <- ApiKeys.ADD_OFFSETS_TO_TXN.oldestVersion to ApiKeys.ADD_OFFSETS_TO_TXN.latestVersion) {

      reset(replicaManager, clientRequestQuotaManager, requestChannel, groupCoordinator, txnCoordinator)

      val capturedResponse: ArgumentCaptor[AddOffsetsToTxnResponse] = ArgumentCaptor.forClass(classOf[AddOffsetsToTxnResponse])
      val responseCallback: ArgumentCaptor[Errors => Unit] = ArgumentCaptor.forClass(classOf[Errors => Unit])

      val groupId = "groupId"
      val transactionalId = "txnId"
      val producerId = 15L
      val epoch = 0.toShort

      val addOffsetsToTxnRequest = new AddOffsetsToTxnRequest.Builder(
        new AddOffsetsToTxnRequestData()
          .setGroupId(groupId)
          .setTransactionalId(transactionalId)
          .setProducerId(producerId)
          .setProducerEpoch(epoch)
      ).build(version.toShort)
      val request = buildRequest(addOffsetsToTxnRequest)

      val partition = 1
      when(groupCoordinator.partitionFor(
        ArgumentMatchers.eq(groupId)
      )).thenReturn(partition)

      val requestLocal = RequestLocal.withThreadConfinedCaching
      when(txnCoordinator.handleAddPartitionsToTransaction(
        ArgumentMatchers.eq(transactionalId),
        ArgumentMatchers.eq(producerId),
        ArgumentMatchers.eq(epoch),
        ArgumentMatchers.eq(Set(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, partition))),
        responseCallback.capture(),
        ArgumentMatchers.eq(TransactionVersion.TV_0),
        ArgumentMatchers.eq(requestLocal)
      )).thenAnswer(_ => responseCallback.getValue.apply(Errors.PRODUCER_FENCED))
      val kafkaApis = createKafkaApis()
      try {
        kafkaApis.handleAddOffsetsToTxnRequest(request, requestLocal)

        verify(requestChannel).sendResponse(
          ArgumentMatchers.eq(request),
          capturedResponse.capture(),
          ArgumentMatchers.eq(None)
        )
        val response = capturedResponse.getValue

        if (version < 2) {
          assertEquals(Errors.INVALID_PRODUCER_EPOCH.code, response.data.errorCode)
        } else {
          assertEquals(Errors.PRODUCER_FENCED.code, response.data.errorCode)
        }
      } finally {
        kafkaApis.close()
      }
    }
  }

  @Test
  def shouldReplaceProducerFencedWithInvalidProducerEpochInAddPartitionToTxnWithOlderClient(): Unit = {
    val topic = "topic"
    addTopicToMetadataCache(topic, numPartitions = 2)

    for (version <- ApiKeys.ADD_PARTITIONS_TO_TXN.oldestVersion to 3) {

      reset(replicaManager, clientRequestQuotaManager, requestChannel, txnCoordinator)

      val capturedResponse: ArgumentCaptor[AddPartitionsToTxnResponse] = ArgumentCaptor.forClass(classOf[AddPartitionsToTxnResponse])
      val responseCallback: ArgumentCaptor[Errors => Unit] = ArgumentCaptor.forClass(classOf[Errors => Unit])

      val transactionalId = "txnId"
      val producerId = 15L
      val epoch = 0.toShort

      val partition = 1
      val topicPartition = new TopicPartition(topic, partition)

      val addPartitionsToTxnRequest = AddPartitionsToTxnRequest.Builder.forClient(
        transactionalId,
        producerId,
        epoch,
        Collections.singletonList(topicPartition)
      ).build(version.toShort)
      val request = buildRequest(addPartitionsToTxnRequest)

      val requestLocal = RequestLocal.withThreadConfinedCaching
      when(txnCoordinator.handleAddPartitionsToTransaction(
        ArgumentMatchers.eq(transactionalId),
        ArgumentMatchers.eq(producerId),
        ArgumentMatchers.eq(epoch),
        ArgumentMatchers.eq(Set(topicPartition)),
        responseCallback.capture(),
        ArgumentMatchers.eq(TransactionVersion.TV_0),
        ArgumentMatchers.eq(requestLocal)
      )).thenAnswer(_ => responseCallback.getValue.apply(Errors.PRODUCER_FENCED))
      val kafkaApis = createKafkaApis()
      try {
        kafkaApis.handleAddPartitionsToTxnRequest(request, requestLocal)

        verify(requestChannel).sendResponse(
          ArgumentMatchers.eq(request),
          capturedResponse.capture(),
          ArgumentMatchers.eq(None)
        )
        val response = capturedResponse.getValue

        if (version < 2) {
          assertEquals(Collections.singletonMap(topicPartition, Errors.INVALID_PRODUCER_EPOCH), response.errors().get(AddPartitionsToTxnResponse.V3_AND_BELOW_TXN_ID))
        } else {
          assertEquals(Collections.singletonMap(topicPartition, Errors.PRODUCER_FENCED), response.errors().get(AddPartitionsToTxnResponse.V3_AND_BELOW_TXN_ID))
        }
      } finally {
        kafkaApis.close()
      }
    }
  }

  @Test
  def testBatchedAddPartitionsToTxnRequest(): Unit = {
    val topic = "topic"
    addTopicToMetadataCache(topic, numPartitions = 2)

    val responseCallback: ArgumentCaptor[Errors => Unit] = ArgumentCaptor.forClass(classOf[Errors => Unit])
    val verifyPartitionsCallback: ArgumentCaptor[AddPartitionsToTxnResult => Unit] = ArgumentCaptor.forClass(classOf[AddPartitionsToTxnResult => Unit])

    val transactionalId1 = "txnId1"
    val transactionalId2 = "txnId2"
    val producerId = 15L
    val epoch = 0.toShort

    val tp0 = new TopicPartition(topic, 0)
    val tp1 = new TopicPartition(topic, 1)

    val addPartitionsToTxnRequest = AddPartitionsToTxnRequest.Builder.forBroker(
      new AddPartitionsToTxnTransactionCollection(
        List(new AddPartitionsToTxnTransaction()
          .setTransactionalId(transactionalId1)
          .setProducerId(producerId)
          .setProducerEpoch(epoch)
          .setVerifyOnly(false)
          .setTopics(new AddPartitionsToTxnTopicCollection(
            Collections.singletonList(new AddPartitionsToTxnTopic()
              .setName(tp0.topic)
              .setPartitions(Collections.singletonList(tp0.partition))
            ).iterator())
          ), new AddPartitionsToTxnTransaction()
          .setTransactionalId(transactionalId2)
          .setProducerId(producerId)
          .setProducerEpoch(epoch)
          .setVerifyOnly(true)
          .setTopics(new AddPartitionsToTxnTopicCollection(
            Collections.singletonList(new AddPartitionsToTxnTopic()
              .setName(tp1.topic)
              .setPartitions(Collections.singletonList(tp1.partition))
            ).iterator())
          )
        ).asJava.iterator()
      )
    ).build(4.toShort)
    val request = buildRequest(addPartitionsToTxnRequest)

    val requestLocal = RequestLocal.withThreadConfinedCaching
    when(txnCoordinator.handleAddPartitionsToTransaction(
      ArgumentMatchers.eq(transactionalId1),
      ArgumentMatchers.eq(producerId),
      ArgumentMatchers.eq(epoch),
      ArgumentMatchers.eq(Set(tp0)),
      responseCallback.capture(),
      any[TransactionVersion],
      ArgumentMatchers.eq(requestLocal)
    )).thenAnswer(_ => responseCallback.getValue.apply(Errors.NONE))

    when(txnCoordinator.handleVerifyPartitionsInTransaction(
      ArgumentMatchers.eq(transactionalId2),
      ArgumentMatchers.eq(producerId),
      ArgumentMatchers.eq(epoch),
      ArgumentMatchers.eq(Set(tp1)),
      verifyPartitionsCallback.capture(),
    )).thenAnswer(_ => verifyPartitionsCallback.getValue.apply(AddPartitionsToTxnResponse.resultForTransaction(transactionalId2, Map(tp1 -> Errors.PRODUCER_FENCED).asJava)))
    kafkaApis = createKafkaApis()
    kafkaApis.handleAddPartitionsToTxnRequest(request, requestLocal)

    val response = verifyNoThrottling[AddPartitionsToTxnResponse](request)

    val expectedErrors = Map(
      transactionalId1 -> Collections.singletonMap(tp0, Errors.NONE),
      transactionalId2 -> Collections.singletonMap(tp1, Errors.PRODUCER_FENCED)
    ).asJava

    assertEquals(expectedErrors, response.errors())
  }

  @ParameterizedTest
  @ApiKeyVersionsSource(apiKey = ApiKeys.ADD_PARTITIONS_TO_TXN)
  def testHandleAddPartitionsToTxnAuthorizationFailedAndMetrics(version: Short): Unit = {
    val requestMetrics = new RequestChannelMetrics(Collections.singleton(ApiKeys.ADD_PARTITIONS_TO_TXN))
    try {
      val topic = "topic"

      val transactionalId = "txnId1"
      val producerId = 15L
      val epoch = 0.toShort

      val tp = new TopicPartition(topic, 0)

      val addPartitionsToTxnRequest =
        if (version < 4)
          AddPartitionsToTxnRequest.Builder.forClient(
            transactionalId,
            producerId,
            epoch,
            Collections.singletonList(tp)).build(version)
        else
          AddPartitionsToTxnRequest.Builder.forBroker(
            new AddPartitionsToTxnTransactionCollection(
              List(new AddPartitionsToTxnTransaction()
                .setTransactionalId(transactionalId)
                .setProducerId(producerId)
                .setProducerEpoch(epoch)
                .setVerifyOnly(true)
                .setTopics(new AddPartitionsToTxnTopicCollection(
                  Collections.singletonList(new AddPartitionsToTxnTopic()
                    .setName(tp.topic)
                    .setPartitions(Collections.singletonList(tp.partition))
                  ).iterator()))
              ).asJava.iterator())).build(version)

      val requestChannelRequest = buildRequest(addPartitionsToTxnRequest, requestMetrics = requestMetrics)

      val authorizer: Authorizer = mock(classOf[Authorizer])
      when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
        .thenReturn(Seq(AuthorizationResult.DENIED).asJava)
      kafkaApis = createKafkaApis(authorizer = Some(authorizer))
      kafkaApis.handle(
        requestChannelRequest,
        RequestLocal.noCaching
      )

      val response = verifyNoThrottlingAndUpdateMetrics[AddPartitionsToTxnResponse](requestChannelRequest)
      val error = if (version < 4)
        response.errors().get(AddPartitionsToTxnResponse.V3_AND_BELOW_TXN_ID).get(tp)
      else
        Errors.forCode(response.data().errorCode)

      val expectedError = if (version < 4) Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED else Errors.CLUSTER_AUTHORIZATION_FAILED
      assertEquals(expectedError, error)

      val metricName = if (version < 4) ApiKeys.ADD_PARTITIONS_TO_TXN.name else RequestMetrics.VERIFY_PARTITIONS_IN_TXN_METRIC_NAME
      assertEquals(8, TestUtils.metersCount(metricName))
    } finally {
      requestMetrics.close()
    }
  }

  @ParameterizedTest
  @ApiKeyVersionsSource(apiKey = ApiKeys.ADD_PARTITIONS_TO_TXN)
  def testAddPartitionsToTxnOperationNotAttempted(version: Short): Unit = {
    val topic = "topic"
    addTopicToMetadataCache(topic, numPartitions = 1)

    val transactionalId = "txnId1"
    val producerId = 15L
    val epoch = 0.toShort

    val tp0 = new TopicPartition(topic, 0)
    val tp1 = new TopicPartition(topic, 1)

    val addPartitionsToTxnRequest = if (version < 4)
      AddPartitionsToTxnRequest.Builder.forClient(
        transactionalId,
        producerId,
        epoch,
        List(tp0, tp1).asJava).build(version)
    else
      AddPartitionsToTxnRequest.Builder.forBroker(
        new AddPartitionsToTxnTransactionCollection(
          List(new AddPartitionsToTxnTransaction()
            .setTransactionalId(transactionalId)
            .setProducerId(producerId)
            .setProducerEpoch(epoch)
            .setVerifyOnly(true)
            .setTopics(new AddPartitionsToTxnTopicCollection(
              Collections.singletonList(new AddPartitionsToTxnTopic()
                .setName(tp0.topic)
                .setPartitions(List[Integer](tp0.partition, tp1.partition()).asJava)
              ).iterator()))
          ).asJava.iterator())).build(version)

    val requestChannelRequest = buildRequest(addPartitionsToTxnRequest)
    kafkaApis = createKafkaApis()
    kafkaApis.handleAddPartitionsToTxnRequest(
      requestChannelRequest,
      RequestLocal.noCaching
    )

    val response = verifyNoThrottling[AddPartitionsToTxnResponse](requestChannelRequest)

    def checkErrorForTp(tp: TopicPartition, expectedError: Errors): Unit = {
      val error = if (version < 4)
        response.errors().get(AddPartitionsToTxnResponse.V3_AND_BELOW_TXN_ID).get(tp)
      else
        response.errors().get(transactionalId).get(tp)

      assertEquals(expectedError, error)
    }

    checkErrorForTp(tp0, Errors.OPERATION_NOT_ATTEMPTED)
    checkErrorForTp(tp1, Errors.UNKNOWN_TOPIC_OR_PARTITION)
  }

  @Test
  def shouldReplaceProducerFencedWithInvalidProducerEpochInEndTxnWithOlderClient(): Unit = {
    val topic = "topic"
    addTopicToMetadataCache(topic, numPartitions = 2)

    for (version <- ApiKeys.END_TXN.oldestVersion to ApiKeys.END_TXN.latestVersion) {
      reset(replicaManager, clientRequestQuotaManager, requestChannel, txnCoordinator)

      val capturedResponse: ArgumentCaptor[EndTxnResponse] = ArgumentCaptor.forClass(classOf[EndTxnResponse])
      val responseCallback: ArgumentCaptor[(Errors, Long, Short) => Unit] = ArgumentCaptor.forClass(classOf[(Errors, Long, Short) => Unit])

      val transactionalId = "txnId"
      val producerId = 15L
      val epoch = 0.toShort

      val clientTransactionVersion = if (version > 4) TransactionVersion.TV_2 else TransactionVersion.TV_0
      val isTransactionV2Enabled = clientTransactionVersion.equals(TransactionVersion.TV_2)

      val endTxnRequest = new EndTxnRequest.Builder(
        new EndTxnRequestData()
          .setTransactionalId(transactionalId)
          .setProducerId(producerId)
          .setProducerEpoch(epoch)
          .setCommitted(true),
        isTransactionV2Enabled
      ).build(version.toShort)
      val request = buildRequest(endTxnRequest)

      val requestLocal = RequestLocal.withThreadConfinedCaching
      when(txnCoordinator.handleEndTransaction(
        ArgumentMatchers.eq(transactionalId),
        ArgumentMatchers.eq(producerId),
        ArgumentMatchers.eq(epoch),
        ArgumentMatchers.eq(TransactionResult.COMMIT),
        ArgumentMatchers.eq(clientTransactionVersion),
        responseCallback.capture(),
        ArgumentMatchers.eq(requestLocal)
      )).thenAnswer(_ => responseCallback.getValue.apply(Errors.PRODUCER_FENCED, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH))
      val kafkaApis = createKafkaApis()
      try {
        kafkaApis.handleEndTxnRequest(request, requestLocal)

        verify(requestChannel).sendResponse(
          ArgumentMatchers.eq(request),
          capturedResponse.capture(),
          ArgumentMatchers.eq(None)
        )
        val response = capturedResponse.getValue

        if (version < 2) {
          assertEquals(Errors.INVALID_PRODUCER_EPOCH.code, response.data.errorCode)
        } else {
          assertEquals(Errors.PRODUCER_FENCED.code, response.data.errorCode)
        }
      } finally {
        kafkaApis.close()
      }
    }
  }

  @Test
  def shouldReplaceProducerFencedWithInvalidProducerEpochInProduceResponse(): Unit = {
    val topic = "topic"
    addTopicToMetadataCache(topic, numPartitions = 2)

    for (version <- ApiKeys.PRODUCE.oldestVersion to ApiKeys.PRODUCE.latestVersion) {

      reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, txnCoordinator)

      val responseCallback: ArgumentCaptor[Map[TopicPartition, PartitionResponse] => Unit] = ArgumentCaptor.forClass(classOf[Map[TopicPartition, PartitionResponse] => Unit])

      val tp = new TopicPartition("topic", 0)

      val produceRequest = ProduceRequest.builder(new ProduceRequestData()
        .setTopicData(new ProduceRequestData.TopicProduceDataCollection(
          Collections.singletonList(new ProduceRequestData.TopicProduceData()
            .setName(tp.topic).setPartitionData(Collections.singletonList(
            new ProduceRequestData.PartitionProduceData()
              .setIndex(tp.partition)
              .setRecords(MemoryRecords.withRecords(Compression.NONE, new SimpleRecord("test".getBytes))))))
            .iterator))
        .setAcks(1.toShort)
        .setTimeoutMs(5000))
        .build(version.toShort)
      val request = buildRequest(produceRequest)

      when(replicaManager.handleProduceAppend(anyLong,
        anyShort,
        ArgumentMatchers.eq(false),
        any(),
        any(),
        responseCallback.capture(),
        any(),
        any(),
        any(),
        any()
      )).thenAnswer(_ => responseCallback.getValue.apply(Map(tp -> new PartitionResponse(Errors.INVALID_PRODUCER_EPOCH))))

      when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
        any[Long])).thenReturn(0)
      when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
        any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
      val kafkaApis = createKafkaApis()
      try {
        kafkaApis.handleProduceRequest(request, RequestLocal.withThreadConfinedCaching)

        val response = verifyNoThrottling[ProduceResponse](request)

        assertEquals(1, response.data.responses.size)
        val topicProduceResponse = response.data.responses.asScala.head
        assertEquals(1, topicProduceResponse.partitionResponses.size)
        val partitionProduceResponse = topicProduceResponse.partitionResponses.asScala.head
        assertEquals(Errors.INVALID_PRODUCER_EPOCH, Errors.forCode(partitionProduceResponse.errorCode))
      } finally {
        kafkaApis.close()
      }
    }
  }

  @Test
  def testProduceResponseContainsNewLeaderOnNotLeaderOrFollower(): Unit = {
    val topic = "topic"
    addTopicToMetadataCache(topic, numPartitions = 2, numBrokers = 3)

    for (version <- 10 to ApiKeys.PRODUCE.latestVersion) {

      reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, txnCoordinator)

      val responseCallback: ArgumentCaptor[Map[TopicPartition, PartitionResponse] => Unit] = ArgumentCaptor.forClass(classOf[Map[TopicPartition, PartitionResponse] => Unit])

      val tp = new TopicPartition(topic, 0)
      val partition = mock(classOf[Partition])
      val newLeaderId = 2
      val newLeaderEpoch = 5

      val produceRequest = ProduceRequest.builder(new ProduceRequestData()
        .setTopicData(new ProduceRequestData.TopicProduceDataCollection(
          Collections.singletonList(new ProduceRequestData.TopicProduceData()
            .setName(tp.topic).setPartitionData(Collections.singletonList(
            new ProduceRequestData.PartitionProduceData()
              .setIndex(tp.partition)
              .setRecords(MemoryRecords.withRecords(Compression.NONE, new SimpleRecord("test".getBytes))))))
            .iterator))
        .setAcks(1.toShort)
        .setTimeoutMs(5000))
        .build(version.toShort)
      val request = buildRequest(produceRequest)

      when(replicaManager.handleProduceAppend(anyLong,
        anyShort,
        ArgumentMatchers.eq(false),
        any(),
        any(),
        responseCallback.capture(),
        any(),
        any(),
        any(),
        any())
      ).thenAnswer(_ => responseCallback.getValue.apply(Map(tp -> new PartitionResponse(Errors.NOT_LEADER_OR_FOLLOWER))))

      when(replicaManager.getPartitionOrError(tp)).thenAnswer(_ => Right(partition))
      when(partition.leaderReplicaIdOpt).thenAnswer(_ => Some(newLeaderId))
      when(partition.getLeaderEpoch).thenAnswer(_ => newLeaderEpoch)

      when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
        any[Long])).thenReturn(0)
      when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
        any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
      kafkaApis = createKafkaApis()
      kafkaApis.handleProduceRequest(request, RequestLocal.withThreadConfinedCaching)

      val response = verifyNoThrottling[ProduceResponse](request)

      assertEquals(1, response.data.responses.size)
      val topicProduceResponse = response.data.responses.asScala.head
      assertEquals(1, topicProduceResponse.partitionResponses.size)
      val partitionProduceResponse = topicProduceResponse.partitionResponses.asScala.head
      assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, Errors.forCode(partitionProduceResponse.errorCode))
      assertEquals(newLeaderId, partitionProduceResponse.currentLeader.leaderId())
      assertEquals(newLeaderEpoch, partitionProduceResponse.currentLeader.leaderEpoch())
      assertEquals(1, response.data.nodeEndpoints.size)
      val node = response.data.nodeEndpoints.asScala.head
      assertEquals(2, node.nodeId)
      assertEquals("broker2", node.host)
    }
  }

  @Test
  def testProduceResponseReplicaManagerLookupErrorOnNotLeaderOrFollower(): Unit = {
    val topic = "topic"
    addTopicToMetadataCache(topic, numPartitions = 2, numBrokers = 3)

    for (version <- 10 to ApiKeys.PRODUCE.latestVersion) {

      reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, txnCoordinator)

      val responseCallback: ArgumentCaptor[Map[TopicPartition, PartitionResponse] => Unit] = ArgumentCaptor.forClass(classOf[Map[TopicPartition, PartitionResponse] => Unit])

      val tp = new TopicPartition(topic, 0)

      val produceRequest = ProduceRequest.builder(new ProduceRequestData()
        .setTopicData(new ProduceRequestData.TopicProduceDataCollection(
          Collections.singletonList(new ProduceRequestData.TopicProduceData()
            .setName(tp.topic).setPartitionData(Collections.singletonList(
            new ProduceRequestData.PartitionProduceData()
              .setIndex(tp.partition)
              .setRecords(MemoryRecords.withRecords(Compression.NONE, new SimpleRecord("test".getBytes))))))
            .iterator))
        .setAcks(1.toShort)
        .setTimeoutMs(5000))
        .build(version.toShort)
      val request = buildRequest(produceRequest)

      when(replicaManager.handleProduceAppend(anyLong,
        anyShort,
        ArgumentMatchers.eq(false),
        any(),
        any(),
        responseCallback.capture(),
        any(),
        any(),
        any(),
        any())
      ).thenAnswer(_ => responseCallback.getValue.apply(Map(tp -> new PartitionResponse(Errors.NOT_LEADER_OR_FOLLOWER))))

      when(replicaManager.getPartitionOrError(tp)).thenAnswer(_ => Left(Errors.UNKNOWN_TOPIC_OR_PARTITION))

      when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
        any[Long])).thenReturn(0)
      when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
        any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
      kafkaApis = createKafkaApis()
      kafkaApis.handleProduceRequest(request, RequestLocal.withThreadConfinedCaching)

      val response = verifyNoThrottling[ProduceResponse](request)

      assertEquals(1, response.data.responses.size)
      val topicProduceResponse = response.data.responses.asScala.head
      assertEquals(1, topicProduceResponse.partitionResponses.size)
      val partitionProduceResponse = topicProduceResponse.partitionResponses.asScala.head
      assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, Errors.forCode(partitionProduceResponse.errorCode))
      // LeaderId and epoch should be the same values inserted into the metadata cache
      assertEquals(0, partitionProduceResponse.currentLeader.leaderId())
      assertEquals(1, partitionProduceResponse.currentLeader.leaderEpoch())
      assertEquals(1, response.data.nodeEndpoints.size)
      val node = response.data.nodeEndpoints.asScala.head
      assertEquals(0, node.nodeId)
      assertEquals("broker0", node.host)
    }
  }

  @Test
  def testProduceResponseMetadataLookupErrorOnNotLeaderOrFollower(): Unit = {
    val topic = "topic"
    metadataCache = mock(classOf[KRaftMetadataCache])

    for (version <- 10 to ApiKeys.PRODUCE.latestVersion) {

      reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, txnCoordinator)

      val responseCallback: ArgumentCaptor[Map[TopicPartition, PartitionResponse] => Unit] = ArgumentCaptor.forClass(classOf[Map[TopicPartition, PartitionResponse] => Unit])

      val tp = new TopicPartition(topic, 0)

      val produceRequest = ProduceRequest.builder(new ProduceRequestData()
        .setTopicData(new ProduceRequestData.TopicProduceDataCollection(
          Collections.singletonList(new ProduceRequestData.TopicProduceData()
            .setName(tp.topic).setPartitionData(Collections.singletonList(
            new ProduceRequestData.PartitionProduceData()
              .setIndex(tp.partition)
              .setRecords(MemoryRecords.withRecords(Compression.NONE, new SimpleRecord("test".getBytes))))))
            .iterator))
        .setAcks(1.toShort)
        .setTimeoutMs(5000))
        .build(version.toShort)
      val request = buildRequest(produceRequest)

      when(replicaManager.handleProduceAppend(anyLong,
        anyShort,
        ArgumentMatchers.eq(false),
        any(),
        any(),
        responseCallback.capture(),
        any(),
        any(),
        any(),
        any())
      ).thenAnswer(_ => responseCallback.getValue.apply(Map(tp -> new PartitionResponse(Errors.NOT_LEADER_OR_FOLLOWER))))

      when(replicaManager.getPartitionOrError(tp)).thenAnswer(_ => Left(Errors.UNKNOWN_TOPIC_OR_PARTITION))

      when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
        any[Long])).thenReturn(0)
      when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
        any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)
      when(metadataCache.contains(tp)).thenAnswer(_ => true)
      when(metadataCache.getLeaderAndIsr(tp.topic(), tp.partition())).thenAnswer(_ => Option.empty)
      when(metadataCache.getAliveBrokerNode(any(), any())).thenReturn(Option.empty)
      kafkaApis = createKafkaApis()
      kafkaApis.handleProduceRequest(request, RequestLocal.withThreadConfinedCaching)

      val response = verifyNoThrottling[ProduceResponse](request)

      assertEquals(1, response.data.responses.size)
      val topicProduceResponse = response.data.responses.asScala.head
      assertEquals(1, topicProduceResponse.partitionResponses.size)
      val partitionProduceResponse = topicProduceResponse.partitionResponses.asScala.head
      assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, Errors.forCode(partitionProduceResponse.errorCode))
      assertEquals(-1, partitionProduceResponse.currentLeader.leaderId())
      assertEquals(-1, partitionProduceResponse.currentLeader.leaderEpoch())
      assertEquals(0, response.data.nodeEndpoints.size)
    }
  }

  @Test
  def testTransactionalParametersSetCorrectly(): Unit = {
    val topic = "topic"
    val transactionalId = "txn1"

    addTopicToMetadataCache(topic, numPartitions = 2)

    for (version <- ApiKeys.PRODUCE.oldestVersion to ApiKeys.PRODUCE.latestVersion) {

      reset(replicaManager, clientQuotaManager, clientRequestQuotaManager, requestChannel, txnCoordinator)

      val tp = new TopicPartition("topic", 0)

      val produceRequest = ProduceRequest.builder(new ProduceRequestData()
        .setTopicData(new ProduceRequestData.TopicProduceDataCollection(
          Collections.singletonList(new ProduceRequestData.TopicProduceData()
            .setName(tp.topic).setPartitionData(Collections.singletonList(
            new ProduceRequestData.PartitionProduceData()
              .setIndex(tp.partition)
              .setRecords(MemoryRecords.withTransactionalRecords(Compression.NONE, 0, 0, 0, new SimpleRecord("test".getBytes))))))
            .iterator))
        .setAcks(1.toShort)
        .setTransactionalId(transactionalId)
        .setTimeoutMs(5000))
        .build(version.toShort)
      val request = buildRequest(produceRequest)

      val kafkaApis = createKafkaApis()
      try {
        kafkaApis.handleProduceRequest(request, RequestLocal.withThreadConfinedCaching)

        verify(replicaManager).handleProduceAppend(anyLong,
          anyShort,
          ArgumentMatchers.eq(false),
          ArgumentMatchers.eq(transactionalId),
          any(),
          any(),
          any(),
          any(),
          any(),
          any())
      } finally {
        kafkaApis.close()
      }
    }
  }

  @Test
  def testAddPartitionsToTxnWithInvalidPartition(): Unit = {
    val topic = "topic"
    addTopicToMetadataCache(topic, numPartitions = 1)

    def checkInvalidPartition(invalidPartitionId: Int): Unit = {
      reset(replicaManager, clientRequestQuotaManager, requestChannel)

      val invalidTopicPartition = new TopicPartition(topic, invalidPartitionId)
      val addPartitionsToTxnRequest = AddPartitionsToTxnRequest.Builder.forClient(
        "txnlId", 15L, 0.toShort, List(invalidTopicPartition).asJava
      ).build()
      val request = buildRequest(addPartitionsToTxnRequest)

      when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
        any[Long])).thenReturn(0)
      val kafkaApis = createKafkaApis()
      try {
        kafkaApis.handleAddPartitionsToTxnRequest(request, RequestLocal.withThreadConfinedCaching)

        val response = verifyNoThrottling[AddPartitionsToTxnResponse](request)
        assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, response.errors().get(AddPartitionsToTxnResponse.V3_AND_BELOW_TXN_ID).get(invalidTopicPartition))
      } finally {
        kafkaApis.close()
      }
    }

    checkInvalidPartition(-1)
    checkInvalidPartition(1) // topic has only one partition
  }

  @Test
  def testAddPartitionsToTxnWithInklessTopic(): Unit = {
    val topic = "topic"
    val topicPartition = new TopicPartition(topic, 0)
    addTopicToMetadataCache(topic, numPartitions = 1)

    val addPartitionsToTxnRequest = AddPartitionsToTxnRequest.Builder.forClient(
      "txnlId", 15L, 0.toShort, List(topicPartition).asJava
    ).build()
    val request = buildRequest(addPartitionsToTxnRequest)

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    val kafkaApis = createKafkaApis(inklessSharedState = Some(createInklessSharedStateWithTopic(topic)))
    try {
      kafkaApis.handleAddPartitionsToTxnRequest(request, RequestLocal.withThreadConfinedCaching)

      val response = verifyNoThrottling[AddPartitionsToTxnResponse](request)
      println(response)
      assertEquals(Errors.INVALID_TOPIC_EXCEPTION, response.errors().get(AddPartitionsToTxnResponse.V3_AND_BELOW_TXN_ID).get(topicPartition))
    } finally {
      kafkaApis.close()
    }
  }

  @Test
  def requiredAclsNotPresentWriteTxnMarkersThrowsAuthorizationException(): Unit = {
    val topicPartition = new TopicPartition("t", 0)
    val (_, request) = createWriteTxnMarkersRequest(asList(topicPartition))

    val authorizer: Authorizer = mock(classOf[Authorizer])
    val clusterResource = new ResourcePattern(ResourceType.CLUSTER, Resource.CLUSTER_NAME, PatternType.LITERAL)
    val alterActions = Collections.singletonList(new Action(AclOperation.ALTER, clusterResource, 1, true, false))
    val clusterActions = Collections.singletonList(new Action(AclOperation.CLUSTER_ACTION, clusterResource, 1, true, true))
    val deniedList = Collections.singletonList(AuthorizationResult.DENIED)
    when(authorizer.authorize(
      request.context,
      alterActions
    )).thenReturn(deniedList)
    when(authorizer.authorize(
      request.context,
      clusterActions
    )).thenReturn(deniedList)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))

    assertThrows(classOf[ClusterAuthorizationException],
      () => kafkaApis.handleWriteTxnMarkersRequest(request, RequestLocal.withThreadConfinedCaching))
  }

  @Test
  def shouldRespondWithUnknownTopicWhenPartitionIsNotHosted(): Unit = {
    val topicPartition = new TopicPartition("t", 0)
    val (_, request) = createWriteTxnMarkersRequest(asList(topicPartition))
    val expectedErrors = Map(topicPartition -> Errors.UNKNOWN_TOPIC_OR_PARTITION).asJava
    val capturedResponse: ArgumentCaptor[WriteTxnMarkersResponse] = ArgumentCaptor.forClass(classOf[WriteTxnMarkersResponse])

    when(replicaManager.onlinePartition(topicPartition))
      .thenReturn(None)
    kafkaApis = createKafkaApis()
    kafkaApis.handleWriteTxnMarkersRequest(request, RequestLocal.withThreadConfinedCaching)

    verify(requestChannel).sendResponse(
      ArgumentMatchers.eq(request),
      capturedResponse.capture(),
      ArgumentMatchers.eq(None)
    )
    val markersResponse = capturedResponse.getValue
    assertEquals(expectedErrors, markersResponse.errorsByProducerId.get(1L))
  }

  @Test
  def testWriteTxnMarkersShouldAllBeIncludedInTheResponse(): Unit = {
    // This test verifies the response will not be sent prematurely because of calling replicaManager append
    // with no records.
    val topicPartition = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)
    val writeTxnMarkersRequest = new WriteTxnMarkersRequest.Builder(
      asList(
        new TxnMarkerEntry(1, 1.toShort, 0, TransactionResult.COMMIT, asList(topicPartition)),
        new TxnMarkerEntry(2, 1.toShort, 0, TransactionResult.COMMIT, asList(topicPartition)),
      )).build()
    val request = buildRequest(writeTxnMarkersRequest)
    val capturedResponse: ArgumentCaptor[WriteTxnMarkersResponse] = ArgumentCaptor.forClass(classOf[WriteTxnMarkersResponse])

    when(replicaManager.onlinePartition(any()))
      .thenReturn(Some(mock(classOf[Partition])))
    when(groupCoordinator.isNewGroupCoordinator)
      .thenReturn(true)
    when(groupCoordinator.completeTransaction(
      ArgumentMatchers.eq(topicPartition),
      any(),
      ArgumentMatchers.eq(1.toShort),
      ArgumentMatchers.eq(0),
      ArgumentMatchers.eq(TransactionResult.COMMIT),
      any()
    )).thenReturn(CompletableFuture.completedFuture[Void](null))

    kafkaApis = createKafkaApis()
    kafkaApis.handleWriteTxnMarkersRequest(request, RequestLocal.withThreadConfinedCaching)

    verify(requestChannel).sendResponse(
      ArgumentMatchers.eq(request),
      capturedResponse.capture(),
      ArgumentMatchers.eq(None)
    )
    val markersResponse = capturedResponse.getValue
    assertEquals(2, markersResponse.errorsByProducerId.size())
  }

  @Test
  def shouldRespondWithUnknownTopicOrPartitionForBadPartitionAndNoErrorsForGoodPartition(): Unit = {
    val tp1 = new TopicPartition("t", 0)
    val tp2 = new TopicPartition("t1", 0)
    val (_, request) = createWriteTxnMarkersRequest(asList(tp1, tp2))
    val expectedErrors = Map(tp1 -> Errors.UNKNOWN_TOPIC_OR_PARTITION, tp2 -> Errors.NONE).asJava

    val capturedResponse: ArgumentCaptor[WriteTxnMarkersResponse] = ArgumentCaptor.forClass(classOf[WriteTxnMarkersResponse])
    val responseCallback: ArgumentCaptor[Map[TopicPartition, PartitionResponse] => Unit] = ArgumentCaptor.forClass(classOf[Map[TopicPartition, PartitionResponse] => Unit])

    when(replicaManager.onlinePartition(tp1))
      .thenReturn(None)
    when(replicaManager.onlinePartition(tp2))
      .thenReturn(Some(mock(classOf[Partition])))

    val requestLocal = RequestLocal.withThreadConfinedCaching
    when(replicaManager.appendRecords(anyLong,
      anyShort,
      ArgumentMatchers.eq(true),
      ArgumentMatchers.eq(AppendOrigin.COORDINATOR),
      any(),
      responseCallback.capture(),
      any(),
      any(),
      ArgumentMatchers.eq(requestLocal),
      any(),
      any()
    )).thenAnswer(_ => responseCallback.getValue.apply(Map(tp2 -> new PartitionResponse(Errors.NONE))))
    kafkaApis = createKafkaApis()
    kafkaApis.handleWriteTxnMarkersRequest(request, requestLocal)
    verify(requestChannel).sendResponse(
      ArgumentMatchers.eq(request),
      capturedResponse.capture(),
      ArgumentMatchers.eq(None)
    )

    val markersResponse = capturedResponse.getValue
    assertEquals(expectedErrors, markersResponse.errorsByProducerId.get(1L))
  }

  @ParameterizedTest
  @ValueSource(strings = Array("ALTER", "CLUSTER_ACTION"))
  def shouldAppendToLogOnWriteTxnMarkersWhenCorrectMagicVersion(allowedAclOperation: String): Unit = {
    val topicPartition = new TopicPartition("t", 0)
    val request = createWriteTxnMarkersRequest(asList(topicPartition))._2
    when(replicaManager.onlinePartition(topicPartition))
      .thenReturn(Some(mock(classOf[Partition])))

    val requestLocal = RequestLocal.withThreadConfinedCaching

    // Allowing WriteTxnMarkers API with the help of allowedAclOperation parameter.
    val authorizer: Authorizer = mock(classOf[Authorizer])
    val clusterResource = new ResourcePattern(ResourceType.CLUSTER, Resource.CLUSTER_NAME, PatternType.LITERAL)
    val allowedAction = Collections.singletonList(new Action(
      AclOperation.fromString(allowedAclOperation),
      clusterResource,
      1,
      true,
      allowedAclOperation.equals("CLUSTER_ACTION")
    ))
    val deniedList = Collections.singletonList(AuthorizationResult.DENIED)
    val allowedList = Collections.singletonList(AuthorizationResult.ALLOWED)
    when(authorizer.authorize(
      ArgumentMatchers.eq(request.context),
      any()
    )).thenReturn(deniedList)
    when(authorizer.authorize(
      request.context,
      allowedAction
    )).thenReturn(allowedList)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))

    kafkaApis.handleWriteTxnMarkersRequest(request, requestLocal)
    verify(replicaManager).appendRecords(anyLong,
      anyShort,
      ArgumentMatchers.eq(true),
      ArgumentMatchers.eq(AppendOrigin.COORDINATOR),
      any(),
      any(),
      any(),
      any(),
      ArgumentMatchers.eq(requestLocal),
      any(),
      any())
  }

  @Test
  def testHandleWriteTxnMarkersRequestWithOldGroupCoordinator(): Unit = {
    val offset0 = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)
    val offset1 = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 1)
    val foo0 = new TopicPartition("foo", 0)
    val foo1 = new TopicPartition("foo", 1)

    val allPartitions = List(
      offset0,
      offset1,
      foo0,
      foo1
    )

    val writeTxnMarkersRequest = new WriteTxnMarkersRequest.Builder(
      List(
        new TxnMarkerEntry(
          1L,
          1.toShort,
          0,
          TransactionResult.COMMIT,
          List(offset0, foo0).asJava
        ),
        new TxnMarkerEntry(
          2L,
          1.toShort,
          0,
          TransactionResult.ABORT,
          List(offset1, foo1).asJava
        )
      ).asJava
    ).build()

    val requestChannelRequest = buildRequest(writeTxnMarkersRequest)

    allPartitions.foreach { tp =>
      when(replicaManager.onlinePartition(tp))
        .thenReturn(Some(mock(classOf[Partition])))
    }

    when(groupCoordinator.onTransactionCompleted(
      ArgumentMatchers.eq(1L),
      ArgumentMatchers.any(),
      ArgumentMatchers.eq(TransactionResult.COMMIT)
    )).thenReturn(CompletableFuture.completedFuture[Void](null))

    when(groupCoordinator.onTransactionCompleted(
      ArgumentMatchers.eq(2L),
      ArgumentMatchers.any(),
      ArgumentMatchers.eq(TransactionResult.ABORT)
    )).thenReturn(FutureUtils.failedFuture[Void](Errors.NOT_CONTROLLER.exception))

    val entriesPerPartition: ArgumentCaptor[Map[TopicPartition, MemoryRecords]] =
      ArgumentCaptor.forClass(classOf[Map[TopicPartition, MemoryRecords]])
    val responseCallback: ArgumentCaptor[Map[TopicPartition, PartitionResponse] => Unit] =
      ArgumentCaptor.forClass(classOf[Map[TopicPartition, PartitionResponse] => Unit])

    when(replicaManager.appendRecords(
      ArgumentMatchers.eq(ServerConfigs.REQUEST_TIMEOUT_MS_DEFAULT.toLong),
      ArgumentMatchers.eq(-1),
      ArgumentMatchers.eq(true),
      ArgumentMatchers.eq(AppendOrigin.COORDINATOR),
      entriesPerPartition.capture(),
      responseCallback.capture(),
      any(),
      any(),
      ArgumentMatchers.eq(RequestLocal.noCaching()),
      any(),
      any()
    )).thenAnswer { _ =>
      responseCallback.getValue.apply(
        entriesPerPartition.getValue.keySet.map { tp =>
          tp -> new PartitionResponse(Errors.NONE)
        }.toMap
      )
    }
    kafkaApis = createKafkaApis(overrideProperties = Map(
      GroupCoordinatorConfig.NEW_GROUP_COORDINATOR_ENABLE_CONFIG -> "false"
    ))
    kafkaApis.handleWriteTxnMarkersRequest(requestChannelRequest, RequestLocal.noCaching())

    val expectedResponse = new WriteTxnMarkersResponseData()
      .setMarkers(List(
        new WriteTxnMarkersResponseData.WritableTxnMarkerResult()
          .setProducerId(1L)
          .setTopics(List(
            new WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult()
              .setName(Topic.GROUP_METADATA_TOPIC_NAME)
              .setPartitions(List(
                new WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult()
                  .setPartitionIndex(0)
                  .setErrorCode(Errors.NONE.code)
              ).asJava),
            new WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult()
              .setName("foo")
              .setPartitions(List(
                new WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult()
                  .setPartitionIndex(0)
                  .setErrorCode(Errors.NONE.code)
              ).asJava)
          ).asJava),
        new WriteTxnMarkersResponseData.WritableTxnMarkerResult()
          .setProducerId(2L)
          .setTopics(List(
            new WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult()
              .setName(Topic.GROUP_METADATA_TOPIC_NAME)
              .setPartitions(List(
                new WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult()
                  .setPartitionIndex(1)
                  .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code)
              ).asJava),
            new WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult()
              .setName("foo")
              .setPartitions(List(
                new WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult()
                  .setPartitionIndex(1)
                  .setErrorCode(Errors.NONE.code)
              ).asJava)
          ).asJava)
      ).asJava)

    val response = verifyNoThrottling[WriteTxnMarkersResponse](requestChannelRequest)
    assertEquals(normalize(expectedResponse), normalize(response.data))
  }

  @Test
  def testHandleWriteTxnMarkersRequestWithNewGroupCoordinator(): Unit = {
    val offset0 = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)
    val offset1 = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 1)
    val foo0 = new TopicPartition("foo", 0)
    val foo1 = new TopicPartition("foo", 1)

    val allPartitions = List(
      offset0,
      offset1,
      foo0,
      foo1
    )

    val writeTxnMarkersRequest = new WriteTxnMarkersRequest.Builder(
      List(
        new TxnMarkerEntry(
          1L,
          1.toShort,
          0,
          TransactionResult.COMMIT,
          List(offset0, foo0).asJava
        ),
        new TxnMarkerEntry(
          2L,
          1.toShort,
          0,
          TransactionResult.ABORT,
          List(offset1, foo1).asJava
        )
      ).asJava
    ).build()

    val requestChannelRequest = buildRequest(writeTxnMarkersRequest)

    allPartitions.foreach { tp =>
      when(replicaManager.onlinePartition(tp))
        .thenReturn(Some(mock(classOf[Partition])))
    }

    when(groupCoordinator.completeTransaction(
      ArgumentMatchers.eq(offset0),
      ArgumentMatchers.eq(1L),
      ArgumentMatchers.eq(1.toShort),
      ArgumentMatchers.eq(0),
      ArgumentMatchers.eq(TransactionResult.COMMIT),
      ArgumentMatchers.eq(Duration.ofMillis(ServerConfigs.REQUEST_TIMEOUT_MS_DEFAULT))
    )).thenReturn(CompletableFuture.completedFuture[Void](null))

    when(groupCoordinator.completeTransaction(
      ArgumentMatchers.eq(offset1),
      ArgumentMatchers.eq(2L),
      ArgumentMatchers.eq(1.toShort),
      ArgumentMatchers.eq(0),
      ArgumentMatchers.eq(TransactionResult.ABORT),
      ArgumentMatchers.eq(Duration.ofMillis(ServerConfigs.REQUEST_TIMEOUT_MS_DEFAULT))
    )).thenReturn(CompletableFuture.completedFuture[Void](null))

    val entriesPerPartition: ArgumentCaptor[Map[TopicPartition, MemoryRecords]] =
      ArgumentCaptor.forClass(classOf[Map[TopicPartition, MemoryRecords]])
    val responseCallback: ArgumentCaptor[Map[TopicPartition, PartitionResponse] => Unit] =
      ArgumentCaptor.forClass(classOf[Map[TopicPartition, PartitionResponse] => Unit])

    when(replicaManager.appendRecords(
      ArgumentMatchers.eq(ServerConfigs.REQUEST_TIMEOUT_MS_DEFAULT.toLong),
      ArgumentMatchers.eq(-1),
      ArgumentMatchers.eq(true),
      ArgumentMatchers.eq(AppendOrigin.COORDINATOR),
      entriesPerPartition.capture(),
      responseCallback.capture(),
      any(),
      any(),
      ArgumentMatchers.eq(RequestLocal.noCaching),
      any(),
      any()
    )).thenAnswer { _ =>
      responseCallback.getValue.apply(
        entriesPerPartition.getValue.keySet.map { tp =>
          tp -> new PartitionResponse(Errors.NONE)
        }.toMap
      )
    }
    kafkaApis = createKafkaApis()
    kafkaApis.handleWriteTxnMarkersRequest(requestChannelRequest, RequestLocal.noCaching)

    val expectedResponse = new WriteTxnMarkersResponseData()
      .setMarkers(List(
        new WriteTxnMarkersResponseData.WritableTxnMarkerResult()
          .setProducerId(1L)
          .setTopics(List(
            new WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult()
              .setName(Topic.GROUP_METADATA_TOPIC_NAME)
              .setPartitions(List(
                new WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult()
                  .setPartitionIndex(0)
                  .setErrorCode(Errors.NONE.code)
              ).asJava),
            new WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult()
              .setName("foo")
              .setPartitions(List(
                new WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult()
                  .setPartitionIndex(0)
                  .setErrorCode(Errors.NONE.code)
              ).asJava)
          ).asJava),
        new WriteTxnMarkersResponseData.WritableTxnMarkerResult()
          .setProducerId(2L)
          .setTopics(List(
            new WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult()
              .setName(Topic.GROUP_METADATA_TOPIC_NAME)
              .setPartitions(List(
                new WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult()
                  .setPartitionIndex(1)
                  .setErrorCode(Errors.NONE.code)
              ).asJava),
            new WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult()
              .setName("foo")
              .setPartitions(List(
                new WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult()
                  .setPartitionIndex(1)
                  .setErrorCode(Errors.NONE.code)
              ).asJava)
          ).asJava)
      ).asJava)

    val response = verifyNoThrottling[WriteTxnMarkersResponse](requestChannelRequest)
    assertEquals(normalize(expectedResponse), normalize(response.data))
  }

  @ParameterizedTest
  @EnumSource(value = classOf[Errors], names = Array(
    "COORDINATOR_NOT_AVAILABLE",
    "COORDINATOR_LOAD_IN_PROGRESS",
    "NOT_COORDINATOR",
    "REQUEST_TIMED_OUT"
  ))
  def testHandleWriteTxnMarkersRequestWithNewGroupCoordinatorErrorTranslation(error: Errors): Unit = {
    val offset0 = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0)

    val writeTxnMarkersRequest = new WriteTxnMarkersRequest.Builder(
      List(
        new TxnMarkerEntry(
          1L,
          1.toShort,
          0,
          TransactionResult.COMMIT,
          List(offset0).asJava
        )
      ).asJava
    ).build()

    val requestChannelRequest = buildRequest(writeTxnMarkersRequest)

    when(replicaManager.onlinePartition(offset0))
      .thenReturn(Some(mock(classOf[Partition])))

    when(groupCoordinator.completeTransaction(
      ArgumentMatchers.eq(offset0),
      ArgumentMatchers.eq(1L),
      ArgumentMatchers.eq(1.toShort),
      ArgumentMatchers.eq(0),
      ArgumentMatchers.eq(TransactionResult.COMMIT),
      ArgumentMatchers.eq(Duration.ofMillis(ServerConfigs.REQUEST_TIMEOUT_MS_DEFAULT))
    )).thenReturn(FutureUtils.failedFuture[Void](error.exception()))
    kafkaApis = createKafkaApis()
    kafkaApis.handleWriteTxnMarkersRequest(requestChannelRequest, RequestLocal.noCaching)

    val expectedError = error match {
      case Errors.COORDINATOR_NOT_AVAILABLE | Errors.COORDINATOR_LOAD_IN_PROGRESS | Errors.NOT_COORDINATOR =>
        Errors.NOT_LEADER_OR_FOLLOWER
      case error =>
        error
    }

    val expectedResponse = new WriteTxnMarkersResponseData()
      .setMarkers(List(
        new WriteTxnMarkersResponseData.WritableTxnMarkerResult()
          .setProducerId(1L)
          .setTopics(List(
            new WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult()
              .setName(Topic.GROUP_METADATA_TOPIC_NAME)
              .setPartitions(List(
                new WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult()
                  .setPartitionIndex(0)
                  .setErrorCode(expectedError.code)
              ).asJava)
          ).asJava)
      ).asJava)

    val response = verifyNoThrottling[WriteTxnMarkersResponse](requestChannelRequest)
    assertEquals(normalize(expectedResponse), normalize(response.data))
  }

  private def normalize(
    response: WriteTxnMarkersResponseData
  ): WriteTxnMarkersResponseData = {
    val copy = response.duplicate()
    copy.markers.sort(
      Comparator.comparingLong[WriteTxnMarkersResponseData.WritableTxnMarkerResult](_.producerId)
    )
    copy.markers.forEach { marker =>
      marker.topics.sort((t1, t2) => t1.name.compareTo(t2.name))
      marker.topics.forEach { topic =>
        topic.partitions.sort(
          Comparator.comparingInt[WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult](_.partitionIndex)
        )
      }
    }
    copy
  }

  @Test
  def testHandleWriteTxnMarkersRequestWithInklessTopic(): Unit = {
    val topic = "topic"
    val topicPartition = new TopicPartition(topic, 0)
    val (_, request) = createWriteTxnMarkersRequest(asList(topicPartition))
    val expectedErrors = Map(topicPartition -> Errors.INVALID_TOPIC_EXCEPTION).asJava
    val capturedResponse: ArgumentCaptor[WriteTxnMarkersResponse] = ArgumentCaptor.forClass(classOf[WriteTxnMarkersResponse])

    val kafkaApis = createKafkaApis(inklessSharedState = Some(createInklessSharedStateWithTopic(topic)))
    kafkaApis.handleWriteTxnMarkersRequest(request, RequestLocal.withThreadConfinedCaching)

    verify(requestChannel).sendResponse(
      ArgumentMatchers.eq(request),
      capturedResponse.capture(),
      ArgumentMatchers.eq(None)
    )
    val markersResponse = capturedResponse.getValue
    assertEquals(expectedErrors, markersResponse.errorsByProducerId.get(1L))
  }

  private def createInklessSharedStateWithTopic(inklessTopic: String): SharedState = {
    val metadataView = mock(classOf[MetadataView])
    when(metadataView.isInklessTopic(ArgumentMatchers.eq(inklessTopic))).thenReturn(true)
    val sharedState = mock(classOf[SharedState])
    when(sharedState.metadata()).thenReturn(metadataView)
    sharedState
  }

  @Test
  def testLeaderReplicaIfLocalRaisesFencedLeaderEpoch(): Unit = {
    testListOffsetFailedGetLeaderReplica(Errors.FENCED_LEADER_EPOCH)
  }

  @Test
  def testLeaderReplicaIfLocalRaisesUnknownLeaderEpoch(): Unit = {
    testListOffsetFailedGetLeaderReplica(Errors.UNKNOWN_LEADER_EPOCH)
  }

  @Test
  def testLeaderReplicaIfLocalRaisesNotLeaderOrFollower(): Unit = {
    testListOffsetFailedGetLeaderReplica(Errors.NOT_LEADER_OR_FOLLOWER)
  }

  @Test
  def testLeaderReplicaIfLocalRaisesUnknownTopicOrPartition(): Unit = {
    testListOffsetFailedGetLeaderReplica(Errors.UNKNOWN_TOPIC_OR_PARTITION)
  }

  @Test
  def testHandleDeleteGroups(): Unit = {
    val deleteGroupsRequest = new DeleteGroupsRequestData().setGroupsNames(List(
      "group-1",
      "group-2",
      "group-3"
    ).asJava)

    val requestChannelRequest = buildRequest(new DeleteGroupsRequest.Builder(deleteGroupsRequest).build())

    val future = new CompletableFuture[DeleteGroupsResponseData.DeletableGroupResultCollection]()
    when(groupCoordinator.deleteGroups(
      requestChannelRequest.context,
      List("group-1", "group-2", "group-3").asJava,
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleDeleteGroupsRequest(
      requestChannelRequest,
      RequestLocal.noCaching
    )

    val results = new DeleteGroupsResponseData.DeletableGroupResultCollection(List(
      new DeleteGroupsResponseData.DeletableGroupResult()
        .setGroupId("group-1")
        .setErrorCode(Errors.NONE.code),
      new DeleteGroupsResponseData.DeletableGroupResult()
        .setGroupId("group-2")
        .setErrorCode(Errors.NOT_CONTROLLER.code),
      new DeleteGroupsResponseData.DeletableGroupResult()
        .setGroupId("group-3")
        .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code),
    ).iterator.asJava)

    future.complete(results)

    val expectedDeleteGroupsResponse = new DeleteGroupsResponseData()
      .setResults(results)

    val response = verifyNoThrottling[DeleteGroupsResponse](requestChannelRequest)
    assertEquals(expectedDeleteGroupsResponse, response.data)
  }

  @Test
  def testHandleDeleteGroupsFutureFailed(): Unit = {
    val deleteGroupsRequest = new DeleteGroupsRequestData().setGroupsNames(List(
      "group-1",
      "group-2",
      "group-3"
    ).asJava)

    val requestChannelRequest = buildRequest(new DeleteGroupsRequest.Builder(deleteGroupsRequest).build())

    val future = new CompletableFuture[DeleteGroupsResponseData.DeletableGroupResultCollection]()
    when(groupCoordinator.deleteGroups(
      requestChannelRequest.context,
      List("group-1", "group-2", "group-3").asJava,
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleDeleteGroupsRequest(
      requestChannelRequest,
      RequestLocal.noCaching
    )

    future.completeExceptionally(Errors.NOT_CONTROLLER.exception)

    val expectedDeleteGroupsResponse = new DeleteGroupsResponseData()
      .setResults(new DeleteGroupsResponseData.DeletableGroupResultCollection(List(
        new DeleteGroupsResponseData.DeletableGroupResult()
          .setGroupId("group-1")
          .setErrorCode(Errors.NOT_CONTROLLER.code),
        new DeleteGroupsResponseData.DeletableGroupResult()
          .setGroupId("group-2")
          .setErrorCode(Errors.NOT_CONTROLLER.code),
        new DeleteGroupsResponseData.DeletableGroupResult()
          .setGroupId("group-3")
          .setErrorCode(Errors.NOT_CONTROLLER.code),
      ).iterator.asJava))

    val response = verifyNoThrottling[DeleteGroupsResponse](requestChannelRequest)
    assertEquals(expectedDeleteGroupsResponse, response.data)
  }

  @Test
  def testHandleDeleteGroupsAuthenticationFailed(): Unit = {
    val deleteGroupsRequest = new DeleteGroupsRequestData().setGroupsNames(List(
      "group-1",
      "group-2",
      "group-3"
    ).asJava)

    val requestChannelRequest = buildRequest(new DeleteGroupsRequest.Builder(deleteGroupsRequest).build())

    val authorizer: Authorizer = mock(classOf[Authorizer])

    val acls = Map(
      "group-1" -> AuthorizationResult.DENIED,
      "group-2" -> AuthorizationResult.ALLOWED,
      "group-3" -> AuthorizationResult.ALLOWED
    )

    when(authorizer.authorize(
      any[RequestContext],
      any[util.List[Action]]
    )).thenAnswer { invocation =>
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      actions.asScala.map { action =>
        acls.getOrElse(action.resourcePattern.name, AuthorizationResult.DENIED)
      }.asJava
    }

    val future = new CompletableFuture[DeleteGroupsResponseData.DeletableGroupResultCollection]()
    when(groupCoordinator.deleteGroups(
      requestChannelRequest.context,
      List("group-2", "group-3").asJava,
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleDeleteGroupsRequest(
      requestChannelRequest,
      RequestLocal.noCaching
    )

    future.complete(new DeleteGroupsResponseData.DeletableGroupResultCollection(List(
      new DeleteGroupsResponseData.DeletableGroupResult()
        .setGroupId("group-2")
        .setErrorCode(Errors.NONE.code),
      new DeleteGroupsResponseData.DeletableGroupResult()
        .setGroupId("group-3")
        .setErrorCode(Errors.NONE.code)
    ).iterator.asJava))

    val expectedDeleteGroupsResponse = new DeleteGroupsResponseData()
      .setResults(new DeleteGroupsResponseData.DeletableGroupResultCollection(List(
        new DeleteGroupsResponseData.DeletableGroupResult()
          .setGroupId("group-2")
          .setErrorCode(Errors.NONE.code),
        new DeleteGroupsResponseData.DeletableGroupResult()
          .setGroupId("group-3")
          .setErrorCode(Errors.NONE.code),
        new DeleteGroupsResponseData.DeletableGroupResult()
          .setGroupId("group-1")
          .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code)).iterator.asJava))

    val response = verifyNoThrottling[DeleteGroupsResponse](requestChannelRequest)
    assertEquals(expectedDeleteGroupsResponse, response.data)
  }

  @Test
  def testHandleDescribeGroups(): Unit = {
    val describeGroupsRequest = new DescribeGroupsRequestData().setGroups(List(
      "group-1",
      "group-2",
      "group-3",
      "group-4"
    ).asJava)

    val requestChannelRequest = buildRequest(new DescribeGroupsRequest.Builder(describeGroupsRequest).build())

    val future = new CompletableFuture[util.List[DescribeGroupsResponseData.DescribedGroup]]()
    when(groupCoordinator.describeGroups(
      requestChannelRequest.context,
      describeGroupsRequest.groups
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleDescribeGroupsRequest(requestChannelRequest)

    val groupResults = List(
      new DescribeGroupsResponseData.DescribedGroup()
        .setGroupId("group-1")
        .setProtocolType("consumer")
        .setProtocolData("range")
        .setGroupState("Stable")
        .setMembers(List(
          new DescribeGroupsResponseData.DescribedGroupMember()
            .setMemberId("member-1")).asJava),
      new DescribeGroupsResponseData.DescribedGroup()
        .setGroupId("group-2")
        .setErrorCode(Errors.NOT_COORDINATOR.code),
      new DescribeGroupsResponseData.DescribedGroup()
        .setGroupId("group-3")
        .setErrorCode(Errors.REQUEST_TIMED_OUT.code),
      new DescribeGroupsResponseData.DescribedGroup()
        .setGroupId("group-4")
        .setGroupState("Dead")
        .setErrorCode(Errors.GROUP_ID_NOT_FOUND.code)
        .setErrorMessage("Group group-4 is not a classic group.")
    ).asJava

    future.complete(groupResults)

    val expectedDescribeGroupsResponse = new DescribeGroupsResponseData().setGroups(groupResults)
    val response = verifyNoThrottling[DescribeGroupsResponse](requestChannelRequest)
    assertEquals(expectedDescribeGroupsResponse, response.data)
  }

  @Test
  def testHandleDescribeGroupsFutureFailed(): Unit = {
    val describeGroupsRequest = new DescribeGroupsRequestData().setGroups(List(
      "group-1",
      "group-2",
      "group-3"
    ).asJava)

    val requestChannelRequest = buildRequest(new DescribeGroupsRequest.Builder(describeGroupsRequest).build())

    val future = new CompletableFuture[util.List[DescribeGroupsResponseData.DescribedGroup]]()
    when(groupCoordinator.describeGroups(
      requestChannelRequest.context,
      describeGroupsRequest.groups
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleDescribeGroupsRequest(requestChannelRequest)

    val expectedDescribeGroupsResponse = new DescribeGroupsResponseData().setGroups(List(
      new DescribeGroupsResponseData.DescribedGroup()
        .setGroupId("group-1")
        .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code),
      new DescribeGroupsResponseData.DescribedGroup()
        .setGroupId("group-2")
        .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code),
      new DescribeGroupsResponseData.DescribedGroup()
        .setGroupId("group-3")
        .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code)
    ).asJava)

    future.completeExceptionally(Errors.UNKNOWN_SERVER_ERROR.exception)

    val response = verifyNoThrottling[DescribeGroupsResponse](requestChannelRequest)
    assertEquals(expectedDescribeGroupsResponse, response.data)
  }

  @Test
  def testHandleDescribeGroupsAuthenticationFailed(): Unit = {
    val describeGroupsRequest = new DescribeGroupsRequestData().setGroups(List(
      "group-1",
      "group-2",
      "group-3"
    ).asJava)

    val requestChannelRequest = buildRequest(new DescribeGroupsRequest.Builder(describeGroupsRequest).build())

    val authorizer: Authorizer = mock(classOf[Authorizer])

    val acls = Map(
      "group-1" -> AuthorizationResult.DENIED,
      "group-2" -> AuthorizationResult.ALLOWED,
      "group-3" -> AuthorizationResult.DENIED
    )

    when(authorizer.authorize(
      any[RequestContext],
      any[util.List[Action]]
    )).thenAnswer { invocation =>
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      actions.asScala.map { action =>
        acls.getOrElse(action.resourcePattern.name, AuthorizationResult.DENIED)
      }.asJava
    }

    val future = new CompletableFuture[util.List[DescribeGroupsResponseData.DescribedGroup]]()
    when(groupCoordinator.describeGroups(
      requestChannelRequest.context,
      List("group-2").asJava
    )).thenReturn(future)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleDescribeGroupsRequest(requestChannelRequest)

    future.complete(List(
      new DescribeGroupsResponseData.DescribedGroup()
        .setGroupId("group-2")
        .setErrorCode(Errors.NOT_COORDINATOR.code)
    ).asJava)

    val expectedDescribeGroupsResponse = new DescribeGroupsResponseData().setGroups(List(
      // group-1 and group-3 are first because unauthorized are put first into the response.
      new DescribeGroupsResponseData.DescribedGroup()
        .setGroupId("group-1")
        .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code),
      new DescribeGroupsResponseData.DescribedGroup()
        .setGroupId("group-3")
        .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code),
      new DescribeGroupsResponseData.DescribedGroup()
        .setGroupId("group-2")
        .setErrorCode(Errors.NOT_COORDINATOR.code)
    ).asJava)

    val response = verifyNoThrottling[DescribeGroupsResponse](requestChannelRequest)
    assertEquals(expectedDescribeGroupsResponse, response.data)
  }

  @Test
  def testOffsetDelete(): Unit = {
    val group = "groupId"
    addTopicToMetadataCache("topic-1", numPartitions = 2)
    addTopicToMetadataCache("topic-2", numPartitions = 2)

    val topics = new OffsetDeleteRequestTopicCollection()
    topics.add(new OffsetDeleteRequestTopic()
      .setName("topic-1")
      .setPartitions(Seq(
        new OffsetDeleteRequestPartition().setPartitionIndex(0),
        new OffsetDeleteRequestPartition().setPartitionIndex(1)).asJava))
    topics.add(new OffsetDeleteRequestTopic()
      .setName("topic-2")
      .setPartitions(Seq(
        new OffsetDeleteRequestPartition().setPartitionIndex(0),
        new OffsetDeleteRequestPartition().setPartitionIndex(1)).asJava))

    val offsetDeleteRequest = new OffsetDeleteRequest.Builder(
      new OffsetDeleteRequestData()
        .setGroupId(group)
        .setTopics(topics)
    ).build()
    val request = buildRequest(offsetDeleteRequest)

    val requestLocal = RequestLocal.withThreadConfinedCaching
    val future = new CompletableFuture[OffsetDeleteResponseData]()
    when(groupCoordinator.deleteOffsets(
      request.context,
      offsetDeleteRequest.data,
      requestLocal.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleOffsetDeleteRequest(request, requestLocal)

    val offsetDeleteResponseData = new OffsetDeleteResponseData()
      .setTopics(new OffsetDeleteResponseData.OffsetDeleteResponseTopicCollection(List(
        new OffsetDeleteResponseData.OffsetDeleteResponseTopic()
          .setName("topic-1")
          .setPartitions(new OffsetDeleteResponseData.OffsetDeleteResponsePartitionCollection(List(
            new OffsetDeleteResponseData.OffsetDeleteResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code),
            new OffsetDeleteResponseData.OffsetDeleteResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.NONE.code)
          ).asJava.iterator)),
        new OffsetDeleteResponseData.OffsetDeleteResponseTopic()
          .setName("topic-2")
          .setPartitions(new OffsetDeleteResponseData.OffsetDeleteResponsePartitionCollection(List(
            new OffsetDeleteResponseData.OffsetDeleteResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code),
            new OffsetDeleteResponseData.OffsetDeleteResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.NONE.code)
          ).asJava.iterator))
      ).asJava.iterator()))

    future.complete(offsetDeleteResponseData)

    val response = verifyNoThrottling[OffsetDeleteResponse](request)
    assertEquals(offsetDeleteResponseData, response.data)
  }

  @Test
  def testOffsetDeleteTopicsAndPartitionsValidation(): Unit = {
    val group = "groupId"
    addTopicToMetadataCache("foo", numPartitions = 2)
    addTopicToMetadataCache("bar", numPartitions = 2)

    val offsetDeleteRequest = new OffsetDeleteRequestData()
      .setGroupId(group)
      .setTopics(new OffsetDeleteRequestTopicCollection(List(
        // foo exists but has only 2 partitions.
        new OffsetDeleteRequestTopic()
          .setName("foo")
          .setPartitions(List(
            new OffsetDeleteRequestPartition().setPartitionIndex(0),
            new OffsetDeleteRequestPartition().setPartitionIndex(1),
            new OffsetDeleteRequestPartition().setPartitionIndex(2)
          ).asJava),
        // bar exists.
        new OffsetDeleteRequestTopic()
          .setName("bar")
          .setPartitions(List(
            new OffsetDeleteRequestPartition().setPartitionIndex(0),
            new OffsetDeleteRequestPartition().setPartitionIndex(1)
          ).asJava),
        // zar does not exist.
        new OffsetDeleteRequestTopic()
          .setName("zar")
          .setPartitions(List(
            new OffsetDeleteRequestPartition().setPartitionIndex(0),
            new OffsetDeleteRequestPartition().setPartitionIndex(1)
          ).asJava),
      ).asJava.iterator))

    val requestChannelRequest = buildRequest(new OffsetDeleteRequest.Builder(offsetDeleteRequest).build())

    // This is the request expected by the group coordinator. It contains
    // only existing topic-partitions.
    val expectedOffsetDeleteRequest = new OffsetDeleteRequestData()
      .setGroupId(group)
      .setTopics(new OffsetDeleteRequestTopicCollection(List(
        new OffsetDeleteRequestTopic()
          .setName("foo")
          .setPartitions(List(
            new OffsetDeleteRequestPartition().setPartitionIndex(0),
            new OffsetDeleteRequestPartition().setPartitionIndex(1)
          ).asJava),
        new OffsetDeleteRequestTopic()
          .setName("bar")
          .setPartitions(List(
            new OffsetDeleteRequestPartition().setPartitionIndex(0),
            new OffsetDeleteRequestPartition().setPartitionIndex(1)
          ).asJava)
      ).asJava.iterator))

    val future = new CompletableFuture[OffsetDeleteResponseData]()
    when(groupCoordinator.deleteOffsets(
      requestChannelRequest.context,
      expectedOffsetDeleteRequest,
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handle(
      requestChannelRequest,
      RequestLocal.noCaching
    )

    // This is the response returned by the group coordinator.
    val offsetDeleteResponse = new OffsetDeleteResponseData()
      .setTopics(new OffsetDeleteResponseTopicCollection(List(
        new OffsetDeleteResponseTopic()
          .setName("foo")
          .setPartitions(new OffsetDeleteResponsePartitionCollection(List(
            new OffsetDeleteResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code),
            new OffsetDeleteResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.NONE.code)
          ).asJava.iterator)),
        new OffsetDeleteResponseTopic()
          .setName("bar")
          .setPartitions(new OffsetDeleteResponsePartitionCollection(List(
            new OffsetDeleteResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code),
            new OffsetDeleteResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.NONE.code)
          ).asJava.iterator)),
      ).asJava.iterator))

    val expectedOffsetDeleteResponse = new OffsetDeleteResponseData()
      .setTopics(new OffsetDeleteResponseTopicCollection(List(
        new OffsetDeleteResponseTopic()
          .setName("foo")
          .setPartitions(new OffsetDeleteResponsePartitionCollection(List(
            // foo-2 is first because partitions failing the validation
            // are put in the response first.
            new OffsetDeleteResponsePartition()
              .setPartitionIndex(2)
              .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code),
            new OffsetDeleteResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code),
            new OffsetDeleteResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.NONE.code)
          ).asJava.iterator)),
        // zar is before bar because topics failing the validation are
        // put in the response first.
        new OffsetDeleteResponseTopic()
          .setName("zar")
          .setPartitions(new OffsetDeleteResponsePartitionCollection(List(
            new OffsetDeleteResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code),
            new OffsetDeleteResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
          ).asJava.iterator)),
        new OffsetDeleteResponseTopic()
          .setName("bar")
          .setPartitions(new OffsetDeleteResponsePartitionCollection(List(
            new OffsetDeleteResponsePartition()
              .setPartitionIndex(0)
              .setErrorCode(Errors.NONE.code),
            new OffsetDeleteResponsePartition()
              .setPartitionIndex(1)
              .setErrorCode(Errors.NONE.code)
          ).asJava.iterator)),
      ).asJava.iterator))

    future.complete(offsetDeleteResponse)
    val response = verifyNoThrottling[OffsetDeleteResponse](requestChannelRequest)
    assertEquals(expectedOffsetDeleteResponse, response.data)
  }

  @Test
  def testOffsetDeleteWithInvalidPartition(): Unit = {
    val group = "groupId"
    val topic = "topic"
    addTopicToMetadataCache(topic, numPartitions = 1)

    def checkInvalidPartition(invalidPartitionId: Int): Unit = {
      reset(groupCoordinator, replicaManager, clientRequestQuotaManager, requestChannel)

      val topics = new OffsetDeleteRequestTopicCollection()
      topics.add(new OffsetDeleteRequestTopic()
        .setName(topic)
        .setPartitions(Collections.singletonList(
          new OffsetDeleteRequestPartition().setPartitionIndex(invalidPartitionId))))
      val offsetDeleteRequest = new OffsetDeleteRequest.Builder(
        new OffsetDeleteRequestData()
          .setGroupId(group)
          .setTopics(topics)
      ).build()
      val request = buildRequest(offsetDeleteRequest)

      // The group coordinator is called even if there are no
      // topic-partitions left after the validation.
      when(groupCoordinator.deleteOffsets(
        request.context,
        new OffsetDeleteRequestData().setGroupId(group),
        RequestLocal.noCaching.bufferSupplier
      )).thenReturn(CompletableFuture.completedFuture(
        new OffsetDeleteResponseData()
      ))
      val kafkaApis = createKafkaApis()
      try {
        kafkaApis.handleOffsetDeleteRequest(request, RequestLocal.noCaching)

        val response = verifyNoThrottling[OffsetDeleteResponse](request)

        assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION,
          Errors.forCode(response.data.topics.find(topic).partitions.find(invalidPartitionId).errorCode))
      } finally {
        kafkaApis.close()
      }
    }

    checkInvalidPartition(-1)
    checkInvalidPartition(1) // topic has only one partition
  }

  @Test
  def testOffsetDeleteWithInvalidGroup(): Unit = {
    val group = "groupId"
    val topic = "topic"
    addTopicToMetadataCache(topic, numPartitions = 1)

    val offsetDeleteRequest = new OffsetDeleteRequest.Builder(
      new OffsetDeleteRequestData().setGroupId(group)
    ).build()
    val request = buildRequest(offsetDeleteRequest)

    val future = new CompletableFuture[OffsetDeleteResponseData]()
    when(groupCoordinator.deleteOffsets(
      request.context,
      offsetDeleteRequest.data,
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleOffsetDeleteRequest(request, RequestLocal.noCaching)

    future.completeExceptionally(Errors.GROUP_ID_NOT_FOUND.exception)

    val response = verifyNoThrottling[OffsetDeleteResponse](request)

    assertEquals(Errors.GROUP_ID_NOT_FOUND, Errors.forCode(response.data.errorCode))
  }

  @Test
  def testOffsetDeleteWithInvalidGroupWithTopLevelError(): Unit = {
    val group = "groupId"
    val topic = "topic"
    addTopicToMetadataCache(topic, numPartitions = 1)

    val offsetDeleteRequest = new OffsetDeleteRequest.Builder(
      new OffsetDeleteRequestData()
        .setGroupId(group)
        .setTopics(new OffsetDeleteRequestTopicCollection(Collections.singletonList(new OffsetDeleteRequestTopic()
          .setName("topic-unknown")
          .setPartitions(Collections.singletonList(new OffsetDeleteRequestPartition()
            .setPartitionIndex(0)
          ))
        ).iterator()))
    ).build()
    val request = buildRequest(offsetDeleteRequest)

    val future = new CompletableFuture[OffsetDeleteResponseData]()
    when(groupCoordinator.deleteOffsets(
      request.context,
      new OffsetDeleteRequestData().setGroupId(group), // Nonexistent topics won't be passed to groupCoordinator.
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleOffsetDeleteRequest(request, RequestLocal.noCaching)

    future.complete(new OffsetDeleteResponseData()
      .setErrorCode(Errors.GROUP_ID_NOT_FOUND.code())
    )

    val response = verifyNoThrottling[OffsetDeleteResponse](request)

    assertEquals(Errors.GROUP_ID_NOT_FOUND, Errors.forCode(response.data.errorCode))
  }

  private def testListOffsetFailedGetLeaderReplica(error: Errors): Unit = {
    val tp = new TopicPartition("foo", 0)
    val isolationLevel = IsolationLevel.READ_UNCOMMITTED
    val currentLeaderEpoch = Optional.of[Integer](15)

    when(replicaManager.fetchOffset(
      ArgumentMatchers.any[Seq[ListOffsetsTopic]](),
      ArgumentMatchers.eq(Set.empty[TopicPartition]),
      ArgumentMatchers.eq(isolationLevel),
      ArgumentMatchers.eq(ListOffsetsRequest.CONSUMER_REPLICA_ID),
      ArgumentMatchers.eq[String](clientId),
      ArgumentMatchers.anyInt(), // correlationId
      ArgumentMatchers.anyShort(), // version
      ArgumentMatchers.any[(Errors, ListOffsetsPartition) => ListOffsetsPartitionResponse](),
      ArgumentMatchers.any[List[ListOffsetsTopicResponse] => Unit](),
      ArgumentMatchers.anyInt() // timeoutMs
    )).thenAnswer(ans => {
      val callback = ans.getArgument[List[ListOffsetsTopicResponse] => Unit](8)
      val partitionResponse = new ListOffsetsPartitionResponse()
        .setErrorCode(error.code())
        .setOffset(ListOffsetsResponse.UNKNOWN_OFFSET)
        .setTimestamp(ListOffsetsResponse.UNKNOWN_TIMESTAMP)
        .setPartitionIndex(tp.partition())
      callback(List(new ListOffsetsTopicResponse().setName(tp.topic()).setPartitions(List(partitionResponse).asJava)))
    })

    val targetTimes = List(new ListOffsetsTopic()
      .setName(tp.topic)
      .setPartitions(List(new ListOffsetsPartition()
        .setPartitionIndex(tp.partition)
        .setTimestamp(ListOffsetsRequest.EARLIEST_TIMESTAMP)
        .setCurrentLeaderEpoch(currentLeaderEpoch.get)).asJava)).asJava
    val listOffsetRequest = ListOffsetsRequest.Builder.forConsumer(true, isolationLevel)
      .setTargetTimes(targetTimes).build()
    val request = buildRequest(listOffsetRequest)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    kafkaApis = createKafkaApis()
    kafkaApis.handleListOffsetRequest(request)

    val response = verifyNoThrottling[ListOffsetsResponse](request)
    val partitionDataOptional = response.topics.asScala.find(_.name == tp.topic).get
      .partitions.asScala.find(_.partitionIndex == tp.partition)
    assertTrue(partitionDataOptional.isDefined)

    val partitionData = partitionDataOptional.get
    assertEquals(error.code, partitionData.errorCode)
    assertEquals(ListOffsetsResponse.UNKNOWN_OFFSET, partitionData.offset)
    assertEquals(ListOffsetsResponse.UNKNOWN_TIMESTAMP, partitionData.timestamp)
  }

  @Test
  def testReadUncommittedConsumerListOffsetLatest(): Unit = {
    testConsumerListOffsetLatest(IsolationLevel.READ_UNCOMMITTED)
  }

  @Test
  def testReadCommittedConsumerListOffsetLatest(): Unit = {
    testConsumerListOffsetLatest(IsolationLevel.READ_COMMITTED)
  }

  @Test
  def testListOffsetMaxTimestampWithUnsupportedVersion(): Unit = {
    testConsumerListOffsetWithUnsupportedVersion(ListOffsetsRequest.MAX_TIMESTAMP, 6)
  }

  @Test
  def testListOffsetEarliestLocalTimestampWithUnsupportedVersion(): Unit = {
    testConsumerListOffsetWithUnsupportedVersion(ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP, 7)
  }

  @Test
  def testListOffsetLatestTieredTimestampWithUnsupportedVersion(): Unit = {
    testConsumerListOffsetWithUnsupportedVersion(ListOffsetsRequest.LATEST_TIERED_TIMESTAMP, 8)
  }

  @Test
  def testListOffsetNegativeTimestampWithOneOrAboveVersion(): Unit = {
    testConsumerListOffsetWithUnsupportedVersion(-6, 1)
  }

  /**
   * Verifies that the metadata response is correct if the broker listeners are inconsistent (i.e. one broker has
   * more listeners than another) and the request is sent on the listener that exists in both brokers.
   */
  @Test
  def testMetadataRequestOnSharedListenerWithInconsistentListenersAcrossBrokers(): Unit = {
    val (plaintextListener, _) = updateMetadataCacheWithInconsistentListeners()
    val response = sendMetadataRequestWithInconsistentListeners(plaintextListener)
    assertEquals(Set(0, 1), response.brokers.asScala.map(_.id).toSet)
  }

  /**
   * Verifies that the metadata response is correct if the broker listeners are inconsistent (i.e. one broker has
   * more listeners than another) and the request is sent on the listener that exists in one broker.
   */
  @Test
  def testMetadataRequestOnDistinctListenerWithInconsistentListenersAcrossBrokers(): Unit = {
    val (_, anotherListener) = updateMetadataCacheWithInconsistentListeners()
    val response = sendMetadataRequestWithInconsistentListeners(anotherListener)
    assertEquals(Set(0), response.brokers.asScala.map(_.id).toSet)
  }


  /**
   * Metadata request to fetch all topics should not result in the followings:
   * 1) Auto topic creation
   * 2) UNKNOWN_TOPIC_OR_PARTITION
   *
   * This case is testing the case that a topic is being deleted from MetadataCache right after
   * authorization but before checking in MetadataCache.
   */
  @Test
  def testGetAllTopicMetadataShouldNotCreateTopicOrReturnUnknownTopicPartition(): Unit = {
    // Setup: authorizer authorizes 2 topics, but one got deleted in metadata cache
    metadataCache = mock(classOf[KRaftMetadataCache])
    when(metadataCache.getAliveBrokerNodes(any())).thenReturn(List(new Node(brokerId,"localhost", 0)))
    when(metadataCache.getRandomAliveBrokerId).thenReturn(None)

    // 2 topics returned for authorization in during handle
    val topicsReturnedFromMetadataCacheForAuthorization = Set("remaining-topic", "later-deleted-topic")
    when(metadataCache.getAllTopics()).thenReturn(topicsReturnedFromMetadataCacheForAuthorization)
    // 1 topic is deleted from metadata right at the time between authorization and the next getTopicMetadata() call
    when(metadataCache.getTopicMetadata(
      ArgumentMatchers.eq(topicsReturnedFromMetadataCacheForAuthorization),
      any[ListenerName],
      anyBoolean,
      anyBoolean
    )).thenReturn(Seq(
      new MetadataResponseTopic()
        .setErrorCode(Errors.NONE.code)
        .setName("remaining-topic")
        .setIsInternal(false)
    ))

    val response = sendMetadataRequestWithInconsistentListeners(new ListenerName("PLAINTEXT"))
    val responseTopics = response.topicMetadata().asScala.map { metadata => metadata.topic() }

    // verify we don't create topic when getAllTopicMetadata
    verify(autoTopicCreationManager, never).createTopics(any(), any(), any())
    assertEquals(List("remaining-topic"), responseTopics)
    assertTrue(response.topicsByError(Errors.UNKNOWN_TOPIC_OR_PARTITION).isEmpty)
  }

  @Test
  def testUnauthorizedTopicMetadataRequest(): Unit = {
    // 1. Set up broker information
    val plaintextListener = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)
    val endpoints = new BrokerEndpointCollection()
    endpoints.add(
      new BrokerEndpoint()
        .setHost("broker0")
        .setPort(9092)
        .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
        .setName(plaintextListener.value)
    )
    MetadataCacheTest.updateCache(metadataCache,
      Seq(new RegisterBrokerRecord().setBrokerId(0).setRack("rack").setFenced(false).setEndPoints(endpoints))
    )

    // 2. Set up authorizer
    val authorizer: Authorizer = mock(classOf[Authorizer])
    val unauthorizedTopic = "unauthorized-topic"
    val authorizedTopic = "authorized-topic"

    val expectedActions = Seq(
      new Action(AclOperation.DESCRIBE, new ResourcePattern(ResourceType.TOPIC, unauthorizedTopic, PatternType.LITERAL), 1, true, true),
      new Action(AclOperation.DESCRIBE, new ResourcePattern(ResourceType.TOPIC, authorizedTopic, PatternType.LITERAL), 1, true, true)
    )

    when(authorizer.authorize(any[RequestContext], argThat((t: java.util.List[Action]) => t.containsAll(expectedActions.asJava))))
      .thenAnswer { invocation =>
        val actions = invocation.getArgument(1).asInstanceOf[util.List[Action]].asScala
        actions.map { action =>
          if (action.resourcePattern().name().equals(authorizedTopic))
            AuthorizationResult.ALLOWED
          else
            AuthorizationResult.DENIED
        }.asJava
      }

    // 3. Set up MetadataCache
    val authorizedTopicId = Uuid.randomUuid()
    val unauthorizedTopicId = Uuid.randomUuid()
    addTopicToMetadataCache(authorizedTopic, 1, topicId = authorizedTopicId)
    addTopicToMetadataCache(unauthorizedTopic, 1, topicId = unauthorizedTopicId)

    def createDummyPartitionRecord(topicId: Uuid) = {
      new PartitionRecord()
        .setTopicId(topicId)
        .setPartitionId(0)
        .setLeader(0)
        .setLeaderEpoch(0)
        .setReplicas(Collections.singletonList(0))
        .setIsr(Collections.singletonList(0))
    }

    val partitionRecords = Seq(authorizedTopicId, unauthorizedTopicId).map(createDummyPartitionRecord)
    MetadataCacheTest.updateCache(metadataCache, partitionRecords)

    // 4. Send TopicMetadataReq using topicId
    val metadataReqByTopicId = new MetadataRequest.Builder(util.Arrays.asList(authorizedTopicId, unauthorizedTopicId)).build()
    val repByTopicId = buildRequest(metadataReqByTopicId, plaintextListener)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleTopicMetadataRequest(repByTopicId)
    val metadataByTopicIdResp = verifyNoThrottling[MetadataResponse](repByTopicId)

    val metadataByTopicId = metadataByTopicIdResp.data().topics().asScala.groupBy(_.topicId()).map(kv => (kv._1, kv._2.head))

    metadataByTopicId.foreach { case (topicId, metadataResponseTopic) =>
      if (topicId == unauthorizedTopicId) {
        // Return an TOPIC_AUTHORIZATION_FAILED on unauthorized error regardless of leaking the existence of topic id
        assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code(), metadataResponseTopic.errorCode())
        // Do not return topic information on unauthorized error
        assertNull(metadataResponseTopic.name())
      } else {
        assertEquals(Errors.NONE.code(), metadataResponseTopic.errorCode())
        assertEquals(authorizedTopic, metadataResponseTopic.name())
      }
    }
    kafkaApis.close()

    // 4. Send TopicMetadataReq using topic name
    reset(clientRequestQuotaManager, requestChannel)
    val metadataReqByTopicName = new MetadataRequest.Builder(util.Arrays.asList(authorizedTopic, unauthorizedTopic), false).build()
    val repByTopicName = buildRequest(metadataReqByTopicName, plaintextListener)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleTopicMetadataRequest(repByTopicName)
    val metadataByTopicNameResp = verifyNoThrottling[MetadataResponse](repByTopicName)

    val metadataByTopicName = metadataByTopicNameResp.data().topics().asScala.groupBy(_.name()).map(kv => (kv._1, kv._2.head))

    metadataByTopicName.foreach { case (topicName, metadataResponseTopic) =>
      if (topicName == unauthorizedTopic) {
        assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code(), metadataResponseTopic.errorCode())
        // Do not return topic Id on unauthorized error
        assertEquals(Uuid.ZERO_UUID, metadataResponseTopic.topicId())
      } else {
        assertEquals(Errors.NONE.code(), metadataResponseTopic.errorCode())
        assertEquals(authorizedTopicId, metadataResponseTopic.topicId())
      }
    }
  }

    /**
   * Verifies that sending a fetch request with version 9 works correctly when
   * ReplicaManager.getLogConfig returns None.
   */
  @Test
  def testFetchRequestV9WithNoLogConfig(): Unit = {
    val tidp = new TopicIdPartition(Uuid.ZERO_UUID, new TopicPartition("foo", 0))
    val tp = tidp.topicPartition
    addTopicToMetadataCache(tp.topic, numPartitions = 1)
    val hw = 3
    val timestamp = 1000

    when(replicaManager.getLogConfig(ArgumentMatchers.eq(tp))).thenReturn(None)

    when(replicaManager.fetchMessages(
      any[FetchParams],
      any[Seq[(TopicIdPartition, FetchRequest.PartitionData)]],
      any[ReplicaQuota],
      any[Seq[(TopicIdPartition, FetchPartitionData)] => Unit]()
    )).thenAnswer(invocation => {
      val callback = invocation.getArgument(3).asInstanceOf[Seq[(TopicIdPartition, FetchPartitionData)] => Unit]
      val records = MemoryRecords.withRecords(Compression.NONE,
        new SimpleRecord(timestamp, "foo".getBytes(StandardCharsets.UTF_8)))
      callback(Seq(tidp -> new FetchPartitionData(Errors.NONE, hw, 0, records,
        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)))
    })

    val fetchData = Map(tidp -> new FetchRequest.PartitionData(Uuid.ZERO_UUID, 0, 0, 1000,
      Optional.empty())).asJava
    val fetchDataBuilder = Map(tp -> new FetchRequest.PartitionData(Uuid.ZERO_UUID, 0, 0, 1000,
      Optional.empty())).asJava
    val fetchMetadata = new JFetchMetadata(0, 0)
    val fetchContext = new FullFetchContext(time, new FetchSessionCacheShard(1000, 100),
      fetchMetadata, fetchData, false, false)
    when(fetchManager.newContext(
      any[Short],
      any[JFetchMetadata],
      any[Boolean],
      any[util.Map[TopicIdPartition, FetchRequest.PartitionData]],
      any[util.List[TopicIdPartition]],
      any[util.Map[Uuid, String]])).thenReturn(fetchContext)

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    val fetchRequest = new FetchRequest.Builder(9, 9, -1, -1, 100, 0, fetchDataBuilder)
      .build()
    val request = buildRequest(fetchRequest)
    kafkaApis = createKafkaApis()
    kafkaApis.handleFetchRequest(request)

    val response = verifyNoThrottling[FetchResponse](request)
    val responseData = response.responseData(metadataCache.topicIdsToNames(), 9)
    assertTrue(responseData.containsKey(tp))

    val partitionData = responseData.get(tp)
    assertEquals(Errors.NONE.code, partitionData.errorCode)
    assertEquals(hw, partitionData.highWatermark)
    assertEquals(-1, partitionData.lastStableOffset)
    assertEquals(0, partitionData.logStartOffset)
    assertEquals(timestamp, FetchResponse.recordsOrFail(partitionData).batches.iterator.next.maxTimestamp)
    assertNull(partitionData.abortedTransactions)
  }

  /**
   * Verifies that partitions with unknown topic ID errors are added to the erroneous set and there is not an attempt to fetch them.
   */
  @ParameterizedTest
  @ValueSource(ints = Array(-1, 0))
  def testFetchRequestErroneousPartitions(replicaId: Int): Unit = {
    val foo = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0))
    val unresolvedFoo = new TopicIdPartition(foo.topicId, new TopicPartition(null, foo.partition))

    addTopicToMetadataCache(foo.topic, 1, topicId = foo.topicId)

    // We will never return a logConfig when the topic name is null. This is ok since we won't have any records to convert.
    when(replicaManager.getLogConfig(ArgumentMatchers.eq(unresolvedFoo.topicPartition))).thenReturn(None)

    // Simulate unknown topic ID in the context
    val fetchData = Map(new TopicIdPartition(foo.topicId, new TopicPartition(null, foo.partition)) ->
      new FetchRequest.PartitionData(foo.topicId, 0, 0, 1000, Optional.empty())).asJava
    val fetchDataBuilder = Map(foo.topicPartition -> new FetchRequest.PartitionData(foo.topicId, 0, 0, 1000,
      Optional.empty())).asJava
    val fetchMetadata = new JFetchMetadata(0, 0)
    val fetchContext = new FullFetchContext(time, new FetchSessionCacheShard(1000, 100),
      fetchMetadata, fetchData, true, replicaId >= 0)
    // We expect to have the resolved partition, but we will simulate an unknown one with the fetchContext we return.
    when(fetchManager.newContext(
      ApiKeys.FETCH.latestVersion,
      fetchMetadata,
      replicaId >= 0,
      Collections.singletonMap(foo, new FetchRequest.PartitionData(foo.topicId, 0, 0, 1000, Optional.empty())),
      Collections.emptyList[TopicIdPartition],
      metadataCache.topicIdsToNames())
    ).thenReturn(fetchContext)

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    // If replicaId is -1 we will build a consumer request. Any non-negative replicaId will build a follower request.
    val replicaEpoch = if (replicaId < 0) -1 else 1
    val fetchRequest = new FetchRequest.Builder(ApiKeys.FETCH.latestVersion, ApiKeys.FETCH.latestVersion,
      replicaId, replicaEpoch, 100, 0, fetchDataBuilder).metadata(fetchMetadata).build()
    val request = buildRequest(fetchRequest)
    kafkaApis = createKafkaApis()
    kafkaApis.handleFetchRequest(request)

    val response = verifyNoThrottling[FetchResponse](request)
    val responseData = response.responseData(metadataCache.topicIdsToNames(), ApiKeys.FETCH.latestVersion)
    assertTrue(responseData.containsKey(foo.topicPartition))

    val partitionData = responseData.get(foo.topicPartition)
    assertEquals(Errors.UNKNOWN_TOPIC_ID.code, partitionData.errorCode)
    assertEquals(-1, partitionData.highWatermark)
    assertEquals(-1, partitionData.lastStableOffset)
    assertEquals(-1, partitionData.logStartOffset)
    assertEquals(MemoryRecords.EMPTY, FetchResponse.recordsOrFail(partitionData))
  }

  @Test
  def testFetchResponseContainsNewLeaderOnNotLeaderOrFollower(): Unit = {
    val topicId = Uuid.randomUuid()
    val tidp = new TopicIdPartition(topicId, new TopicPartition("foo", 0))
    val tp = tidp.topicPartition
    addTopicToMetadataCache(tp.topic, numPartitions = 1, numBrokers = 3, topicId)

    when(replicaManager.getLogConfig(ArgumentMatchers.eq(tp))).thenReturn(Some(LogConfig.fromProps(
      Collections.emptyMap(),
      new Properties()
    )))

    val partition = mock(classOf[Partition])
    val newLeaderId = 2
    val newLeaderEpoch = 5

    when(replicaManager.getPartitionOrError(tp)).thenAnswer(_ => Right(partition))
    when(partition.leaderReplicaIdOpt).thenAnswer(_ => Some(newLeaderId))
    when(partition.getLeaderEpoch).thenAnswer(_ => newLeaderEpoch)

    when(replicaManager.fetchMessages(
      any[FetchParams],
      any[Seq[(TopicIdPartition, FetchRequest.PartitionData)]],
      any[ReplicaQuota],
      any[Seq[(TopicIdPartition, FetchPartitionData)] => Unit]()
    )).thenAnswer(invocation => {
      val callback = invocation.getArgument(3).asInstanceOf[Seq[(TopicIdPartition, FetchPartitionData)] => Unit]
      callback(Seq(tidp -> new FetchPartitionData(Errors.NOT_LEADER_OR_FOLLOWER, UnifiedLog.UNKNOWN_OFFSET, UnifiedLog.UNKNOWN_OFFSET, MemoryRecords.EMPTY,
        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)))
    })

    val fetchData = Map(tidp -> new FetchRequest.PartitionData(topicId, 0, 0, 1000,
      Optional.empty())).asJava
    val fetchDataBuilder = Map(tp -> new FetchRequest.PartitionData(topicId, 0, 0, 1000,
      Optional.empty())).asJava
    val fetchMetadata = new JFetchMetadata(0, 0)
    val fetchContext = new FullFetchContext(time, new FetchSessionCacheShard(1000, 100),
      fetchMetadata, fetchData, true, false)
    when(fetchManager.newContext(
      any[Short],
      any[JFetchMetadata],
      any[Boolean],
      any[util.Map[TopicIdPartition, FetchRequest.PartitionData]],
      any[util.List[TopicIdPartition]],
      any[util.Map[Uuid, String]])).thenReturn(fetchContext)

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    val fetchRequest = new FetchRequest.Builder(16, 16, -1, -1, 100, 0, fetchDataBuilder)
      .build()
    val request = buildRequest(fetchRequest)
    kafkaApis = createKafkaApis()
    kafkaApis.handleFetchRequest(request)

    val response = verifyNoThrottling[FetchResponse](request)
    val responseData = response.responseData(metadataCache.topicIdsToNames(), 16)

    val partitionData = responseData.get(tp)
    assertEquals(Errors.NOT_LEADER_OR_FOLLOWER.code, partitionData.errorCode)
    assertEquals(newLeaderId, partitionData.currentLeader.leaderId())
    assertEquals(newLeaderEpoch, partitionData.currentLeader.leaderEpoch())
    val node = response.data.nodeEndpoints.asScala
    assertEquals(Seq(2), node.map(_.nodeId))
    assertEquals(Seq("broker2"), node.map(_.host))
  }

  @Test
  def testHandleShareFetchRequestSuccessWithoutAcknowledgements(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: Uuid = Uuid.ZERO_UUID

    val shareSessionEpoch = 0

    val records = memoryRecords(10, 0)

    when(sharePartitionManager.fetchMessages(any(), any(), any(), anyInt(), anyInt(), any())).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareFetchResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)) ->
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.NONE.code)
            .setAcknowledgeErrorCode(Errors.NONE.code)
            .setRecords(records)
            .setAcquiredRecords(new util.ArrayList(List(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setDeliveryCount(1)
            ).asJava))
      ).asJava)
    )

    when(sharePartitionManager.newContext(any(), any(), any(), any(), any())).thenReturn(
      new ShareSessionContext(new ShareRequestMetadata(memberId, shareSessionEpoch), util.List.of(
        new TopicIdPartition(topicId, partitionIndex, topicName)))
    )

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    val shareFetchRequestData = new ShareFetchRequestData().
      setGroupId("group").
      setMemberId(memberId.toString).
      setShareSessionEpoch(shareSessionEpoch).
      setTopics(util.List.of(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(util.List.of(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(partitionIndex)))))

    val shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    val request = buildRequest(shareFetchRequest)
    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    kafkaApis.handleShareFetchRequest(request)
    val response = verifyNoThrottling[ShareFetchResponse](request)
    val responseData = response.data()
    val topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    assertEquals(topicId, topicResponses.get(0).topicId)
    assertEquals(1, topicResponses.get(0).partitions.size())
    assertEquals(partitionIndex, topicResponses.get(0).partitions.get(0).partitionIndex)
    assertEquals(Errors.NONE.code, topicResponses.get(0).partitions.get(0).errorCode)
    assertEquals(records, topicResponses.get(0).partitions.get(0).records)
    assertArrayEquals(expectedAcquiredRecords(0, 9, 1).toArray(), topicResponses.get(0).partitions.get(0).acquiredRecords.toArray())
  }

  @Test
  def testHandleShareFetchRequestInvalidRequestOnInitialEpoch(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: Uuid = Uuid.ZERO_UUID

    val groupId = "group"
    val partitionIndex = 0

    val records = memoryRecords(10, 0)

    when(sharePartitionManager.fetchMessages(any(), any(), any(), anyInt(), anyInt(), any())).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareFetchResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)) ->
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.NONE.code)
            .setAcknowledgeErrorCode(Errors.NONE.code)
            .setRecords(records)
            .setAcquiredRecords(new util.ArrayList(List(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setDeliveryCount(1)
            ).asJava))
      ).asJava)
    )

    val cachedSharePartitions = new ImplicitLinkedHashCollection[CachedSharePartition]
    cachedSharePartitions.mustAdd(new CachedSharePartition(
      new TopicIdPartition(topicId, partitionIndex, topicName), false))

    when(sharePartitionManager.newContext(any(), any(), any(), any(), any())).thenThrow(
      Errors.INVALID_REQUEST.exception()
    ).thenReturn(new ShareSessionContext(new ShareRequestMetadata(memberId, 1), new ShareSession(
      new ShareSessionKey(groupId, memberId), cachedSharePartitions, 0L, 0L, 2
    )))

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    var shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(0).
      setTopics(util.List.of(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(util.List.of(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(partitionIndex)
            .setAcknowledgementBatches(util.List.of(
            new AcknowledgementBatch()
              .setFirstOffset(0)
              .setLastOffset(9)
              .setAcknowledgeTypes(util.List.of(1.toByte))
          ))
        ))
      ))

    var shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    var request = buildRequest(shareFetchRequest)
    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    kafkaApis.handleShareFetchRequest(request)
    var response = verifyNoThrottling[ShareFetchResponse](request)
    var responseData = response.data()

    assertEquals(Errors.INVALID_REQUEST.code, responseData.errorCode)

    // Testing whether the subsequent request with the incremented share session epoch works or not.
    shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(1).
      setTopics(util.List.of(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(util.List.of(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(0)
        ))
      ))

    shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    request = buildRequest(shareFetchRequest)
    kafkaApis.handleShareFetchRequest(request)
    response = verifyNoThrottling[ShareFetchResponse](request)
    responseData = response.data()
    val topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    assertEquals(topicId, topicResponses.get(0).topicId)
    assertEquals(1, topicResponses.get(0).partitions.size())
    assertEquals(partitionIndex, topicResponses.get(0).partitions.get(0).partitionIndex)
    assertEquals(Errors.NONE.code, topicResponses.get(0).partitions.get(0).errorCode)
    assertEquals(records, topicResponses.get(0).partitions.get(0).records)
    assertArrayEquals(expectedAcquiredRecords(0, 9, 1).toArray(), topicResponses.get(0).partitions.get(0).acquiredRecords.toArray())
  }

  @Test
  def testHandleShareFetchRequestInvalidRequestOnFinalEpoch(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: Uuid = Uuid.ZERO_UUID

    val groupId = "group"

    val records = memoryRecords(10, 0)

    when(sharePartitionManager.fetchMessages(any(), any(), any(), anyInt(), anyInt(), any())).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareFetchResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)) ->
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.NONE.code)
            .setAcknowledgeErrorCode(Errors.NONE.code)
            .setRecords(records)
            .setAcquiredRecords(new util.ArrayList(List(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setDeliveryCount(1)
            ).asJava))
      ).asJava)
    )

    when(sharePartitionManager.newContext(any(), any(), any(), any(), any())).thenReturn(
      new ShareSessionContext(new ShareRequestMetadata(memberId, 0), util.List.of(
        new TopicIdPartition(topicId, partitionIndex, topicName)
      ))
    ).thenThrow(Errors.INVALID_REQUEST.exception)

    when(sharePartitionManager.releaseSession(any(), any())).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)) ->
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(partitionIndex)
            .setErrorCode(Errors.NONE.code)
      ).asJava)
    )

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    var shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(0).
      setTopics(util.List.of(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(util.List.of(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(0)))))

    var shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    var request = buildRequest(shareFetchRequest)
    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    kafkaApis.handleShareFetchRequest(request)
    var response = verifyNoThrottling[ShareFetchResponse](request)
    var responseData = response.data()
    val topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    assertEquals(topicId, topicResponses.get(0).topicId)
    assertEquals(1, topicResponses.get(0).partitions.size())
    assertEquals(partitionIndex, topicResponses.get(0).partitions.get(0).partitionIndex)
    assertEquals(Errors.NONE.code, topicResponses.get(0).partitions.get(0).errorCode)
    assertEquals(records, topicResponses.get(0).partitions.get(0).records)
    assertArrayEquals(expectedAcquiredRecords(0, 9, 1).toArray(), topicResponses.get(0).partitions.get(0).acquiredRecords.toArray())

    shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(-1).
      setTopics(List(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(List(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(0)
            .setAcknowledgementBatches(List(
              new AcknowledgementBatch()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setAcknowledgeTypes(Collections.singletonList(1.toByte))
            ).asJava)
        ).asJava)
      ).asJava)

    shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    request = buildRequest(shareFetchRequest)
    kafkaApis.handleShareFetchRequest(request)
    response = verifyNoThrottling[ShareFetchResponse](request)
    responseData = response.data()

    assertEquals(Errors.INVALID_REQUEST.code, responseData.errorCode)
  }

  @Test
  def testHandleShareFetchRequestFetchThrowsException(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: Uuid = Uuid.ZERO_UUID

    when(sharePartitionManager.fetchMessages(any(), any(), any(), anyInt(), anyInt(), any())).thenReturn(
      FutureUtils.failedFuture[util.Map[TopicIdPartition, ShareFetchResponseData.PartitionData]](Errors.UNKNOWN_SERVER_ERROR.exception())
    )

    when(sharePartitionManager.newContext(any(), any(), any(), any(), any())).thenReturn(
      new ShareSessionContext(new ShareRequestMetadata(memberId, 0), util.List.of(
        new TopicIdPartition(topicId, partitionIndex, topicName)
      ))
    )

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    val shareFetchRequestData = new ShareFetchRequestData().
      setGroupId("group").
      setMemberId(memberId.toString).
      setShareSessionEpoch(0).
      setTopics(util.List.of(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(util.List.of(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(0)))))

    val shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    val request = buildRequest(shareFetchRequest)
    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    kafkaApis.handleShareFetchRequest(request)
    val response = verifyNoThrottling[ShareFetchResponse](request)
    val responseData = response.data()

    assertEquals(Errors.UNKNOWN_SERVER_ERROR.code, responseData.errorCode)
  }

  @Test
  def testHandleShareFetchRequestAcknowledgeThrowsException(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: Uuid = Uuid.ZERO_UUID

    val groupId = "group"

    val records = memoryRecords(10, 0)

    when(sharePartitionManager.fetchMessages(any(), any(), any(), anyInt(), anyInt(), any())).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareFetchResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)) ->
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.NONE.code)
            .setRecords(records)
            .setAcquiredRecords(new util.ArrayList(List(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setDeliveryCount(1)
            ).asJava))
      ).asJava)
    )

    when(sharePartitionManager.acknowledge(any(), any(), any())).thenReturn(
      FutureUtils.failedFuture[util.Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]](Errors.UNKNOWN_SERVER_ERROR.exception())
    )

    val cachedSharePartitions = new ImplicitLinkedHashCollection[CachedSharePartition]
    cachedSharePartitions.mustAdd(new CachedSharePartition(
      new TopicIdPartition(topicId, partitionIndex, topicName), false))

    when(sharePartitionManager.newContext(any(), any(), any(), any(), any()))
      .thenReturn(new ShareSessionContext(new ShareRequestMetadata(memberId, 1), new ShareSession(
        new ShareSessionKey(groupId, memberId), cachedSharePartitions, 0L, 0L, 2))
      )

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    val shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(1).
      setTopics(util.List.of(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(util.List.of(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(0)
            .setAcknowledgementBatches(util.List.of(
              new AcknowledgementBatch()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setAcknowledgeTypes(util.List.of(1.toByte))
            ))
        ))
      ))

    val shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    val request = buildRequest(shareFetchRequest)
    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    kafkaApis.handleShareFetchRequest(request)
    val response = verifyNoThrottling[ShareFetchResponse](request)
    val responseData = response.data()

    assertEquals(Errors.UNKNOWN_SERVER_ERROR.code, responseData.errorCode)
  }

  @Test
  def testHandleShareFetchRequestFetchAndAcknowledgeThrowsException(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: Uuid = Uuid.ZERO_UUID

    val groupId = "group"

    when(sharePartitionManager.fetchMessages(any(), any(), any(), anyInt(), anyInt(), any())).thenReturn(
      FutureUtils.failedFuture[util.Map[TopicIdPartition, ShareFetchResponseData.PartitionData]](Errors.UNKNOWN_SERVER_ERROR.exception())
    )

    when(sharePartitionManager.acknowledge(any(), any(), any())).thenReturn(
      FutureUtils.failedFuture[util.Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]](Errors.UNKNOWN_SERVER_ERROR.exception())
    )

    val cachedSharePartitions = new ImplicitLinkedHashCollection[CachedSharePartition]
    cachedSharePartitions.mustAdd(new CachedSharePartition(
      new TopicIdPartition(topicId, partitionIndex, topicName), false))

    when(sharePartitionManager.newContext(any(), any(), any(), any(), any()))
      .thenReturn(new ShareSessionContext(new ShareRequestMetadata(memberId, 1), new ShareSession(
        new ShareSessionKey(groupId, memberId), cachedSharePartitions, 0L, 0L, 2))
      )

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    val shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(1).
      setTopics(util.List.of(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(util.List.of(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(0)
            .setAcknowledgementBatches(util.List.of(
              new AcknowledgementBatch()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setAcknowledgeTypes(util.List.of(1.toByte))
            ))
        ))
      ))

    val shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    val request = buildRequest(shareFetchRequest)
    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    kafkaApis.handleShareFetchRequest(request)
    val response = verifyNoThrottling[ShareFetchResponse](request)
    val responseData = response.data()

    assertEquals(Errors.UNKNOWN_SERVER_ERROR.code, responseData.errorCode)
  }

  @Test
  def testHandleShareFetchRequestErrorInReadingPartition(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: Uuid = Uuid.ZERO_UUID

    val records = MemoryRecords.EMPTY

    when(sharePartitionManager.fetchMessages(any(), any(), any(), anyInt(), anyInt(), any())).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareFetchResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)) ->
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.REPLICA_NOT_AVAILABLE.code)
            .setRecords(records)
            .setAcquiredRecords(new util.ArrayList(List().asJava))
      ).asJava)
    )

    when(sharePartitionManager.newContext(any(), any(), any(), any(), any())).thenReturn(
      new ShareSessionContext(new ShareRequestMetadata(memberId, 0), util.List.of(
        new TopicIdPartition(topicId, partitionIndex, topicName)
      ))
    )

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    val shareFetchRequestData = new ShareFetchRequestData().
      setGroupId("group").
      setMemberId(memberId.toString).
      setShareSessionEpoch(0).
      setTopics(util.List.of(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(util.List.of(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(0)))))

    val shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    val request = buildRequest(shareFetchRequest)
    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    kafkaApis.handleShareFetchRequest(request)
    val response = verifyNoThrottling[ShareFetchResponse](request)
    val responseData = response.data()
    val topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    assertEquals(topicId, topicResponses.get(0).topicId)
    assertEquals(1, topicResponses.get(0).partitions.size())
    assertEquals(partitionIndex, topicResponses.get(0).partitions.get(0).partitionIndex)
    assertEquals(Errors.REPLICA_NOT_AVAILABLE.code, topicResponses.get(0).partitions.get(0).errorCode)
    assertEquals(records, topicResponses.get(0).partitions.get(0).records)
    assertTrue(topicResponses.get(0).partitions.get(0).acquiredRecords.toArray().isEmpty)
  }

  @Test
  def testHandleShareFetchRequestShareSessionNotFoundError(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: Uuid = Uuid.ZERO_UUID

    val groupId = "group"
    val records = memoryRecords(10, 0)

    when(sharePartitionManager.fetchMessages(any(), any(), any(), anyInt(), anyInt(), any())).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareFetchResponseData.PartitionData](
        new TopicIdPartition(topicId, partitionIndex, topicName) ->
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.NONE.code)
            .setRecords(records)
            .setAcquiredRecords(new util.ArrayList(List(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setDeliveryCount(1)
            ).asJava))
      ).asJava)
    )

    when(sharePartitionManager.newContext(any(), any(), any(), any(), any())).thenReturn(
      new ShareSessionContext(new ShareRequestMetadata(memberId, 0), util.List.of(
        new TopicIdPartition(topicId, partitionIndex, topicName)
      ))
    ).thenThrow(Errors.SHARE_SESSION_NOT_FOUND.exception)

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    var shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(0).
      setTopics(util.List.of(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(util.List.of(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(0)))))

    var shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    var request = buildRequest(shareFetchRequest)
    // First share fetch request is to establish the share session with the broker.
    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    kafkaApis.handleShareFetchRequest(request)
    var response = verifyNoThrottling[ShareFetchResponse](request)
    var responseData = response.data()
    val topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    assertEquals(topicId, topicResponses.get(0).topicId)
    assertEquals(1, topicResponses.get(0).partitions.size())
    assertEquals(partitionIndex, topicResponses.get(0).partitions.get(0).partitionIndex)
    assertEquals(Errors.NONE.code, topicResponses.get(0).partitions.get(0).errorCode)
    assertEquals(records, topicResponses.get(0).partitions.get(0).records)
    assertArrayEquals(expectedAcquiredRecords(0, 9, 1).toArray(), topicResponses.get(0).partitions.get(0).acquiredRecords.toArray())

    val memberId2 = Uuid.randomUuid()

    // Using wrong member ID.
    shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId2.toString).
      setShareSessionEpoch(1).
      setTopics(util.List.of(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(util.List.of(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(0)))))

    shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    request = buildRequest(shareFetchRequest)
    kafkaApis.handleShareFetchRequest(request)
    response = verifyNoThrottling[ShareFetchResponse](request)
    responseData = response.data()

    assertEquals(Errors.SHARE_SESSION_NOT_FOUND.code, responseData.errorCode)
  }

  @Test
  def testHandleShareFetchRequestInvalidShareSessionError(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: Uuid = Uuid.ZERO_UUID

    val groupId = "group"
    val records = memoryRecords(10, 0)

    when(sharePartitionManager.fetchMessages(any(), any(), any(), anyInt(), anyInt(), any())).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareFetchResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)) ->
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.NONE.code)
            .setRecords(records)
            .setAcquiredRecords(new util.ArrayList(List(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setDeliveryCount(1)
            ).asJava))
      ).asJava)
    )

    when(sharePartitionManager.newContext(any(), any(), any(), any(), any())).thenReturn(
      new ShareSessionContext(new ShareRequestMetadata(memberId, 0), util.List.of(
        new TopicIdPartition(topicId, partitionIndex, topicName)
      ))
    ).thenThrow(Errors.INVALID_SHARE_SESSION_EPOCH.exception)

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    var shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(0).
      setTopics(util.List.of(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(util.List.of(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(0)))))

    var shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    var request = buildRequest(shareFetchRequest)
    // First share fetch request is to establish the share session with the broker.
    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    kafkaApis.handleShareFetchRequest(request)
    var response = verifyNoThrottling[ShareFetchResponse](request)
    var responseData = response.data()
    val topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    assertEquals(topicId, topicResponses.get(0).topicId)
    assertEquals(1, topicResponses.get(0).partitions.size())
    assertEquals(partitionIndex, topicResponses.get(0).partitions.get(0).partitionIndex)
    assertEquals(Errors.NONE.code, topicResponses.get(0).partitions.get(0).errorCode)
    assertEquals(records, topicResponses.get(0).partitions.get(0).records)
    assertArrayEquals(expectedAcquiredRecords(0, 9, 1).toArray(), topicResponses.get(0).partitions.get(0).acquiredRecords.toArray())

    shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(2). // Invalid share session epoch, should have 1 for the second request.
      setTopics(util.List.of(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(util.List.of(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(0)))))

    shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    request = buildRequest(shareFetchRequest)
    kafkaApis.handleShareFetchRequest(request)
    response = verifyNoThrottling[ShareFetchResponse](request)
    responseData = response.data()

    assertEquals(Errors.INVALID_SHARE_SESSION_EPOCH.code, responseData.errorCode)
  }

  @Test
  def testHandleShareFetchRequestShareSessionSuccessfullyEstablished(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: Uuid = Uuid.randomUuid()

    val groupId = "group"

    val records1 = memoryRecords(10, 0)
    val records2 = memoryRecords(10, 10)
    val records3 = memoryRecords(10, 20)

    when(sharePartitionManager.fetchMessages(any(), any(), any(), anyInt(), anyInt(), any())).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareFetchResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)) ->
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.NONE.code)
            .setRecords(records1)
            .setAcquiredRecords(new util.ArrayList(List(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setDeliveryCount(1)
            ).asJava))
      ).asJava)
    ).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareFetchResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)) ->
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.NONE.code)
            .setAcknowledgeErrorCode(Errors.NONE.code)
            .setRecords(records2)
            .setAcquiredRecords(new util.ArrayList(List(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(10)
                .setLastOffset(19)
                .setDeliveryCount(1)
            ).asJava))
      ).asJava)
    ).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareFetchResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)) ->
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.NONE.code)
            .setAcknowledgeErrorCode(Errors.NONE.code)
            .setRecords(records3)
            .setAcquiredRecords(new util.ArrayList(List(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(20)
                .setLastOffset(29)
                .setDeliveryCount(1)
            ).asJava))
      ).asJava)
    )

    when(sharePartitionManager.acknowledge(any(), any(), any())).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)) ->
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(partitionIndex)
            .setErrorCode(Errors.NONE.code)
      ).asJava)
    ).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)) ->
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(partitionIndex)
            .setErrorCode(Errors.NONE.code)
      ).asJava)
    )

    val cachedSharePartitions = new ImplicitLinkedHashCollection[CachedSharePartition]
    cachedSharePartitions.mustAdd(new CachedSharePartition(
      new TopicIdPartition(topicId, partitionIndex, topicName), false)
    )

    when(sharePartitionManager.newContext(any(), any(), any(), any(), any())).thenReturn(
      new ShareSessionContext(new ShareRequestMetadata(memberId, 0), util.List.of(
        new TopicIdPartition(topicId, partitionIndex, topicName)
      ))
    ).thenReturn(new ShareSessionContext(new ShareRequestMetadata(memberId, 1), new ShareSession(
      new ShareSessionKey(groupId, memberId), cachedSharePartitions, 0L, 0L, 2))
    ).thenReturn(new ShareSessionContext(new ShareRequestMetadata(memberId, 2), new ShareSession(
      new ShareSessionKey(groupId, memberId), cachedSharePartitions, 0L, 10L, 3))
    )

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    var shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(0).
      setTopics(util.List.of(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(util.List.of(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(partitionIndex)))))

    var shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    var request = buildRequest(shareFetchRequest)

    // First share fetch request is to establish the share session with the broker.
    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    kafkaApis.handleShareFetchRequest(request)
    var response = verifyNoThrottling[ShareFetchResponse](request)
    var responseData = response.data()
    var topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    assertEquals(topicId, topicResponses.get(0).topicId)
    assertEquals(1, topicResponses.get(0).partitions.size())

    compareResponsePartitions(
      partitionIndex,
      Errors.NONE.code,
      Errors.NONE.code,
      records1,
      expectedAcquiredRecords(0, 9, 1),
      topicResponses.get(0).partitions.get(0)
    )

    shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(1).
      setTopics(util.List.of(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(util.List.of(
          new ShareFetchRequestData.FetchPartition().
            setPartitionIndex(partitionIndex).
            setAcknowledgementBatches(util.List.of(
              new ShareFetchRequestData.AcknowledgementBatch().
                setFirstOffset(0).
                setLastOffset(9).
                setAcknowledgeTypes(List[java.lang.Byte](1.toByte).asJava)))))))

    shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    request = buildRequest(shareFetchRequest)

    kafkaApis.handleShareFetchRequest(request)
    response = verifyNoThrottling[ShareFetchResponse](request)
    responseData = response.data()
    topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    assertEquals(topicId, topicResponses.get(0).topicId)
    assertEquals(1, topicResponses.get(0).partitions.size())

    compareResponsePartitions(
      partitionIndex,
      Errors.NONE.code,
      Errors.NONE.code,
      records2,
      expectedAcquiredRecords(10, 19, 1),
      topicResponses.get(0).partitions.get(0)
    )

    shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(2).
      setTopics(util.List.of(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(util.List.of(
          new ShareFetchRequestData.FetchPartition().
            setPartitionIndex(partitionIndex).
            setAcknowledgementBatches(util.List.of(
              new ShareFetchRequestData.AcknowledgementBatch().
                setFirstOffset(10).
                setLastOffset(19).
                setAcknowledgeTypes(List[java.lang.Byte](1.toByte).asJava)))))))

    shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    request = buildRequest(shareFetchRequest)

    kafkaApis.handleShareFetchRequest(request)
    response = verifyNoThrottling[ShareFetchResponse](request)
    responseData = response.data()
    topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    assertEquals(topicId, topicResponses.get(0).topicId)
    assertEquals(1, topicResponses.get(0).partitions.size())

    compareResponsePartitions(
      partitionIndex,
      Errors.NONE.code,
      Errors.NONE.code,
      records3,
      expectedAcquiredRecords(20, 29, 1),
      topicResponses.get(0).partitions.get(0)
    )
  }

  @Test
  def testHandleShareFetchRequestSuccessfulShareSessionLifecycle(): Unit = {
    val topicName1 = "foo1"
    val topicId1 = Uuid.randomUuid()

    val topicName2 = "foo2"
    val topicId2 = Uuid.randomUuid()

    val topicName3 = "foo3"
    val topicId3 = Uuid.randomUuid()

    val topicName4 = "foo4"
    val topicId4 = Uuid.randomUuid()

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicName1, 2, topicId = topicId1)
    addTopicToMetadataCache(topicName2, 2, topicId = topicId2)
    addTopicToMetadataCache(topicName3, 1, topicId = topicId3)
    addTopicToMetadataCache(topicName4, 1, topicId = topicId4)
    val memberId: Uuid = Uuid.ZERO_UUID

    val records_t1_p1_1 = memoryRecords(10, 0)
    val records_t1_p2_1 = memoryRecords(10, 10)

    val records_t2_p1_1 = memoryRecords(10, 43)
    val records_t2_p2_1 = memoryRecords(10, 17)

    val records_t3_p1_1 = memoryRecords(20, 54)
    val records_t3_p1_2 = memoryRecords(20, 74)

    val records_t4_p1_1 = memoryRecords(15, 10)

    val groupId = "group"

    when(sharePartitionManager.fetchMessages(any(), any(), any(), anyInt(), anyInt(), any())).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareFetchResponseData.PartitionData](
        new TopicIdPartition(topicId1, new TopicPartition(topicName1, 0)) ->
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.NONE.code)
            .setRecords(records_t1_p1_1)
            .setAcquiredRecords(new util.ArrayList(List(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setDeliveryCount(1)
            ).asJava)),
        new TopicIdPartition(topicId1, new TopicPartition(topicName1, 1)) ->
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.NONE.code)
            .setRecords(records_t1_p2_1)
            .setAcquiredRecords(new util.ArrayList(List(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(10)
                .setLastOffset(19)
                .setDeliveryCount(1)
            ).asJava)),
        new TopicIdPartition(topicId2, new TopicPartition(topicName2, 0)) ->
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.NONE.code)
            .setRecords(records_t2_p1_1)
            .setAcquiredRecords(new util.ArrayList(List(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(43)
                .setLastOffset(52)
                .setDeliveryCount(1)
            ).asJava)),
        new TopicIdPartition(topicId2, new TopicPartition(topicName2, 1)) ->
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.NONE.code)
            .setRecords(records_t2_p2_1)
            .setAcquiredRecords(new util.ArrayList(List(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(17)
                .setLastOffset(26)
                .setDeliveryCount(1)
            ).asJava))
      ).asJava)
    ).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareFetchResponseData.PartitionData](
        new TopicIdPartition(topicId3, new TopicPartition(topicName3, 0)) ->
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.NONE.code)
            .setRecords(records_t3_p1_1)
            .setAcquiredRecords(new util.ArrayList(List(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(54)
                .setLastOffset(73)
                .setDeliveryCount(1)
            ).asJava))
      ).asJava)
    ).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareFetchResponseData.PartitionData](
        new TopicIdPartition(topicId3, new TopicPartition(topicName3, 0)) ->
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.NONE.code)
            .setRecords(records_t3_p1_2)
            .setAcquiredRecords(new util.ArrayList(List(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(74)
                .setLastOffset(93)
                .setDeliveryCount(1)
            ).asJava)),
        new TopicIdPartition(topicId4, new TopicPartition(topicName4, 0)) ->
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.NONE.code)
            .setRecords(records_t4_p1_1)
            .setAcquiredRecords(new util.ArrayList(List(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(10)
                .setLastOffset(24)
                .setDeliveryCount(1)
            ).asJava))
      ).asJava)
    )

    val cachedSharePartitions1 = new ImplicitLinkedHashCollection[CachedSharePartition]
    cachedSharePartitions1.mustAdd(new CachedSharePartition(
      new TopicIdPartition(topicId1, 0, topicName1), false
    ))
    cachedSharePartitions1.mustAdd(new CachedSharePartition(
      new TopicIdPartition(topicId1, 1, topicName1), false
    ))
    cachedSharePartitions1.mustAdd(new CachedSharePartition(
      new TopicIdPartition(topicId2, 0, topicName2), false
    ))
    cachedSharePartitions1.mustAdd(new CachedSharePartition(
      new TopicIdPartition(topicId2, 1, topicName2), false
    ))
    cachedSharePartitions1.mustAdd(new CachedSharePartition(
      new TopicIdPartition(topicId3, 0, topicName3), false
    ))

    val cachedSharePartitions2 = new ImplicitLinkedHashCollection[CachedSharePartition]
    cachedSharePartitions2.mustAdd(new CachedSharePartition(
      new TopicIdPartition(topicId3, 0, topicName3), false
    ))
    cachedSharePartitions2.mustAdd(new CachedSharePartition(
      new TopicIdPartition(topicId4, 0, topicName4), false
    ))

    when(sharePartitionManager.newContext(any(), any(), any(), any(), any())).thenReturn(
      new ShareSessionContext(new ShareRequestMetadata(memberId, 0), util.List.of(
        new TopicIdPartition(topicId1, new TopicPartition(topicName1, 0)),
        new TopicIdPartition(topicId1, new TopicPartition(topicName1, 1)),
        new TopicIdPartition(topicId2, new TopicPartition(topicName2, 0)),
        new TopicIdPartition(topicId2, new TopicPartition(topicName2, 1))
      ))
    ).thenReturn(new ShareSessionContext(new ShareRequestMetadata(memberId, 1), new ShareSession(
      new ShareSessionKey(groupId, memberId), cachedSharePartitions1, 0L, 0L, 2))
    ).thenReturn(new ShareSessionContext(new ShareRequestMetadata(memberId, 2), new ShareSession(
      new ShareSessionKey(groupId, memberId), cachedSharePartitions2, 0L, 0L, 3))
    ).thenReturn(new FinalContext())

    when(sharePartitionManager.releaseSession(any(), any())).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData](
        new TopicIdPartition(topicId3, new TopicPartition(topicName3, 0)) ->
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code),
        new TopicIdPartition(topicId4, new TopicPartition(topicName4, 0)) ->
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code)
      ).asJava)
    )

    when(sharePartitionManager.acknowledge(any(), any(), any())).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData](
        new TopicIdPartition(topicId1, new TopicPartition(topicName1, 0)) ->
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code),
        new TopicIdPartition(topicId1, new TopicPartition(topicName1, 1)) ->
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(1)
            .setErrorCode(Errors.NONE.code),
        new TopicIdPartition(topicId2, new TopicPartition(topicName2, 0)) ->
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code),
        new TopicIdPartition(topicId2, new TopicPartition(topicName2, 1)) ->
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(1)
            .setErrorCode(Errors.NONE.code),
        new TopicIdPartition(topicId3, new TopicPartition(topicName3, 0)) ->
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code),
        new TopicIdPartition(topicId4, new TopicPartition(topicName4, 0)) ->
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code),
      ).asJava)
    )

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    var shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(0).
      setTopics(util.List.of(
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId1).
          setPartitions(util.List.of(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0),
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(1)
          )),
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId2).
          setPartitions(util.List.of(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0),
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(1)
          ))
      ))

    var shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    var request = buildRequest(shareFetchRequest)
    // First share fetch request is to establish the share session with the broker.
    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    kafkaApis.handleShareFetchRequest(request)
    var response = verifyNoThrottling[ShareFetchResponse](request)
    var responseData = response.data()
    var topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(2, topicResponses.size())
    var topicResponsesScala = topicResponses.asScala.toList
    var topicResponsesMap: Map[Uuid, ShareFetchableTopicResponse] = topicResponsesScala.map(topic => topic.topicId -> topic).toMap
    assertTrue(topicResponsesMap.contains(topicId1))
    val topicIdResponse1: ShareFetchableTopicResponse = topicResponsesMap.getOrElse(topicId1, null)
    assertEquals(2, topicIdResponse1.partitions.size())
    val partitionsScala1 = topicIdResponse1.partitions.asScala.toList
    val partitionsMap1: Map[Int, PartitionData] = partitionsScala1.map(partition => partition.partitionIndex -> partition).toMap
    assertTrue(partitionsMap1.contains(0))
    val partition11: PartitionData = partitionsMap1.getOrElse(0, null)

    compareResponsePartitions(
      0,
      Errors.NONE.code,
      Errors.NONE.code,
      records_t1_p1_1,
      expectedAcquiredRecords(0, 9, 1),
      partition11
    )

    assertTrue(partitionsMap1.contains(1))
    val partition12: PartitionData = partitionsMap1.getOrElse(1, null)

    compareResponsePartitions(
      1,
      Errors.NONE.code,
      Errors.NONE.code,
      records_t1_p2_1,
      expectedAcquiredRecords(10, 19, 1),
      partition12
    )

    assertTrue(topicResponsesMap.contains(topicId2))
    val topicIdResponse2: ShareFetchableTopicResponse = topicResponsesMap.getOrElse(topicId2, null)
    assertEquals(2, topicIdResponse2.partitions.size())
    val partitionsScala2 = topicIdResponse2.partitions.asScala.toList
    val partitionsMap2: Map[Int, PartitionData] = partitionsScala2.map(partition => partition.partitionIndex -> partition).toMap
    assertTrue(partitionsMap2.contains(0))
    val partition21: PartitionData = partitionsMap2.getOrElse(0, null)

    compareResponsePartitions(
      0,
      Errors.NONE.code,
      Errors.NONE.code,
      records_t2_p1_1,
      expectedAcquiredRecords(43, 52, 1),
      partition21
    )

    assertTrue(partitionsMap2.contains(1))
    val partition22: PartitionData = partitionsMap2.getOrElse(1, null)

    compareResponsePartitions(
      1,
      Errors.NONE.code,
      Errors.NONE.code,
      records_t2_p2_1,
      expectedAcquiredRecords(17, 26, 1),
      partition22
    )

    shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(1).
      setTopics(util.List.of(
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId3).
          setPartitions(util.List.of(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0)
          )),
      ))

    shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    request = buildRequest(shareFetchRequest)
    kafkaApis.handleShareFetchRequest(request)
    response = verifyNoThrottling[ShareFetchResponse](request)
    responseData = response.data()
    topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    assertEquals(topicId3, topicResponses.get(0).topicId)
    assertEquals(1, topicResponses.get(0).partitions.size())

    compareResponsePartitions(
      0,
      Errors.NONE.code,
      Errors.NONE.code,
      records_t3_p1_1,
      expectedAcquiredRecords(54, 73, 1),
      topicResponses.get(0).partitions.get(0)
    )

    shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(2).
      setTopics(util.List.of(
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId4).
          setPartitions(util.List.of(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0)
          )),
      ))
      .setForgottenTopicsData(util.List.of(
        new ForgottenTopic()
          .setTopicId(topicId1)
          .setPartitions(util.List.of(Integer.valueOf(0), Integer.valueOf(1))),
        new ForgottenTopic()
          .setTopicId(topicId2)
          .setPartitions(util.List.of(Integer.valueOf(0), Integer.valueOf(1)))
      ))

    shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    request = buildRequest(shareFetchRequest)
    kafkaApis.handleShareFetchRequest(request)
    response = verifyNoThrottling[ShareFetchResponse](request)
    responseData = response.data()
    topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(2, topicResponses.size())
    topicResponsesScala = topicResponses.asScala.toList
    topicResponsesMap = topicResponsesScala.map(topic => topic.topicId -> topic).toMap
    assertTrue(topicResponsesMap.contains(topicId3))
    val topicIdResponse3 = topicResponsesMap.getOrElse(topicId3, null)
    assertEquals(1, topicIdResponse3.partitions.size())

    compareResponsePartitions(
      0,
      Errors.NONE.code,
      Errors.NONE.code,
      records_t3_p1_2,
      expectedAcquiredRecords(74, 93, 1),
      topicIdResponse3.partitions.get(0)
    )

    assertTrue(topicResponsesMap.contains(topicId4))
    val topicIdResponse4 = topicResponsesMap.getOrElse(topicId4, null)
    assertEquals(1, topicIdResponse4.partitions.size())

    compareResponsePartitions(
      0,
      Errors.NONE.code,
      Errors.NONE.code,
      records_t4_p1_1,
      expectedAcquiredRecords(10, 24, 1),
      topicIdResponse4.partitions.get(0)
    )

    // Final request with acknowledgements.
    shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(-1).
      setTopics(util.List.of(
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId1).
          setPartitions(util.List.of(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0)
              .setAcknowledgementBatches(util.List.of(
                new AcknowledgementBatch()
                  .setFirstOffset(0)
                  .setLastOffset(9)
                  .setAcknowledgeTypes(util.List.of(1.toByte)),
              )),
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(1)
              .setAcknowledgementBatches(util.List.of(
                new AcknowledgementBatch()
                  .setFirstOffset(10)
                  .setLastOffset(19)
                  .setAcknowledgeTypes(util.List.of(1.toByte)),
              ))
          )),
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId2).
          setPartitions(util.List.of(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0)
              .setAcknowledgementBatches(util.List.of(
                new AcknowledgementBatch()
                  .setFirstOffset(43)
                  .setLastOffset(52)
                  .setAcknowledgeTypes(util.List.of(1.toByte)),
              )),
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(1)
              .setAcknowledgementBatches(util.List.of(
                new AcknowledgementBatch()
                  .setFirstOffset(17)
                  .setLastOffset(26)
                  .setAcknowledgeTypes(util.List.of(1.toByte)),
              ))
          )),
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId3).
          setPartitions(util.List.of(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0)
              .setAcknowledgementBatches(util.List.of(
                new AcknowledgementBatch()
                  .setFirstOffset(54)
                  .setLastOffset(93)
                  .setAcknowledgeTypes(util.List.of(1.toByte)),
              )),
          )),
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId4).
          setPartitions(util.List.of(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0)
              .setAcknowledgementBatches(util.List.of(
                new AcknowledgementBatch()
                  .setFirstOffset(10)
                  .setLastOffset(24)
                  .setAcknowledgeTypes(util.List.of(1.toByte)),
              )),
          )),
      ))

    shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    request = buildRequest(shareFetchRequest)
    kafkaApis.handleShareFetchRequest(request)
    response = verifyNoThrottling[ShareFetchResponse](request)
    responseData = response.data()
    topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
  }

  @Test
  def testHandleFetchFromShareFetchRequestSuccess(): Unit = {
    val shareSessionEpoch = 0
    val topicName1 = "foo1"
    val topicName2 = "foo2"
    val topicId1 = Uuid.randomUuid()
    val topicId2 = Uuid.randomUuid()

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicName1, 1, topicId = topicId1)
    addTopicToMetadataCache(topicName2, 2, topicId = topicId2)

    val memberId: Uuid = Uuid.ZERO_UUID
    val groupId: String = "group"

    val records_t1_p1 = memoryRecords(10, 0)
    val records_t2_p1 = memoryRecords(15, 0)
    val records_t2_p2 = memoryRecords(20, 0)

    val tp1 = new TopicIdPartition(topicId1, new TopicPartition(topicName1, 0))
    val tp2 = new TopicIdPartition(topicId2, new TopicPartition(topicName2, 0))
    val tp3 = new TopicIdPartition(topicId2, new TopicPartition(topicName2, 1))

    when(sharePartitionManager.fetchMessages(any(), any(), any(), anyInt(), anyInt(), any())).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareFetchResponseData.PartitionData](
        tp1 ->
          new ShareFetchResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code)
            .setRecords(records_t1_p1)
            .setAcquiredRecords(new util.ArrayList(List(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setDeliveryCount(1)
            ).asJava)),
        tp2 ->
          new ShareFetchResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code)
            .setRecords(records_t2_p1)
            .setAcquiredRecords(new util.ArrayList(List(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(0)
                .setLastOffset(14)
                .setDeliveryCount(1)
            ).asJava)),
        tp3 ->
          new ShareFetchResponseData.PartitionData()
            .setPartitionIndex(1)
            .setErrorCode(Errors.NONE.code)
            .setRecords(records_t2_p2)
            .setAcquiredRecords(new util.ArrayList(List(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(0)
                .setLastOffset(19)
                .setDeliveryCount(1)
            ).asJava)),
      ).asJava)
    )

    val erroneousPartitions: util.Map[TopicIdPartition, ShareFetchResponseData.PartitionData] = new util.HashMap()

    val validPartitions: util.List[TopicIdPartition] = util.List.of(tp1, tp2, tp3)

    val erroneousAndValidPartitionData: ErroneousAndValidPartitionData =
      new ErroneousAndValidPartitionData(erroneousPartitions, validPartitions)

    var authorizedTopics: Set[String] = Set.empty[String]
    authorizedTopics = authorizedTopics + topicName1
    authorizedTopics = authorizedTopics + topicName2

    val shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(shareSessionEpoch).
      setTopics(util.List.of(
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId1).
          setPartitions(util.List.of(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0)
          )),
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId2).
          setPartitions(util.List.of(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0),
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(1)
          )),
      ))

    val shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    val request = buildRequest(shareFetchRequest)
    // First share fetch request is to establish the share session with the broker.
    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    val fetchResult: Map[TopicIdPartition, ShareFetchResponseData.PartitionData] =
      kafkaApis.handleFetchFromShareFetchRequest(
        request,
        0,
        erroneousAndValidPartitionData,
        sharePartitionManager,
        authorizedTopics
      ).get()

    assertEquals(3, fetchResult.size)

    assertTrue(fetchResult.contains(tp1))
    val partitionData1: PartitionData = fetchResult.getOrElse(tp1, null)
    assertNotNull(partitionData1)

    compareResponsePartitions(
      0,
      Errors.NONE.code,
      Errors.NONE.code,
      records_t1_p1,
      expectedAcquiredRecords(0, 9, 1),
      partitionData1
    )

    assertTrue(fetchResult.contains(tp2))
    val partitionData2: PartitionData = fetchResult.getOrElse(tp2, null)
    assertNotNull(partitionData2)

    compareResponsePartitions(
      0,
      Errors.NONE.code,
      Errors.NONE.code,
      records_t2_p1,
      expectedAcquiredRecords(0, 14, 1),
      partitionData2
    )

    assertTrue(fetchResult.contains(tp3))
    val partitionData3: PartitionData = fetchResult.getOrElse(tp3, null)
    assertNotNull(partitionData3)

    compareResponsePartitions(
      1,
      Errors.NONE.code,
      Errors.NONE.code,
      records_t2_p2,
      expectedAcquiredRecords(0, 19, 1),
      partitionData3
    )
  }

  @Test
  def testHandleShareFetchFromShareFetchRequestWithErroneousPartitions(): Unit = {
    val shareSessionEpoch = 0
    val topicName1 = "foo1"
    val topicName2 = "foo2"
    val topicId1 = Uuid.randomUuid()
    val topicId2 = Uuid.randomUuid()

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicName1, 1, topicId = topicId1)
    addTopicToMetadataCache(topicName2, 2, topicId = topicId2)

    val memberId: Uuid = Uuid.ZERO_UUID
    val groupId: String = "group"

    val records_t1_p1 = memoryRecords(10, 0)

    val tp1 = new TopicIdPartition(topicId1, new TopicPartition(topicName1, 0))
    val tp2 = new TopicIdPartition(topicId1, new TopicPartition(topicName1, 1))
    val tp3 = new TopicIdPartition(topicId2, new TopicPartition(topicName2, 0))

    when(sharePartitionManager.fetchMessages(any(), any(), any(), anyInt(), anyInt(), any())).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareFetchResponseData.PartitionData](
        tp1 ->
          new ShareFetchResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code)
            .setRecords(records_t1_p1)
            .setAcquiredRecords(new util.ArrayList(List(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setDeliveryCount(1)
            ).asJava))
      ).asJava)
    )

    val erroneousPartitions: util.Map[TopicIdPartition, ShareFetchResponseData.PartitionData] = new util.HashMap()
    erroneousPartitions.put(
      tp2,
      new ShareFetchResponseData.PartitionData()
        .setPartitionIndex(1)
        .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
    )
    erroneousPartitions.put(
      tp3,
      new ShareFetchResponseData.PartitionData()
        .setPartitionIndex(0)
        .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
    )

    val validPartitions: util.List[TopicIdPartition] = util.List.of(tp1)

    val erroneousAndValidPartitionData: ErroneousAndValidPartitionData =
      new ErroneousAndValidPartitionData(erroneousPartitions, validPartitions)

    var authorizedTopics: Set[String] = Set.empty[String]
    authorizedTopics = authorizedTopics + topicName1
    authorizedTopics = authorizedTopics + topicName2

    val shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(shareSessionEpoch).
      setTopics(util.List.of(
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId1).
          setPartitions(util.List.of(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0),
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(1)
          )),
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId2).
          setPartitions(util.List.of(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0),
          )),
      ))

    val shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    val request = buildRequest(shareFetchRequest)
    // First share fetch request is to establish the share session with the broker.
    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    val fetchResult: Map[TopicIdPartition, ShareFetchResponseData.PartitionData] =
      kafkaApis.handleFetchFromShareFetchRequest(
        request,
        0,
        erroneousAndValidPartitionData,
        sharePartitionManager,
        authorizedTopics
      ).get()

    assertEquals(3, fetchResult.size)

    assertTrue(fetchResult.contains(tp1))
    val partitionData1: PartitionData = fetchResult.getOrElse(tp1, null)
    assertNotNull(partitionData1)

    compareResponsePartitions(
      0,
      Errors.NONE.code,
      Errors.NONE.code,
      records_t1_p1,
      expectedAcquiredRecords(0, 9, 1),
      partitionData1
    )

    assertTrue(fetchResult.contains(tp2))
    val partitionData2: PartitionData = fetchResult.getOrElse(tp2, null)
    assertNotNull(partitionData2)

    compareResponsePartitionsFetchError(
      1,
      Errors.UNKNOWN_TOPIC_OR_PARTITION.code,
      partitionData2
    )

    assertTrue(fetchResult.contains(tp3))
    val partitionData3: PartitionData = fetchResult.getOrElse(tp3, null)
    assertNotNull(partitionData3)

    compareResponsePartitionsFetchError(
      0,
      Errors.UNKNOWN_TOPIC_OR_PARTITION.code,
      partitionData3
    )
  }

  @Test
  def testHandleShareFetchFetchMessagesReturnErrorCode(): Unit = {
    val shareSessionEpoch = 0
    val topicName1 = "foo1"
    val topicName2 = "foo2"
    val topicId1 = Uuid.randomUuid()
    val topicId2 = Uuid.randomUuid()

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicName1, 1, topicId = topicId1)
    addTopicToMetadataCache(topicName2, 2, topicId = topicId2)

    val memberId: Uuid = Uuid.ZERO_UUID
    val groupId: String = "group"

    val emptyRecords = MemoryRecords.EMPTY

    val tp1 = new TopicIdPartition(topicId1, new TopicPartition(topicName1, 0))
    val tp2 = new TopicIdPartition(topicId2, new TopicPartition(topicName2, 0))
    val tp3 = new TopicIdPartition(topicId2, new TopicPartition(topicName2, 1))

    when(sharePartitionManager.fetchMessages(any(), any(), any(), anyInt(), anyInt(), any())).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareFetchResponseData.PartitionData](
        tp1 ->
          new ShareFetchResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code)
            .setRecords(emptyRecords)
            .setAcquiredRecords(new util.ArrayList(List().asJava)),
        tp2 ->
          new ShareFetchResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code)
            .setRecords(emptyRecords)
            .setAcquiredRecords(new util.ArrayList(List().asJava)),
        tp3 ->
          new ShareFetchResponseData.PartitionData()
            .setPartitionIndex(1)
            .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code)
            .setRecords(emptyRecords)
            .setAcquiredRecords(new util.ArrayList(List().asJava))
      ).asJava)
    )

    val erroneousPartitions: util.Map[TopicIdPartition, ShareFetchResponseData.PartitionData] = new util.HashMap()

    val validPartitions: util.List[TopicIdPartition] = util.List.of(tp1, tp2, tp3)

    val erroneousAndValidPartitionData: ErroneousAndValidPartitionData =
      new ErroneousAndValidPartitionData(erroneousPartitions, validPartitions)

    var authorizedTopics: Set[String] = Set.empty[String]
    authorizedTopics = authorizedTopics + topicName1
    authorizedTopics = authorizedTopics + topicName2

    val shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(shareSessionEpoch).
      setTopics(util.List.of(
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId1).
          setPartitions(util.List.of(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0)
          )),
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId2).
          setPartitions(util.List.of(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0),
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(1)
          )),
      ))

    val shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    val request = buildRequest(shareFetchRequest)
    // First share fetch request is to establish the share session with the broker.
    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    val fetchResult: Map[TopicIdPartition, ShareFetchResponseData.PartitionData] =
      kafkaApis.handleFetchFromShareFetchRequest(
        request,
        0,
        erroneousAndValidPartitionData,
        sharePartitionManager,
        authorizedTopics
      ).get()

    assertEquals(3, fetchResult.size)

    assertTrue(fetchResult.contains(tp1))
    val partitionData1: PartitionData = fetchResult.getOrElse(tp1, null)
    assertNotNull(partitionData1)

    compareResponsePartitions(
      0,
      Errors.UNKNOWN_SERVER_ERROR.code,
      Errors.NONE.code,
      emptyRecords,
      Collections.emptyList[AcquiredRecords](),
      partitionData1
    )

    assertTrue(fetchResult.contains(tp2))
    val partitionData2: PartitionData = fetchResult.getOrElse(tp2, null)
    assertNotNull(partitionData2)

    compareResponsePartitions(
      0,
      Errors.UNKNOWN_SERVER_ERROR.code,
      Errors.NONE.code,
      emptyRecords,
      Collections.emptyList[AcquiredRecords](),
      partitionData2
    )

    assertTrue(fetchResult.contains(tp3))
    val partitionData3: PartitionData = fetchResult.getOrElse(tp3, null)
    assertNotNull(partitionData3)

    compareResponsePartitions(
      1,
      Errors.UNKNOWN_SERVER_ERROR.code,
      Errors.NONE.code,
      emptyRecords,
      Collections.emptyList[AcquiredRecords](),
      partitionData3
    )
  }

  @Test
  def testHandleShareFetchFromShareFetchRequestErrorTopicsInRequest(): Unit = {
    val shareSessionEpoch = 0
    val topicName1 = "foo1"
    val topicName2 = "foo2"
    val topicName3 = "foo3"
    val topicId1 = Uuid.randomUuid()
    val topicId2 = Uuid.randomUuid()
    val topicId3 = Uuid.randomUuid()

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicName1, 1, topicId = topicId1)
    addTopicToMetadataCache(topicName2, 2, topicId = topicId2)
    // topicName3 is not in the metadataCache.

    val memberId: Uuid = Uuid.ZERO_UUID
    val groupId: String = "group"

    val records1 = memoryRecords(10, 0)
    val records2 = memoryRecords(20, 0)

    val tp1 = new TopicIdPartition(topicId1, new TopicPartition(topicName1, 0))
    val tp2 = new TopicIdPartition(topicId2, new TopicPartition(topicName2, 0))
    val tp3 = new TopicIdPartition(topicId2, new TopicPartition(topicName2, 1))
    val tp4 = new TopicIdPartition(topicId3, new TopicPartition(topicName3, 0))

    when(sharePartitionManager.fetchMessages(any(), any(), any(), anyInt(), anyInt(), any())).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareFetchResponseData.PartitionData](
        tp2 ->
          new ShareFetchResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code)
            .setRecords(records1)
            .setAcquiredRecords(new util.ArrayList(List(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setDeliveryCount(1)
            ).asJava)),
        tp3 ->
          new ShareFetchResponseData.PartitionData()
            .setPartitionIndex(1)
            .setErrorCode(Errors.NONE.code)
            .setRecords(records2)
            .setAcquiredRecords(new util.ArrayList(List(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(0)
                .setLastOffset(19)
                .setDeliveryCount(1)
            ).asJava))
      ).asJava)
    )

    val erroneousPartitions: util.Map[TopicIdPartition, ShareFetchResponseData.PartitionData] = new util.HashMap()

    val validPartitions: util.List[TopicIdPartition] = util.List.of(tp1, tp2, tp3, tp4)

    val erroneousAndValidPartitionData: ErroneousAndValidPartitionData =
      new ErroneousAndValidPartitionData(erroneousPartitions, validPartitions)

    var authorizedTopics: Set[String] = Set.empty[String]
    // topicName1 is not in authorizedTopic.
    authorizedTopics = authorizedTopics + topicName2
    authorizedTopics = authorizedTopics + topicName3

    val shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(shareSessionEpoch).
      setTopics(util.List.of(
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId1).
          setPartitions(util.List.of(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0)
          )),
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId2).
          setPartitions(util.List.of(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0),
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(1)
          )),
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId3).
          setPartitions(util.List.of(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0)
          )),
      ))

    val shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    val request = buildRequest(shareFetchRequest)
    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    val fetchResult: Map[TopicIdPartition, ShareFetchResponseData.PartitionData] =
      kafkaApis.handleFetchFromShareFetchRequest(
        request,
        0,
        erroneousAndValidPartitionData,
        sharePartitionManager,
        authorizedTopics
      ).get()

    assertEquals(4, fetchResult.size)

    assertTrue(fetchResult.contains(tp1))
    val partitionData1: PartitionData = fetchResult.getOrElse(tp1, null)
    assertNotNull(partitionData1)

    compareResponsePartitions(
      0,
      Errors.TOPIC_AUTHORIZATION_FAILED.code,
      Errors.NONE.code,
      null,
      Collections.emptyList[AcquiredRecords](),
      partitionData1
    )

    assertTrue(fetchResult.contains(tp2))
    val partitionData2: PartitionData = fetchResult.getOrElse(tp2, null)
    assertNotNull(partitionData2)

    compareResponsePartitions(
      0,
      Errors.NONE.code,
      Errors.NONE.code,
      records1,
      expectedAcquiredRecords(0, 9, 1),
      partitionData2
    )

    assertTrue(fetchResult.contains(tp3))
    val partitionData3: PartitionData = fetchResult.getOrElse(tp3, null)
    assertNotNull(partitionData3)

    compareResponsePartitions(
      1,
      Errors.NONE.code,
      Errors.NONE.code,
      records2,
      expectedAcquiredRecords(0, 19, 1),
      partitionData3
    )

    assertTrue(fetchResult.contains(tp4))
    val partitionData4: PartitionData = fetchResult.getOrElse(tp4, null)
    assertNotNull(partitionData4)

    compareResponsePartitions(
      0,
      Errors.UNKNOWN_TOPIC_OR_PARTITION.code,
      Errors.NONE.code,
      null,
      Collections.emptyList[AcquiredRecords](),
      partitionData4
    )
  }

  private def compareResponsePartitions(expPartitionIndex: Int,
                                        expErrorCode: Short,
                                        expAckErrorCode: Short,
                                        expRecords: MemoryRecords,
                                        expAcquiredRecords: util.List[AcquiredRecords],
                                        partitionData: PartitionData): Unit = {
    assertEquals(expPartitionIndex, partitionData.partitionIndex)
    assertEquals(expErrorCode, partitionData.errorCode)
    assertEquals(expAckErrorCode, partitionData.acknowledgeErrorCode)
    assertEquals(expRecords, partitionData.records)
    assertArrayEquals(expAcquiredRecords.toArray(), partitionData.acquiredRecords.toArray())
  }

  private def compareResponsePartitionsFetchError(
                                                   expPartitionIndex: Int,
                                                   expErrorCode: Short,
                                                   partitionData: PartitionData
                                                 ): Unit = {
    assertEquals(expPartitionIndex, partitionData.partitionIndex)
    assertEquals(expErrorCode, partitionData.errorCode)
  }

  @Test
  def testHandleShareFetchRequestSuccessWithAcknowledgements(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: Uuid = Uuid.randomUuid()

    val records1 = memoryRecords(10, 0)
    val records2 = memoryRecords(10, 10)

    val groupId = "group"

    when(sharePartitionManager.fetchMessages(any(), any(), any(), anyInt(), anyInt(), any())).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareFetchResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)) ->
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.NONE.code)
            .setAcknowledgeErrorCode(Errors.NONE.code)
            .setRecords(records1)
            .setAcquiredRecords(new util.ArrayList(List(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setDeliveryCount(1)
            ).asJava))
      ).asJava)
    ).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareFetchResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)) ->
          new ShareFetchResponseData.PartitionData()
            .setErrorCode(Errors.NONE.code)
            .setAcknowledgeErrorCode(Errors.NONE.code)
            .setRecords(records2)
            .setAcquiredRecords(new util.ArrayList(List(
              new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(10)
                .setLastOffset(19)
                .setDeliveryCount(1)
            ).asJava))
      ).asJava)
    )

    val cachedSharePartitions = new ImplicitLinkedHashCollection[CachedSharePartition]
    cachedSharePartitions.mustAdd(new CachedSharePartition(
      new TopicIdPartition(topicId, 0, topicName), false
    ))

    when(sharePartitionManager.newContext(any(), any(), any(), any(), any())).thenReturn(
      new ShareSessionContext(new ShareRequestMetadata(memberId, 0), util.List.of(
        new TopicIdPartition(topicId, partitionIndex, topicName)
      ))
    ).thenReturn(new ShareSessionContext(new ShareRequestMetadata(memberId, 1), new ShareSession(
      new ShareSessionKey(groupId, memberId), cachedSharePartitions, 0L, 0L, 2))
    )

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    when(sharePartitionManager.acknowledge(any(), any(), any())).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, 0)) ->
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code),
      ).asJava)
    )

    var shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(0).
      setTopics(util.List.of(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(util.List.of(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(0)))))

    var shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    var request = buildRequest(shareFetchRequest)
    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    kafkaApis.handleShareFetchRequest(request)
    var response = verifyNoThrottling[ShareFetchResponse](request)
    var responseData = response.data()
    var topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    assertEquals(topicId, topicResponses.get(0).topicId)
    assertEquals(1, topicResponses.get(0).partitions.size())
    assertEquals(partitionIndex, topicResponses.get(0).partitions.get(0).partitionIndex)
    assertEquals(Errors.NONE.code, topicResponses.get(0).partitions.get(0).errorCode)
    assertEquals(records1, topicResponses.get(0).partitions.get(0).records)
    assertArrayEquals(expectedAcquiredRecords(0, 9, 1).toArray(), topicResponses.get(0).partitions.get(0).acquiredRecords.toArray())

    shareFetchRequestData = new ShareFetchRequestData().
      setGroupId("group").
      setMemberId(memberId.toString).
      setShareSessionEpoch(1).
      setTopics(util.List.of(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(util.List.of(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(0)
            .setAcknowledgementBatches(util.List.of(
              new AcknowledgementBatch()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setAcknowledgeTypes(util.List.of(1.toByte))
            ))
        ))
      ))

    shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    request = buildRequest(shareFetchRequest)
    kafkaApis.handleShareFetchRequest(request)
    response = verifyNoThrottling[ShareFetchResponse](request)
    responseData = response.data()
    topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    assertEquals(topicId, topicResponses.get(0).topicId)
    assertEquals(1, topicResponses.get(0).partitions.size())
    assertEquals(partitionIndex, topicResponses.get(0).partitions.get(0).partitionIndex)
    assertEquals(Errors.NONE.code, topicResponses.get(0).partitions.get(0).errorCode)
    assertEquals(Errors.NONE.code, topicResponses.get(0).partitions.get(0).acknowledgeErrorCode)
    assertEquals(records2, topicResponses.get(0).partitions.get(0).records)
    assertArrayEquals(expectedAcquiredRecords(10, 19, 1).toArray(), topicResponses.get(0).partitions.get(0).acquiredRecords.toArray())
  }

  @Test
  def testHandleShareFetchNewGroupCoordinatorDisabled(): Unit = {
    val topicId = Uuid.randomUuid()
    val memberId: Uuid = Uuid.randomUuid()
    val groupId = "group"

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)

    val shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(1).
      setTopics(util.List.of(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(util.List.of(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(0)
            .setAcknowledgementBatches(util.List.of(
              new AcknowledgementBatch()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setAcknowledgeTypes(util.List.of(1.toByte))
            ))
        ))
      ))

    val shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    val request = buildRequest(shareFetchRequest)

    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        GroupCoordinatorConfig.NEW_GROUP_COORDINATOR_ENABLE_CONFIG -> "false",
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    kafkaApis.handleShareFetchRequest(request)

    val response = verifyNoThrottling[ShareFetchResponse](request)
    val responseData = response.data()

    assertEquals(Errors.UNSUPPORTED_VERSION.code, responseData.errorCode)
  }

  @Test
  def testHandleShareFetchShareGroupDisabled(): Unit = {
    val topicId = Uuid.randomUuid()
    val memberId: Uuid = Uuid.randomUuid()
    val groupId = "group"

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)

    val shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(1).
      setTopics(util.List.of(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(util.List.of(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(0)
            .setAcknowledgementBatches(util.List.of(
              new AcknowledgementBatch()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setAcknowledgeTypes(util.List.of(1.toByte))
            ))
        ))
      ))

    val shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    val request = buildRequest(shareFetchRequest)

    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "false"),
      )
    kafkaApis.handleShareFetchRequest(request)

    val response = verifyNoThrottling[ShareFetchResponse](request)
    val responseData = response.data()

    assertEquals(Errors.UNSUPPORTED_VERSION.code, responseData.errorCode)
  }

  @Test
  def testHandleShareFetchRequestGroupAuthorizationError(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: Uuid = Uuid.ZERO_UUID

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    val shareFetchRequestData = new ShareFetchRequestData().
      setGroupId("group").
      setMemberId(memberId.toString).
      setShareSessionEpoch(1).
      setTopics(util.List.of(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(util.List.of(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(partitionIndex)
            .setAcknowledgementBatches(util.List.of(
              new ShareFetchRequestData.AcknowledgementBatch()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setAcknowledgeTypes(util.List.of(1.toByte))
            ))
        ))
      ))

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any(), any())).thenReturn(List[AuthorizationResult](
      AuthorizationResult.DENIED
    ).asJava)

    val shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData)
      .build(ApiKeys.SHARE_ACKNOWLEDGE.latestVersion)
    val request = buildRequest(shareFetchRequest)
    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      authorizer = Option(authorizer),
      )
    kafkaApis.handleShareFetchRequest(request)

    val response = verifyNoThrottling[ShareFetchResponse](request)
    val responseData = response.data()

    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code, responseData.errorCode)
  }

  @Test
  def testHandleShareFetchRequestReleaseAcquiredRecordsThrowError(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: Uuid = Uuid.ZERO_UUID

    val groupId = "group"

    when(sharePartitionManager.acknowledge(any(), any(), any())).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, 0)) ->
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code),
      ).asJava)
    )

    when(sharePartitionManager.newContext(any(), any(), any(), any(), any())).thenReturn(
      new FinalContext()
    )

    when(sharePartitionManager.releaseSession(any(), any())).thenReturn(
      FutureUtils.failedFuture[util.Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]](Errors.UNKNOWN_SERVER_ERROR.exception())
    )

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    val shareFetchRequestData = new ShareFetchRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(ShareRequestMetadata.FINAL_EPOCH).
      setTopics(List(new ShareFetchRequestData.FetchTopic().
        setTopicId(topicId).
        setPartitions(List(
          new ShareFetchRequestData.FetchPartition()
            .setPartitionIndex(0)
            .setAcknowledgementBatches(List(
              new AcknowledgementBatch()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setAcknowledgeTypes(Collections.singletonList(1.toByte))
            ).asJava)
        ).asJava)).asJava)

    val shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    val request = buildRequest(shareFetchRequest)
    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    kafkaApis.handleShareFetchRequest(request)
    val response = verifyNoThrottling[ShareFetchResponse](request)
    val responseData = response.data()
    val topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    assertEquals(topicId, topicResponses.get(0).topicId)
    assertEquals(1, topicResponses.get(0).partitions.size())
    assertEquals(partitionIndex, topicResponses.get(0).partitions.get(0).partitionIndex)
    assertEquals(Errors.NONE.code, topicResponses.get(0).partitions.get(0).errorCode)
    assertEquals(Errors.NONE.code, topicResponses.get(0).partitions.get(0).acknowledgeErrorCode)
    assertNull(topicResponses.get(0).partitions.get(0).records)
    assertEquals(0, topicResponses.get(0).partitions.get(0).acquiredRecords.toArray().length)
  }

  @Test
  def testHandleShareAcknowledgeRequestSuccess(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: Uuid = Uuid.randomUuid()

    val groupId = "group"

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    when(sharePartitionManager.acknowledge(any(), any(), any())).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, 0)) ->
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code)
      ).asJava)
    )

    doNothing().when(sharePartitionManager).acknowledgeSessionUpdate(any(), any())

    val shareAcknowledgeRequestData = new ShareAcknowledgeRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(1).
      setTopics(List(new ShareAcknowledgeRequestData.AcknowledgeTopic().
        setTopicId(topicId).
        setPartitions(List(
          new ShareAcknowledgeRequestData.AcknowledgePartition()
            .setPartitionIndex(partitionIndex)
            .setAcknowledgementBatches(List(
              new ShareAcknowledgeRequestData.AcknowledgementBatch()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setAcknowledgeTypes(Collections.singletonList(1.toByte))
            ).asJava)
        ).asJava)
      ).asJava)

    val shareAcknowledgeRequest = new ShareAcknowledgeRequest.Builder(shareAcknowledgeRequestData)
      .build(ApiKeys.SHARE_ACKNOWLEDGE.latestVersion)
    val request = buildRequest(shareAcknowledgeRequest)
    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    kafkaApis.handleShareAcknowledgeRequest(request)
    val response = verifyNoThrottling[ShareAcknowledgeResponse](request)
    val responseData = response.data()
    val topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    assertEquals(topicId, topicResponses.get(0).topicId)
    assertEquals(1, topicResponses.get(0).partitions.size())
    assertEquals(partitionIndex, topicResponses.get(0).partitions.get(0).partitionIndex)
    assertEquals(Errors.NONE.code, topicResponses.get(0).partitions.get(0).errorCode)
  }

  @Test
  def testHandleShareAcknowledgeNewGroupCoordinatorDisabled(): Unit = {
    val topicId = Uuid.randomUuid()
    val memberId: Uuid = Uuid.randomUuid()
    val groupId = "group"

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)

    val shareAcknowledgeRequestData = new ShareAcknowledgeRequestData()
      .setGroupId(groupId)
      .setMemberId(memberId.toString)
      .setShareSessionEpoch(1)
      .setTopics(List(new ShareAcknowledgeRequestData.AcknowledgeTopic()
        .setTopicId(topicId)
        .setPartitions(List(
          new ShareAcknowledgeRequestData.AcknowledgePartition()
            .setPartitionIndex(0)
            .setAcknowledgementBatches(List(
              new ShareAcknowledgeRequestData.AcknowledgementBatch()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setAcknowledgeTypes(Collections.singletonList(1.toByte))
            ).asJava)
        ).asJava)
      ).asJava)

    val shareAcknowledgeRequest = new ShareAcknowledgeRequest.Builder(shareAcknowledgeRequestData).build(ApiKeys.SHARE_ACKNOWLEDGE.latestVersion)
    val request = buildRequest(shareAcknowledgeRequest)

    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        GroupCoordinatorConfig.NEW_GROUP_COORDINATOR_ENABLE_CONFIG -> "false",
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    kafkaApis.handleShareAcknowledgeRequest(request)

    val response = verifyNoThrottling[ShareAcknowledgeResponse](request)
    val responseData = response.data()

    assertEquals(Errors.UNSUPPORTED_VERSION.code, responseData.errorCode)
  }

  @Test
  def testHandleShareAcknowledgeShareGroupDisabled(): Unit = {
    val topicId = Uuid.randomUuid()
    val memberId: Uuid = Uuid.randomUuid()
    val groupId = "group"

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)

    val shareAcknowledgeRequestData = new ShareAcknowledgeRequestData()
      .setGroupId(groupId)
      .setMemberId(memberId.toString)
      .setShareSessionEpoch(1)
      .setTopics(List(new ShareAcknowledgeRequestData.AcknowledgeTopic()
        .setTopicId(topicId)
        .setPartitions(List(
          new ShareAcknowledgeRequestData.AcknowledgePartition()
            .setPartitionIndex(0)
            .setAcknowledgementBatches(List(
              new ShareAcknowledgeRequestData.AcknowledgementBatch()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setAcknowledgeTypes(Collections.singletonList(1.toByte))
            ).asJava)
        ).asJava)
      ).asJava)

    val shareAcknowledgeRequest = new ShareAcknowledgeRequest.Builder(shareAcknowledgeRequestData).build(ApiKeys.SHARE_ACKNOWLEDGE.latestVersion)
    val request = buildRequest(shareAcknowledgeRequest)

    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "false"),
      )
    kafkaApis.handleShareAcknowledgeRequest(request)

    val response = verifyNoThrottling[ShareAcknowledgeResponse](request)
    val responseData = response.data()

    assertEquals(Errors.UNSUPPORTED_VERSION.code, responseData.errorCode)
  }

  @Test
  def testHandleShareAcknowledgeRequestGroupAuthorizationError(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: Uuid = Uuid.ZERO_UUID

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    val shareAcknowledgeRequestData = new ShareAcknowledgeRequestData().
      setGroupId("group").
      setMemberId(memberId.toString).
      setShareSessionEpoch(1).
      setTopics(List(new ShareAcknowledgeRequestData.AcknowledgeTopic().
        setTopicId(topicId).
        setPartitions(List(
          new ShareAcknowledgeRequestData.AcknowledgePartition()
            .setPartitionIndex(partitionIndex)
            .setAcknowledgementBatches(List(
              new ShareAcknowledgeRequestData.AcknowledgementBatch()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setAcknowledgeTypes(Collections.singletonList(1.toByte))
            ).asJava)
        ).asJava)
      ).asJava)

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any(), any())).thenReturn(List[AuthorizationResult](
      AuthorizationResult.DENIED
    ).asJava)

    val shareAcknowledgeRequest = new ShareAcknowledgeRequest.Builder(shareAcknowledgeRequestData)
      .build(ApiKeys.SHARE_ACKNOWLEDGE.latestVersion)
    val request = buildRequest(shareAcknowledgeRequest)
    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      authorizer = Option(authorizer),
      )
    kafkaApis.handleShareAcknowledgeRequest(request)

    val response = verifyNoThrottling[ShareAcknowledgeResponse](request)
    val responseData = response.data()

    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code, responseData.errorCode)
  }

  @Test
  def testHandleShareAcknowledgeRequestInvalidRequestOnInitialEpoch(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: Uuid = Uuid.ZERO_UUID

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    when(sharePartitionManager.acknowledgeSessionUpdate(any(), any())).thenThrow(
      Errors.INVALID_SHARE_SESSION_EPOCH.exception
    )

    val shareAcknowledgeRequestData = new ShareAcknowledgeRequestData().
      setGroupId("group").
      setMemberId(memberId.toString).
      setShareSessionEpoch(0).
      setTopics(List(new ShareAcknowledgeRequestData.AcknowledgeTopic().
        setTopicId(topicId).
        setPartitions(List(
          new ShareAcknowledgeRequestData.AcknowledgePartition()
            .setPartitionIndex(partitionIndex)
            .setAcknowledgementBatches(List(
              new ShareAcknowledgeRequestData.AcknowledgementBatch()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setAcknowledgeTypes(Collections.singletonList(1.toByte))
            ).asJava)
        ).asJava)
      ).asJava)

    val shareAcknowledgeRequest = new ShareAcknowledgeRequest.Builder(shareAcknowledgeRequestData)
      .build(ApiKeys.SHARE_ACKNOWLEDGE.latestVersion)
    val request = buildRequest(shareAcknowledgeRequest)

    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    kafkaApis.handleShareAcknowledgeRequest(request)

    val response = verifyNoThrottling[ShareAcknowledgeResponse](request)
    val responseData = response.data()

    assertEquals(Errors.INVALID_SHARE_SESSION_EPOCH.code, responseData.errorCode)
  }

  @Test
  def testHandleShareAcknowledgeRequestSessionNotFound(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: Uuid = Uuid.ZERO_UUID

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    when(sharePartitionManager.acknowledgeSessionUpdate(any(), any())).thenThrow(
      Errors.SHARE_SESSION_NOT_FOUND.exception
    )

    val shareAcknowledgeRequestData = new ShareAcknowledgeRequestData().
      setGroupId("group").
      setMemberId(memberId.toString).
      setShareSessionEpoch(0).
      setTopics(List(new ShareAcknowledgeRequestData.AcknowledgeTopic().
        setTopicId(topicId).
        setPartitions(List(
          new ShareAcknowledgeRequestData.AcknowledgePartition()
            .setPartitionIndex(partitionIndex)
            .setAcknowledgementBatches(List(
              new ShareAcknowledgeRequestData.AcknowledgementBatch()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setAcknowledgeTypes(Collections.singletonList(1.toByte))
            ).asJava)
        ).asJava)
      ).asJava)

    val shareAcknowledgeRequest = new ShareAcknowledgeRequest.Builder(shareAcknowledgeRequestData)
      .build(ApiKeys.SHARE_ACKNOWLEDGE.latestVersion)
    val request = buildRequest(shareAcknowledgeRequest)

    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    kafkaApis.handleShareAcknowledgeRequest(request)

    val response = verifyNoThrottling[ShareAcknowledgeResponse](request)
    val responseData = response.data()

    assertEquals(Errors.SHARE_SESSION_NOT_FOUND.code, responseData.errorCode)
  }

  @Test
  def testHandleShareAcknowledgeRequestBatchValidationError(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val groupId: String = "group"
    val memberId: Uuid = Uuid.ZERO_UUID

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    doNothing().when(sharePartitionManager).acknowledgeSessionUpdate(any(), any())

    val shareAcknowledgeRequestData = new ShareAcknowledgeRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(1).
      setTopics(List(new ShareAcknowledgeRequestData.AcknowledgeTopic().
        setTopicId(topicId).
        setPartitions(List(
          new ShareAcknowledgeRequestData.AcknowledgePartition()
            .setPartitionIndex(partitionIndex)
            .setAcknowledgementBatches(List(
              new ShareAcknowledgeRequestData.AcknowledgementBatch()
                .setFirstOffset(10)
                .setLastOffset(4) // end offset is less than base offset
                .setAcknowledgeTypes(Collections.singletonList(1.toByte))
            ).asJava)
        ).asJava)
      ).asJava)

    val shareAcknowledgeRequest = new ShareAcknowledgeRequest.Builder(shareAcknowledgeRequestData)
      .build(ApiKeys.SHARE_ACKNOWLEDGE.latestVersion)
    val request = buildRequest(shareAcknowledgeRequest)
    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    kafkaApis.handleShareAcknowledgeRequest(request)

    val response = verifyNoThrottling[ShareAcknowledgeResponse](request)
    val responseData = response.data()
    val topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    assertEquals(topicId, topicResponses.get(0).topicId)
    assertEquals(1, topicResponses.get(0).partitions.size())
    assertEquals(partitionIndex, topicResponses.get(0).partitions.get(0).partitionIndex)
    assertEquals(Errors.INVALID_REQUEST.code, topicResponses.get(0).partitions.get(0).errorCode)
  }

  @Test
  def testHandleShareAcknowledgeResponseContainsNewLeaderOnNotLeaderOrFollower(): Unit = {
    val topicId = Uuid.randomUuid()
    val topicName = "foo"
    val partitionIndex = 0
    val topicIdPartition = new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex))
    val topicPartition = topicIdPartition.topicPartition
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicPartition.topic, numPartitions = 1, numBrokers = 3, topicId)
    val memberId: Uuid = Uuid.ZERO_UUID

    val partition = mock(classOf[Partition])
    val newLeaderId = 2
    val newLeaderEpoch = 5

    when(replicaManager.getPartitionOrError(topicPartition)).thenAnswer(_ => Right(partition))
    when(partition.leaderReplicaIdOpt).thenAnswer(_ => Some(newLeaderId))
    when(partition.getLeaderEpoch).thenAnswer(_ => newLeaderEpoch)

    doNothing().when(sharePartitionManager).acknowledgeSessionUpdate(any(), any())

    when(sharePartitionManager.acknowledge(
      any(),
      any(),
      any()
    )).thenReturn(CompletableFuture.completedFuture(Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData](
      new TopicIdPartition(topicId, new TopicPartition(topicName, partitionIndex)) ->
        new ShareAcknowledgeResponseData.PartitionData()
          .setPartitionIndex(partitionIndex)
          .setErrorCode(Errors.NOT_LEADER_OR_FOLLOWER.code())
    ).asJava))

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    val shareAcknowledgeRequestData = new ShareAcknowledgeRequestData().
      setGroupId("group").
      setMemberId(memberId.toString).
      setShareSessionEpoch(1).
      setTopics(List(new ShareAcknowledgeRequestData.AcknowledgeTopic().
        setTopicId(topicId).
        setPartitions(List(
          new ShareAcknowledgeRequestData.AcknowledgePartition()
            .setPartitionIndex(0)
            .setAcknowledgementBatches(List(
              new ShareAcknowledgeRequestData.AcknowledgementBatch()
                .setFirstOffset(10)
                .setLastOffset(20)
                .setAcknowledgeTypes(util.Arrays.asList(1.toByte,1.toByte,0.toByte,1.toByte,1.toByte,1.toByte,1.toByte,1.toByte,1.toByte,1.toByte,1.toByte))
            ).asJava)
        ).asJava)
      ).asJava)

    val shareAcknowledgeRequest = new ShareAcknowledgeRequest.Builder(shareAcknowledgeRequestData)
      .build(ApiKeys.SHARE_ACKNOWLEDGE.latestVersion)
    val request = buildRequest(shareAcknowledgeRequest)
    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    kafkaApis.handleShareAcknowledgeRequest(request)

    val response = verifyNoThrottling[ShareAcknowledgeResponse](request)
    val responseData = response.data()
    val topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    assertEquals(topicId, topicResponses.get(0).topicId)
    assertEquals(1, topicResponses.get(0).partitions.size())
    assertEquals(partitionIndex, topicResponses.get(0).partitions.get(0).partitionIndex)
    assertEquals(Errors.NOT_LEADER_OR_FOLLOWER.code, topicResponses.get(0).partitions.get(0).errorCode)
    assertEquals(newLeaderId, topicResponses.get(0).partitions.get(0).currentLeader.leaderId)
    assertEquals(newLeaderEpoch, topicResponses.get(0).partitions.get(0).currentLeader.leaderEpoch)
    assertEquals(2, responseData.nodeEndpoints.asScala.head.nodeId)
  }

  @Test
  def testHandleShareAcknowledgeRequestAcknowledgeThrowsError(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: Uuid = Uuid.randomUuid()

    val groupId = "group"

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    when(sharePartitionManager.acknowledge(any(), any(), any())).thenReturn(
      FutureUtils.failedFuture[util.Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]](Errors.UNKNOWN_SERVER_ERROR.exception())
    )

    doNothing().when(sharePartitionManager).acknowledgeSessionUpdate(any(), any())

    val shareAcknowledgeRequestData = new ShareAcknowledgeRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(1).
      setTopics(List(new ShareAcknowledgeRequestData.AcknowledgeTopic().
        setTopicId(topicId).
        setPartitions(List(
          new ShareAcknowledgeRequestData.AcknowledgePartition()
            .setPartitionIndex(partitionIndex)
            .setAcknowledgementBatches(List(
              new ShareAcknowledgeRequestData.AcknowledgementBatch()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setAcknowledgeTypes(Collections.singletonList(1.toByte))
            ).asJava)
        ).asJava)
      ).asJava)

    val shareAcknowledgeRequest = new ShareAcknowledgeRequest.Builder(shareAcknowledgeRequestData)
      .build(ApiKeys.SHARE_ACKNOWLEDGE.latestVersion)
    val request = buildRequest(shareAcknowledgeRequest)
    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    kafkaApis.handleShareAcknowledgeRequest(request)
    val response = verifyNoThrottling[ShareAcknowledgeResponse](request)
    val responseData = response.data()

    assertEquals(Errors.UNKNOWN_SERVER_ERROR.code, responseData.errorCode)
  }

  @Test
  def testHandleShareAcknowledgeRequestSuccessOnFinalEpoch(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: Uuid = Uuid.randomUuid()

    val groupId = "group"

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    when(sharePartitionManager.acknowledge(any(), any(), any())).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, 0)) ->
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code)
      ).asJava)
    )

    doNothing().when(sharePartitionManager).acknowledgeSessionUpdate(any(), any())

    when(sharePartitionManager.releaseSession(any(), any())).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, 0)) ->
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code)
      ).asJava)
    )

    val shareAcknowledgeRequestData = new ShareAcknowledgeRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(ShareRequestMetadata.FINAL_EPOCH).
      setTopics(List(new ShareAcknowledgeRequestData.AcknowledgeTopic().
        setTopicId(topicId).
        setPartitions(List(
          new ShareAcknowledgeRequestData.AcknowledgePartition()
            .setPartitionIndex(partitionIndex)
            .setAcknowledgementBatches(List(
              new ShareAcknowledgeRequestData.AcknowledgementBatch()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setAcknowledgeTypes(Collections.singletonList(1.toByte))
            ).asJava)
        ).asJava)
      ).asJava)

    val shareAcknowledgeRequest = new ShareAcknowledgeRequest.Builder(shareAcknowledgeRequestData)
      .build(ApiKeys.SHARE_ACKNOWLEDGE.latestVersion)
    val request = buildRequest(shareAcknowledgeRequest)
    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    kafkaApis.handleShareAcknowledgeRequest(request)
    val response = verifyNoThrottling[ShareAcknowledgeResponse](request)
    val responseData = response.data()
    val topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    assertEquals(topicId, topicResponses.get(0).topicId)
    assertEquals(1, topicResponses.get(0).partitions.size())
    assertEquals(partitionIndex, topicResponses.get(0).partitions.get(0).partitionIndex)
    assertEquals(Errors.NONE.code, topicResponses.get(0).partitions.get(0).errorCode)
  }

  @Test
  def testHandleShareAcknowledgeRequestReleaseAcquiredRecordsThrowError(): Unit = {
    val topicName = "foo"
    val topicId = Uuid.randomUuid()
    val partitionIndex = 0
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicName, 1, topicId = topicId)
    val memberId: Uuid = Uuid.randomUuid()

    val groupId = "group"

    when(clientQuotaManager.maybeRecordAndGetThrottleTimeMs(
      any[RequestChannel.Request](), anyDouble, anyLong)).thenReturn(0)

    when(sharePartitionManager.acknowledge(any(), any(), any())).thenReturn(
      CompletableFuture.completedFuture(Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData](
        new TopicIdPartition(topicId, new TopicPartition(topicName, 0)) ->
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code)
      ).asJava)
    )

    doNothing().when(sharePartitionManager).acknowledgeSessionUpdate(any(), any())

    when(sharePartitionManager.releaseSession(any(), any())).thenReturn(
      FutureUtils.failedFuture[util.Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]](Errors.UNKNOWN_SERVER_ERROR.exception())
    )

    val shareAcknowledgeRequestData = new ShareAcknowledgeRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(ShareRequestMetadata.FINAL_EPOCH).
      setTopics(List(new ShareAcknowledgeRequestData.AcknowledgeTopic().
        setTopicId(topicId).
        setPartitions(List(
          new ShareAcknowledgeRequestData.AcknowledgePartition()
            .setPartitionIndex(partitionIndex)
            .setAcknowledgementBatches(List(
              new ShareAcknowledgeRequestData.AcknowledgementBatch()
                .setFirstOffset(0)
                .setLastOffset(9)
                .setAcknowledgeTypes(Collections.singletonList(1.toByte))
            ).asJava)
        ).asJava)
      ).asJava)

    val shareAcknowledgeRequest = new ShareAcknowledgeRequest.Builder(shareAcknowledgeRequestData)
      .build(ApiKeys.SHARE_ACKNOWLEDGE.latestVersion)
    val request = buildRequest(shareAcknowledgeRequest)
    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    kafkaApis.handleShareAcknowledgeRequest(request)
    val response = verifyNoThrottling[ShareAcknowledgeResponse](request)
    val responseData = response.data()
    val topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(1, topicResponses.size())
    assertEquals(topicId, topicResponses.get(0).topicId)
    assertEquals(1, topicResponses.get(0).partitions.size())
    assertEquals(partitionIndex, topicResponses.get(0).partitions.get(0).partitionIndex)
    assertEquals(Errors.NONE.code, topicResponses.get(0).partitions.get(0).errorCode)
  }

  private def expectedAcquiredRecords(firstOffset: Long, lastOffset: Long, deliveryCount: Int): util.List[AcquiredRecords] = {
    val acquiredRecordsList: util.List[AcquiredRecords] = new util.ArrayList()
    acquiredRecordsList.add(new AcquiredRecords()
      .setFirstOffset(firstOffset)
      .setLastOffset(lastOffset)
      .setDeliveryCount(deliveryCount.toShort))
    acquiredRecordsList
  }

  @Test
  def testGetAcknowledgeBatchesFromShareFetchRequest(): Unit = {
    val topicId1 = Uuid.randomUuid()
    val topicId2 = Uuid.randomUuid()
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    val shareFetchRequestData = new ShareFetchRequestData().
      setGroupId("group").
      setMemberId(Uuid.randomUuid().toString).
      setShareSessionEpoch(0).
      setTopics(util.List.of(
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId1).
          setPartitions(util.List.of(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0)
              .setAcknowledgementBatches(util.List.of(
                new ShareFetchRequestData.AcknowledgementBatch()
                  .setFirstOffset(0)
                  .setLastOffset(9)
                  .setAcknowledgeTypes(util.List.of(1.toByte)),
                new ShareFetchRequestData.AcknowledgementBatch()
                  .setFirstOffset(10)
                  .setLastOffset(17)
                  .setAcknowledgeTypes(util.List.of(1.toByte))
              )),
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(1)
              .setAcknowledgementBatches(util.List.of(
                new ShareFetchRequestData.AcknowledgementBatch()
                  .setFirstOffset(0)
                  .setLastOffset(9)
                  .setAcknowledgeTypes(util.List.of(2.toByte))
              ))
          )),
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId2).
          setPartitions(util.List.of(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0)
              .setAcknowledgementBatches(util.List.of(
                new ShareFetchRequestData.AcknowledgementBatch()
                  .setFirstOffset(24)
                  .setLastOffset(65)
                  .setAcknowledgeTypes(util.List.of(3.toByte))
              )),
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(1)
          ))
      ))
    val shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    val topicNames = new util.HashMap[Uuid, String]
    topicNames.put(topicId1, "foo1")
    topicNames.put(topicId2, "foo2")
    val erroneous = mutable.Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]()

    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    val acknowledgeBatches = kafkaApis.getAcknowledgeBatchesFromShareFetchRequest(shareFetchRequest, topicNames, erroneous)

    assertEquals(4, acknowledgeBatches.size)
    assertTrue(acknowledgeBatches.contains(new TopicIdPartition(topicId1, new TopicPartition("foo1", 0))))
    assertTrue(acknowledgeBatches.contains(new TopicIdPartition(topicId1, new TopicPartition("foo1", 1))))
    assertTrue(acknowledgeBatches.contains(new TopicIdPartition(topicId2, new TopicPartition("foo2", 0))))

    assertTrue(compareAcknowledgementBatches(0, 9, 1, acknowledgeBatches.getOrElse(new TopicIdPartition(topicId1, new TopicPartition("foo1", 0)), null).get(0)))
    assertTrue(compareAcknowledgementBatches(10, 17, 1, acknowledgeBatches.getOrElse(new TopicIdPartition(topicId1, new TopicPartition("foo1", 0)), null).get(1)))
    assertTrue(compareAcknowledgementBatches(0, 9, 2, acknowledgeBatches.getOrElse(new TopicIdPartition(topicId1, new TopicPartition("foo1", 1)), null).get(0)))
    assertTrue(compareAcknowledgementBatches(24, 65, 3, acknowledgeBatches.getOrElse(new TopicIdPartition(topicId2, new TopicPartition("foo2", 0)), null).get(0)))
  }

  @Test
  def testGetAcknowledgeBatchesFromShareFetchRequestError(): Unit = {
    val topicId1 = Uuid.randomUuid()
    val topicId2 = Uuid.randomUuid()
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    val shareFetchRequestData = new ShareFetchRequestData().
      setGroupId("group").
      setMemberId(Uuid.randomUuid().toString).
      setShareSessionEpoch(0).
      setTopics(util.List.of(
        new ShareFetchRequestData.FetchTopic().
          setTopicId(topicId1).
          setPartitions(util.List.of(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0)
              .setAcknowledgementBatches(util.List.of(
                new ShareFetchRequestData.AcknowledgementBatch()
                  .setFirstOffset(0)
                  .setLastOffset(9)
                  .setAcknowledgeTypes(util.List.of(7.toByte)) // wrong acknowledgement type here (can only be 0, 1, 2 or 3)
              )),
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(1)
              .setAcknowledgementBatches(util.List.of(
                new ShareFetchRequestData.AcknowledgementBatch()
                  .setFirstOffset(0)
                  .setLastOffset(9)
                  .setAcknowledgeTypes(util.List.of()) // wrong acknowledgement type here (can only be 0, 1, 2 or 3)
              ))
          )),
        new ShareFetchRequestData.FetchTopic()
          .setTopicId(topicId2)
          .setPartitions(util.List.of(
            new ShareFetchRequestData.FetchPartition()
              .setPartitionIndex(0)
              .setAcknowledgementBatches(util.List.of(
                new ShareFetchRequestData.AcknowledgementBatch()
                  .setFirstOffset(24)
                  .setLastOffset(65)
                  .setAcknowledgeTypes(util.List.of(3.toByte))
              ))
          ))
      ))
    val shareFetchRequest = new ShareFetchRequest.Builder(shareFetchRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    val topicIdNames = new util.HashMap[Uuid, String]
    topicIdNames.put(topicId1, "foo1") // topicId2 is not present in topicIdNames
    val erroneous = mutable.Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]()

    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    val acknowledgeBatches = kafkaApis.getAcknowledgeBatchesFromShareFetchRequest(shareFetchRequest, topicIdNames, erroneous)
    val erroneousTopicIdPartitions = kafkaApis.validateAcknowledgementBatches(acknowledgeBatches, erroneous)

    assertEquals(3, erroneous.size)
    assertEquals(2, erroneousTopicIdPartitions.size)
    assertTrue(erroneous.contains(new TopicIdPartition(topicId1, new TopicPartition("foo1", 0))))
    assertTrue(erroneous.contains(new TopicIdPartition(topicId1, new TopicPartition("foo1", 1))))
    assertTrue(erroneous.contains(new TopicIdPartition(topicId2, new TopicPartition(null, 0))))
    assertEquals(Errors.INVALID_REQUEST.code, erroneous(new TopicIdPartition(topicId1, new TopicPartition("foo1", 0))).errorCode)
    assertEquals(Errors.INVALID_REQUEST.code, erroneous(new TopicIdPartition(topicId1, new TopicPartition("foo1", 1))).errorCode)
    assertEquals(Errors.UNKNOWN_TOPIC_ID.code, erroneous(new TopicIdPartition(topicId2, new TopicPartition(null, 0))).errorCode)
  }

  @Test
  def testGetAcknowledgeBatchesFromShareAcknowledgeRequest(): Unit = {
    val topicId1 = Uuid.randomUuid()
    val topicId2 = Uuid.randomUuid()

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    val shareAcknowledgeRequestData = new ShareAcknowledgeRequestData().
      setGroupId("group").
      setMemberId(Uuid.randomUuid().toString).
      setShareSessionEpoch(0).
      setTopics(List(
        new ShareAcknowledgeRequestData.AcknowledgeTopic().
          setTopicId(topicId1).
          setPartitions(List(
            new ShareAcknowledgeRequestData.AcknowledgePartition()
              .setPartitionIndex(0)
              .setAcknowledgementBatches(List(
                new ShareAcknowledgeRequestData.AcknowledgementBatch()
                  .setFirstOffset(0)
                  .setLastOffset(9)
                  .setAcknowledgeTypes(Collections.singletonList(1.toByte)),
                new ShareAcknowledgeRequestData.AcknowledgementBatch()
                  .setFirstOffset(10)
                  .setLastOffset(17)
                  .setAcknowledgeTypes(Collections.singletonList(1.toByte))
              ).asJava),
            new ShareAcknowledgeRequestData.AcknowledgePartition()
              .setPartitionIndex(1)
              .setAcknowledgementBatches(List(
                new ShareAcknowledgeRequestData.AcknowledgementBatch()
                  .setFirstOffset(0)
                  .setLastOffset(9)
                  .setAcknowledgeTypes(Collections.singletonList(2.toByte))
              ).asJava)
          ).asJava),
        new ShareAcknowledgeRequestData.AcknowledgeTopic().
          setTopicId(topicId2).
          setPartitions(List(
            new ShareAcknowledgeRequestData.AcknowledgePartition()
              .setPartitionIndex(0)
              .setAcknowledgementBatches(List(
                new ShareAcknowledgeRequestData.AcknowledgementBatch()
                  .setFirstOffset(24)
                  .setLastOffset(65)
                  .setAcknowledgeTypes(Collections.singletonList(3.toByte))
              ).asJava)
          ).asJava)
      ).asJava)

    val shareAcknowledgeRequest = new ShareAcknowledgeRequest.Builder(shareAcknowledgeRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    val topicNames = new util.HashMap[Uuid, String]
    topicNames.put(topicId1, "foo1")
    topicNames.put(topicId2, "foo2")
    val erroneous = mutable.Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]()

    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    val acknowledgeBatches = kafkaApis.getAcknowledgeBatchesFromShareAcknowledgeRequest(shareAcknowledgeRequest, topicNames, erroneous)

    assertEquals(3, acknowledgeBatches.size)
    assertTrue(acknowledgeBatches.contains(new TopicIdPartition(topicId1, new TopicPartition("foo1", 0))))
    assertTrue(acknowledgeBatches.contains(new TopicIdPartition(topicId1, new TopicPartition("foo1", 1))))
    assertTrue(acknowledgeBatches.contains(new TopicIdPartition(topicId2, new TopicPartition("foo2", 0))))

    assertTrue(compareAcknowledgementBatches(0, 9, 1, acknowledgeBatches.getOrElse(new TopicIdPartition(topicId1, new TopicPartition("foo1", 0)), null).get(0)))
    assertTrue(compareAcknowledgementBatches(10, 17, 1, acknowledgeBatches.getOrElse(new TopicIdPartition(topicId1, new TopicPartition("foo1", 0)), null).get(1)))
    assertTrue(compareAcknowledgementBatches(0, 9, 2, acknowledgeBatches.getOrElse(new TopicIdPartition(topicId1, new TopicPartition("foo1", 1)), null).get(0)))
    assertTrue(compareAcknowledgementBatches(24, 65, 3, acknowledgeBatches.getOrElse(new TopicIdPartition(topicId2, new TopicPartition("foo2", 0)), null).get(0)))
  }

  @Test
  def testGetAcknowledgeBatchesFromShareAcknowledgeRequestError(): Unit = {
    val topicId1 = Uuid.randomUuid()
    val topicId2 = Uuid.randomUuid()
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    val shareAcknowledgeRequestData = new ShareAcknowledgeRequestData().
      setGroupId("group").
      setMemberId(Uuid.randomUuid().toString).
      setShareSessionEpoch(0).
      setTopics(List(
        new ShareAcknowledgeRequestData.AcknowledgeTopic().
          setTopicId(topicId1).
          setPartitions(List(
            new ShareAcknowledgeRequestData.AcknowledgePartition()
              .setPartitionIndex(0)
              .setAcknowledgementBatches(List(
                new ShareAcknowledgeRequestData.AcknowledgementBatch()
                  .setFirstOffset(0)
                  .setLastOffset(9)
                  .setAcknowledgeTypes(Collections.singletonList(7.toByte)) // wrong acknowledgement type here (can only be 0, 1, 2 or 3)
              ).asJava),
            new ShareAcknowledgeRequestData.AcknowledgePartition()
              .setPartitionIndex(1)
              .setAcknowledgementBatches(List(
                new ShareAcknowledgeRequestData.AcknowledgementBatch()
                  .setFirstOffset(0)
                  .setLastOffset(9)
                  .setAcknowledgeTypes(Collections.emptyList()) // wrong acknowledgement type here (can only be 0, 1, 2 or 3)
              ).asJava)
          ).asJava),
        new ShareAcknowledgeRequestData.AcknowledgeTopic().
          setTopicId(topicId2).
          setPartitions(List(
            new ShareAcknowledgeRequestData.AcknowledgePartition()
              .setPartitionIndex(0)
              .setAcknowledgementBatches(List(
                new ShareAcknowledgeRequestData.AcknowledgementBatch()
                  .setFirstOffset(24)
                  .setLastOffset(65)
                  .setAcknowledgeTypes(Collections.singletonList(3.toByte))
              ).asJava)
          ).asJava)
      ).asJava)

    val shareAcknowledgeRequest = new ShareAcknowledgeRequest.Builder(shareAcknowledgeRequestData).build(ApiKeys.SHARE_FETCH.latestVersion)
    val topicIdNames = new util.HashMap[Uuid, String]
    topicIdNames.put(topicId1, "foo1") // topicId2 not present in topicIdNames
    val erroneous = mutable.Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]()

    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    val acknowledgeBatches = kafkaApis.getAcknowledgeBatchesFromShareAcknowledgeRequest(shareAcknowledgeRequest, topicIdNames, erroneous)
    val erroneousTopicIdPartitions = kafkaApis.validateAcknowledgementBatches(acknowledgeBatches, erroneous)

    assertEquals(3, erroneous.size)
    assertEquals(2, erroneousTopicIdPartitions.size)
    assertTrue(erroneous.contains(new TopicIdPartition(topicId1, new TopicPartition("foo1", 0))))
    assertTrue(erroneous.contains(new TopicIdPartition(topicId1, new TopicPartition("foo1", 1))))
    assertTrue(erroneous.contains(new TopicIdPartition(topicId2, new TopicPartition(null, 0))))

    assertTrue(erroneous.contains(new TopicIdPartition(topicId2, new TopicPartition(null, 0))))
    assertEquals(Errors.INVALID_REQUEST.code, erroneous(new TopicIdPartition(topicId1, new TopicPartition("foo1", 0))).errorCode)
    assertEquals(Errors.INVALID_REQUEST.code, erroneous(new TopicIdPartition(topicId1, new TopicPartition("foo1", 1))).errorCode)
    assertEquals(Errors.UNKNOWN_TOPIC_ID.code, erroneous(new TopicIdPartition(topicId2, new TopicPartition(null, 0))).errorCode)
  }

  @Test
  def testHandleAcknowledgementsSuccess(): Unit = {
    val groupId = "group"

    val topicName1 = "foo1"
    val topicName2 = "foo2"

    val topicId1 = Uuid.randomUuid()
    val topicId2 = Uuid.randomUuid()
    val memberId = Uuid.randomUuid()

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicName1, 1, topicId = topicId1)
    addTopicToMetadataCache(topicName2, 2, topicId = topicId2)

    val tp1 = new TopicIdPartition(topicId1, new TopicPartition(topicName1, 0))
    val tp2 = new TopicIdPartition(topicId2, new TopicPartition(topicName2, 0))
    val tp3 = new TopicIdPartition(topicId2, new TopicPartition(topicName2, 1))

    when(sharePartitionManager.acknowledge(any(), any(), any()))
      .thenReturn(CompletableFuture.completedFuture(Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData](
        tp1 ->
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code),
        tp2 ->
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code),
        tp3 ->
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(1)
            .setErrorCode(Errors.NONE.code)
      ).asJava))

    val acknowledgementData = mutable.Map[TopicIdPartition, util.List[ShareAcknowledgementBatch]]()

    acknowledgementData += (tp1 -> util.Arrays.asList(
      new ShareAcknowledgementBatch(0, 9, Collections.singletonList(1.toByte)),
      new ShareAcknowledgementBatch(10, 19, Collections.singletonList(2.toByte))
    ))
    acknowledgementData += (tp2 -> util.Arrays.asList(
      new ShareAcknowledgementBatch(5, 19, Collections.singletonList(2.toByte))
    ))
    acknowledgementData += (tp3 -> util.Arrays.asList(
      new ShareAcknowledgementBatch(34, 56, Collections.singletonList(1.toByte))
    ))

    val authorizedTopics: Set[String] = Set(topicName1, topicName2)

    val erroneous = mutable.Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]()

    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    val ackResult = kafkaApis.handleAcknowledgements(
      acknowledgementData,
      erroneous,
      sharePartitionManager,
      authorizedTopics,
      groupId,
      memberId.toString
    ).get()

    assertEquals(3, ackResult.size)
    assertTrue(ackResult.contains(new TopicIdPartition(topicId1, new TopicPartition("foo1", 0))))
    assertTrue(ackResult.contains(new TopicIdPartition(topicId2, new TopicPartition("foo2", 0))))
    assertTrue(ackResult.contains(new TopicIdPartition(topicId2, new TopicPartition("foo2", 1))))

    assertTrue(compareAcknowledgeResponsePartitionData(0, Errors.NONE.code, ackResult.getOrElse(
      new TopicIdPartition(topicId1, new TopicPartition("foo1", 0)), null)))
    assertTrue(compareAcknowledgeResponsePartitionData(0, Errors.NONE.code, ackResult.getOrElse(
      new TopicIdPartition(topicId2, new TopicPartition("foo2", 0)), null)))
    assertTrue(compareAcknowledgeResponsePartitionData(1, Errors.NONE.code, ackResult.getOrElse(
      new TopicIdPartition(topicId2, new TopicPartition("foo2", 1)), null)))
  }

  @Test
  def testHandleAcknowledgementsInvalidAcknowledgementBatches(): Unit = {
    val groupId = "group"

    val topicName1 = "foo1"
    val topicName2 = "foo2"

    val topicId1 = Uuid.randomUuid()
    val topicId2 = Uuid.randomUuid()
    val memberId = Uuid.randomUuid()

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicName1, 1, topicId = topicId1)
    addTopicToMetadataCache(topicName2, 2, topicId = topicId2)

    val tp1 = new TopicIdPartition(topicId1, new TopicPartition(topicName1, 0))
    val tp2 = new TopicIdPartition(topicId2, new TopicPartition(topicName2, 0))
    val tp3 = new TopicIdPartition(topicId2, new TopicPartition(topicName2, 1))

    when(sharePartitionManager.acknowledge(any(), any(), any()))
      .thenReturn(CompletableFuture.completedFuture(Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData](
        new TopicIdPartition(topicId1, new TopicPartition("foo1", 0)) ->
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code),
        new TopicIdPartition(topicId2, new TopicPartition("foo2", 0)) ->
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code),
        new TopicIdPartition(topicId2, new TopicPartition("foo2", 1)) ->
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(1)
            .setErrorCode(Errors.NONE.code)
      ).asJava))

    val acknowledgementData = mutable.Map[TopicIdPartition, util.List[ShareAcknowledgementBatch]]()
    acknowledgementData += (tp1 -> util.Arrays.asList(
      new ShareAcknowledgementBatch(39, 24, Collections.singletonList(1.toByte)), // this is an invalid batch because last offset is less than base offset
      new ShareAcknowledgementBatch(43, 56, Collections.singletonList(2.toByte))
    ))
    acknowledgementData += (tp2 -> util.Arrays.asList(
      new ShareAcknowledgementBatch(5, 19, util.Arrays.asList(0.toByte, 2.toByte))
    ))
    acknowledgementData += (tp3 -> util.Arrays.asList(
      new ShareAcknowledgementBatch(34, 56, Collections.singletonList(1.toByte)),
      new ShareAcknowledgementBatch(10, 19, Collections.singletonList(1.toByte)) // this is an invalid batch because start is offset is less than previous end offset
    ))

    val authorizedTopics: Set[String] = Set(topicName1, topicName2)

    val erroneous = mutable.Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]()

    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    val ackResult = kafkaApis.handleAcknowledgements(
      acknowledgementData,
      erroneous,
      sharePartitionManager,
      authorizedTopics,
      groupId,
      memberId.toString
    ).get()

    assertEquals(3, ackResult.size)
    assertTrue(ackResult.contains(new TopicIdPartition(topicId1, new TopicPartition("foo1", 0))))
    assertTrue(ackResult.contains(new TopicIdPartition(topicId2, new TopicPartition("foo2", 0))))
    assertTrue(ackResult.contains(new TopicIdPartition(topicId2, new TopicPartition("foo2", 1))))

    assertTrue(compareAcknowledgeResponsePartitionData(0, Errors.INVALID_REQUEST.code, ackResult.getOrElse(
      new TopicIdPartition(topicId1, new TopicPartition("foo1", 0)), null)))
    assertTrue(compareAcknowledgeResponsePartitionData(0, Errors.INVALID_REQUEST.code, ackResult.getOrElse(
      new TopicIdPartition(topicId2, new TopicPartition("foo2", 0)), null)))
    assertTrue(compareAcknowledgeResponsePartitionData(1, Errors.INVALID_REQUEST.code, ackResult.getOrElse(
      new TopicIdPartition(topicId2, new TopicPartition("foo2", 1)), null)))
  }

  @Test
  def testHandleAcknowledgementsUnauthorizedTopics(): Unit = {
    val groupId = "group"

    val topicName1 = "foo1"
    val topicName2 = "foo2"

    val topicId1 = Uuid.randomUuid()
    val topicId2 = Uuid.randomUuid()
    val memberId = Uuid.randomUuid()

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    // Topic with id topicId1 is not present in Metadata Cache
    addTopicToMetadataCache(topicName2, 2, topicId = topicId2)

    val tp1 = new TopicIdPartition(topicId1, new TopicPartition(topicName1, 0))
    val tp2 = new TopicIdPartition(topicId2, new TopicPartition(topicName2, 0))
    val tp3 = new TopicIdPartition(topicId2, new TopicPartition(topicName2, 1))

    when(sharePartitionManager.acknowledge(any(), any(), any()))
      .thenReturn(CompletableFuture.completedFuture(Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData](
        new TopicIdPartition(topicId1, new TopicPartition("foo1", 0)) ->
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code),
        new TopicIdPartition(topicId2, new TopicPartition("foo2", 0)) ->
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code),
        new TopicIdPartition(topicId2, new TopicPartition("foo2", 1)) ->
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(1)
            .setErrorCode(Errors.NONE.code)
      ).asJava))

    val acknowledgementData = mutable.Map[TopicIdPartition, util.List[ShareAcknowledgementBatch]]()

    acknowledgementData += (tp1 -> util.Arrays.asList(
      new ShareAcknowledgementBatch(24, 39, Collections.singletonList(1.toByte)),
      new ShareAcknowledgementBatch(43, 56, Collections.singletonList(2.toByte))
    ))
    acknowledgementData += (tp2 -> util.Arrays.asList(
      new ShareAcknowledgementBatch(5, 19, Collections.singletonList(2.toByte))
    ))
    acknowledgementData += (tp3 -> util.Arrays.asList(
      new ShareAcknowledgementBatch(34, 56, Collections.singletonList(1.toByte)),
      new ShareAcknowledgementBatch(67, 87, Collections.singletonList(1.toByte))
    ))

    val authorizedTopics: Set[String] = Set(topicName1) // Topic with topicId2 is not authorized

    val erroneous = mutable.Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]()

    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    val ackResult = kafkaApis.handleAcknowledgements(
      acknowledgementData,
      erroneous,
      sharePartitionManager,
      authorizedTopics,
      groupId,
      memberId.toString
    ).get()

    assertEquals(3, ackResult.size)
    assertTrue(ackResult.contains(new TopicIdPartition(topicId1, new TopicPartition("foo1", 0))))
    assertTrue(ackResult.contains(new TopicIdPartition(topicId2, new TopicPartition("foo2", 0))))
    assertTrue(ackResult.contains(new TopicIdPartition(topicId2, new TopicPartition("foo2", 1))))

    assertTrue(compareAcknowledgeResponsePartitionData(0, Errors.UNKNOWN_TOPIC_OR_PARTITION.code, ackResult.getOrElse(
      new TopicIdPartition(topicId1, new TopicPartition("foo1", 0)), null)))
    assertTrue(compareAcknowledgeResponsePartitionData(0, Errors.TOPIC_AUTHORIZATION_FAILED.code, ackResult.getOrElse(
      new TopicIdPartition(topicId2, new TopicPartition("foo2", 0)), null)))
    assertTrue(compareAcknowledgeResponsePartitionData(1, Errors.TOPIC_AUTHORIZATION_FAILED.code, ackResult.getOrElse(
      new TopicIdPartition(topicId2, new TopicPartition("foo2", 1)), null)))
  }

  @Test
  def testHandleAcknowledgementsWithErroneous(): Unit = {
    val groupId = "group"

    val topicName1 = "foo1"
    val topicName2 = "foo2"

    val topicId1 = Uuid.randomUuid()
    val topicId2 = Uuid.randomUuid()
    val memberId = Uuid.randomUuid()

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicName1, 1, topicId = topicId1)
    addTopicToMetadataCache(topicName2, 2, topicId = topicId2)

    val tp1 = new TopicIdPartition(topicId1, new TopicPartition(topicName1, 0))
    val tp2 = new TopicIdPartition(topicId2, new TopicPartition(topicName2, 0))
    val tp3 = new TopicIdPartition(topicId2, new TopicPartition(topicName2, 1))

    when(sharePartitionManager.acknowledge(any(), any(), any()))
      .thenReturn(CompletableFuture.completedFuture(Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData](
        tp1 ->
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code),
        tp2 ->
          new ShareAcknowledgeResponseData.PartitionData()
            .setPartitionIndex(0)
            .setErrorCode(Errors.NONE.code)
      ).asJava))

    val acknowledgementData = mutable.Map[TopicIdPartition, util.List[ShareAcknowledgementBatch]]()

    acknowledgementData += (tp1 -> util.Arrays.asList(
      new ShareAcknowledgementBatch(0, 9, Collections.singletonList(1.toByte)),
      new ShareAcknowledgementBatch(10, 19, Collections.singletonList(2.toByte))
    ))
    acknowledgementData += (tp2 -> util.Arrays.asList(
      new ShareAcknowledgementBatch(5, 19, Collections.singletonList(2.toByte))
    ))

    val authorizedTopics: Set[String] = Set(topicName1, topicName2)

    val erroneous = mutable.Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData]()

    erroneous += (tp3 -> ShareAcknowledgeResponse.partitionResponse(tp3, Errors.UNKNOWN_TOPIC_ID))

    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    val ackResult = kafkaApis.handleAcknowledgements(
      acknowledgementData,
      erroneous,
      sharePartitionManager,
      authorizedTopics,
      groupId,
      memberId.toString
    ).get()

    assertEquals(3, ackResult.size)
    assertTrue(ackResult.contains(new TopicIdPartition(topicId1, new TopicPartition("foo1", 0))))
    assertTrue(ackResult.contains(new TopicIdPartition(topicId2, new TopicPartition("foo2", 0))))
    assertTrue(ackResult.contains(new TopicIdPartition(topicId2, new TopicPartition("foo2", 1))))

    assertTrue(compareAcknowledgeResponsePartitionData(0, Errors.NONE.code, ackResult.getOrElse(
      new TopicIdPartition(topicId1, new TopicPartition("foo1", 0)), null)))
    assertTrue(compareAcknowledgeResponsePartitionData(0, Errors.NONE.code, ackResult.getOrElse(
      new TopicIdPartition(topicId2, new TopicPartition("foo2", 0)), null)))
    assertTrue(compareAcknowledgeResponsePartitionData(1, Errors.UNKNOWN_TOPIC_ID.code, ackResult.getOrElse(
      new TopicIdPartition(topicId2, new TopicPartition("foo2", 1)), null)))
  }

  @Test
  def testProcessShareAcknowledgeResponse(): Unit = {
    val groupId = "group"

    val memberId = Uuid.randomUuid()

    val topicId1 = Uuid.randomUuid()
    val topicId2 = Uuid.randomUuid()

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    val responseAcknowledgeData: mutable.Map[TopicIdPartition, ShareAcknowledgeResponseData.PartitionData] = mutable.Map()
    responseAcknowledgeData += (new TopicIdPartition(topicId1, new TopicPartition("foo", 0)) ->
      new ShareAcknowledgeResponseData.PartitionData().setPartitionIndex(0).setErrorCode(Errors.NONE.code))
    responseAcknowledgeData += (new TopicIdPartition(topicId1, new TopicPartition("foo", 1)) ->
      new ShareAcknowledgeResponseData.PartitionData().setPartitionIndex(0).setErrorCode(Errors.INVALID_REQUEST.code))
    responseAcknowledgeData += (new TopicIdPartition(topicId2, new TopicPartition("bar", 0)) ->
      new ShareAcknowledgeResponseData.PartitionData().setPartitionIndex(0).setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code))
    responseAcknowledgeData += (new TopicIdPartition(topicId2, new TopicPartition("bar", 1)) ->
      new ShareAcknowledgeResponseData.PartitionData().setPartitionIndex(0).setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code))

    val shareAcknowledgeRequestData = new ShareAcknowledgeRequestData().
      setGroupId(groupId).
      setMemberId(memberId.toString).
      setShareSessionEpoch(1).
      setTopics(List(
        new ShareAcknowledgeRequestData.AcknowledgeTopic().
          setTopicId(topicId1).
          setPartitions(List(
            new ShareAcknowledgeRequestData.AcknowledgePartition()
              .setPartitionIndex(0)
              .setAcknowledgementBatches(List(
                new ShareAcknowledgeRequestData.AcknowledgementBatch()
                  .setFirstOffset(0)
                  .setLastOffset(9)
                  .setAcknowledgeTypes(Collections.singletonList(1.toByte))
              ).asJava),
            new ShareAcknowledgeRequestData.AcknowledgePartition()
              .setPartitionIndex(1)
              .setAcknowledgementBatches(List(
                new ShareAcknowledgeRequestData.AcknowledgementBatch()
                  .setFirstOffset(0)
                  .setLastOffset(9)
                  .setAcknowledgeTypes(Collections.singletonList(1.toByte))
              ).asJava)
          ).asJava),
        new ShareAcknowledgeRequestData.AcknowledgeTopic().
          setTopicId(topicId2).
          setPartitions(List(
            new ShareAcknowledgeRequestData.AcknowledgePartition()
              .setPartitionIndex(0)
              .setAcknowledgementBatches(List(
                new ShareAcknowledgeRequestData.AcknowledgementBatch()
                  .setFirstOffset(0)
                  .setLastOffset(9)
                  .setAcknowledgeTypes(Collections.singletonList(1.toByte))
              ).asJava),
            new ShareAcknowledgeRequestData.AcknowledgePartition()
              .setPartitionIndex(1)
              .setAcknowledgementBatches(List(
                new ShareAcknowledgeRequestData.AcknowledgementBatch()
                  .setFirstOffset(0)
                  .setLastOffset(9)
                  .setAcknowledgeTypes(Collections.singletonList(1.toByte))
              ).asJava)
          ).asJava)
      ).asJava)

    val shareAcknowledgeRequest = new ShareAcknowledgeRequest.Builder(shareAcknowledgeRequestData)
      .build(ApiKeys.SHARE_ACKNOWLEDGE.latestVersion)
    val request = buildRequest(shareAcknowledgeRequest)
    kafkaApis = createKafkaApis(
      overrideProperties = Map(
        ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG -> "true",
        ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      )
    val response = kafkaApis.processShareAcknowledgeResponse(responseAcknowledgeData, request)
    val responseData = response.data()
    val topicResponses = responseData.responses()

    assertEquals(Errors.NONE.code, responseData.errorCode)
    assertEquals(2, topicResponses.size())

    val topicResponsesScala = topicResponses.asScala.toList
    val topicResponsesMap: Map[Uuid, ShareAcknowledgeResponseData.ShareAcknowledgeTopicResponse] = topicResponsesScala.map(topic => topic.topicId -> topic).toMap
    assertTrue(topicResponsesMap.contains(topicId1))

    val topicIdResponse1: ShareAcknowledgeResponseData.ShareAcknowledgeTopicResponse = topicResponsesMap.getOrElse(topicId1, null)
    assertEquals(2, topicIdResponse1.partitions().size())

    val partitionResponses1 = topicIdResponse1.partitions().asScala.toList
    val partitionResponsesMap1: Map[Int, ShareAcknowledgeResponseData.PartitionData] = partitionResponses1.map(partition => partition.partitionIndex -> partition).toMap
    assertTrue(partitionResponsesMap1.contains(0))
    assertTrue(partitionResponsesMap1.contains(1))
    assertTrue(partitionResponsesMap1.getOrElse(0, null).errorCode == Errors.NONE.code)
    assertTrue(partitionResponsesMap1.getOrElse(1, null).errorCode == Errors.INVALID_REQUEST.code)

    assertTrue(topicResponsesMap.contains(topicId2))

    val topicIdResponse2: ShareAcknowledgeResponseData.ShareAcknowledgeTopicResponse = topicResponsesMap.getOrElse(topicId2, null)
    assertEquals(2, topicIdResponse2.partitions().size())

    val partitionResponses2 = topicIdResponse2.partitions().asScala.toList
    val partitionResponsesMap2: Map[Int, ShareAcknowledgeResponseData.PartitionData] = partitionResponses2.map(partition => partition.partitionIndex -> partition).toMap
    assertTrue(partitionResponsesMap2.contains(0))
    assertTrue(partitionResponsesMap2.contains(1))
    assertTrue(partitionResponsesMap2.getOrElse(0, null).errorCode == Errors.TOPIC_AUTHORIZATION_FAILED.code)
    assertTrue(partitionResponsesMap2.getOrElse(1, null).errorCode == Errors.UNKNOWN_TOPIC_OR_PARTITION.code)
  }

  private def compareAcknowledgementBatches(baseOffset: Long,
                                            endOffset: Long,
                                            acknowledgementType: Byte,
                                            acknowledgementBatch: ShareAcknowledgementBatch
                                           ): Boolean = {
    if (baseOffset == acknowledgementBatch.firstOffset()
      && endOffset == acknowledgementBatch.lastOffset()
      && acknowledgementType == acknowledgementBatch.acknowledgeTypes().get(0)) {
      return true
    }
    false
  }

  private def compareAcknowledgeResponsePartitionData(partitionIndex: Int,
                                              ackErrorCode: Short,
                                              partitionData: ShareAcknowledgeResponseData.PartitionData
                                             ): Boolean = {
    if (partitionIndex == partitionData.partitionIndex() && ackErrorCode == partitionData.errorCode()) {
      return true
    }
    false
  }

  private def memoryRecordsBuilder(numOfRecords: Int, startOffset: Long): MemoryRecordsBuilder = {

    val buffer: ByteBuffer = ByteBuffer.allocate(1024)
    val compression: Compression = Compression.of(CompressionType.NONE).build()
    val timestampType: TimestampType = TimestampType.CREATE_TIME

    val builder: MemoryRecordsBuilder = MemoryRecords.builder(buffer, compression, timestampType, startOffset)
    for (i <- 0 until numOfRecords) {
      builder.appendWithOffset(startOffset + i, 0L, TestUtils.randomBytes(10), TestUtils.randomBytes(10))
    }
    builder
  }

  private def memoryRecords(numOfRecords: Int, startOffset: Long): MemoryRecords = {
    memoryRecordsBuilder(numOfRecords, startOffset).build()
  }

  @ParameterizedTest
  @ApiKeyVersionsSource(apiKey = ApiKeys.JOIN_GROUP)
  def testHandleJoinGroupRequest(version: Short): Unit = {
    val joinGroupRequest = new JoinGroupRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setProtocolType("consumer")
      .setRebalanceTimeoutMs(1000)
      .setSessionTimeoutMs(2000)

    val requestChannelRequest = buildRequest(new JoinGroupRequest.Builder(joinGroupRequest).build(version))

    val expectedJoinGroupRequest = new JoinGroupRequestData()
      .setGroupId(joinGroupRequest.groupId)
      .setMemberId(joinGroupRequest.memberId)
      .setProtocolType(joinGroupRequest.protocolType)
      .setRebalanceTimeoutMs(if (version >= 1) joinGroupRequest.rebalanceTimeoutMs else joinGroupRequest.sessionTimeoutMs)
      .setSessionTimeoutMs(joinGroupRequest.sessionTimeoutMs)

    val future = new CompletableFuture[JoinGroupResponseData]()
    when(groupCoordinator.joinGroup(
      requestChannelRequest.context,
      expectedJoinGroupRequest,
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleJoinGroupRequest(
      requestChannelRequest,
      RequestLocal.noCaching
    )

    val expectedJoinGroupResponse = new JoinGroupResponseData()
      .setMemberId("member")
      .setGenerationId(0)
      .setLeader("leader")
      .setProtocolType(if (version >= 7) "consumer" else null)
      .setProtocolName("range")

    future.complete(expectedJoinGroupResponse)
    val response = verifyNoThrottling[JoinGroupResponse](requestChannelRequest)
    assertEquals(expectedJoinGroupResponse, response.data)
  }

  @ParameterizedTest
  @ApiKeyVersionsSource(apiKey = ApiKeys.JOIN_GROUP)
  def testJoinGroupProtocolNameBackwardCompatibility(version: Short): Unit = {
    val joinGroupRequest = new JoinGroupRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setProtocolType("consumer")
      .setRebalanceTimeoutMs(1000)
      .setSessionTimeoutMs(2000)

    val requestChannelRequest = buildRequest(new JoinGroupRequest.Builder(joinGroupRequest).build(version))

    val expectedJoinGroupRequest = new JoinGroupRequestData()
      .setGroupId(joinGroupRequest.groupId)
      .setMemberId(joinGroupRequest.memberId)
      .setProtocolType(joinGroupRequest.protocolType)
      .setRebalanceTimeoutMs(if (version >= 1) joinGroupRequest.rebalanceTimeoutMs else joinGroupRequest.sessionTimeoutMs)
      .setSessionTimeoutMs(joinGroupRequest.sessionTimeoutMs)

    val future = new CompletableFuture[JoinGroupResponseData]()
    when(groupCoordinator.joinGroup(
      requestChannelRequest.context,
      expectedJoinGroupRequest,
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleJoinGroupRequest(
      requestChannelRequest,
      RequestLocal.noCaching
    )

    val joinGroupResponse = new JoinGroupResponseData()
      .setErrorCode(Errors.INCONSISTENT_GROUP_PROTOCOL.code)
      .setMemberId("member")
      .setProtocolName(null)

    val expectedJoinGroupResponse = new JoinGroupResponseData()
      .setErrorCode(Errors.INCONSISTENT_GROUP_PROTOCOL.code)
      .setMemberId("member")
      .setProtocolName(if (version >= 7) null else kafka.coordinator.group.GroupCoordinator.NoProtocol)

    future.complete(joinGroupResponse)
    val response = verifyNoThrottling[JoinGroupResponse](requestChannelRequest)
    assertEquals(expectedJoinGroupResponse, response.data)
  }

  @Test
  def testHandleJoinGroupRequestFutureFailed(): Unit = {
    val joinGroupRequest = new JoinGroupRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setProtocolType("consumer")
      .setRebalanceTimeoutMs(1000)
      .setSessionTimeoutMs(2000)

    val requestChannelRequest = buildRequest(new JoinGroupRequest.Builder(joinGroupRequest).build())

    val future = new CompletableFuture[JoinGroupResponseData]()
    when(groupCoordinator.joinGroup(
      requestChannelRequest.context,
      joinGroupRequest,
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleJoinGroupRequest(
      requestChannelRequest,
      RequestLocal.noCaching
    )

    future.completeExceptionally(Errors.REQUEST_TIMED_OUT.exception)
    val response = verifyNoThrottling[JoinGroupResponse](requestChannelRequest)
    assertEquals(Errors.REQUEST_TIMED_OUT, response.error)
  }

  @Test
  def testHandleJoinGroupRequestAuthorizationFailed(): Unit = {
    val joinGroupRequest = new JoinGroupRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setProtocolType("consumer")
      .setRebalanceTimeoutMs(1000)
      .setSessionTimeoutMs(2000)

    val requestChannelRequest = buildRequest(new JoinGroupRequest.Builder(joinGroupRequest).build())

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenReturn(Seq(AuthorizationResult.DENIED).asJava)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleJoinGroupRequest(
      requestChannelRequest,
      RequestLocal.noCaching
    )

    val response = verifyNoThrottling[JoinGroupResponse](requestChannelRequest)
    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED, response.error)
  }

  @Test
  def testHandleJoinGroupRequestUnexpectedException(): Unit = {
    val joinGroupRequest = new JoinGroupRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setProtocolType("consumer")
      .setRebalanceTimeoutMs(1000)
      .setSessionTimeoutMs(2000)

    val requestChannelRequest = buildRequest(new JoinGroupRequest.Builder(joinGroupRequest).build())

    val future = new CompletableFuture[JoinGroupResponseData]()
    when(groupCoordinator.joinGroup(
      requestChannelRequest.context,
      joinGroupRequest,
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)

    var response: JoinGroupResponse = null
    when(requestChannel.sendResponse(any(), any(), any())).thenAnswer { _ =>
      throw new Exception("Something went wrong")
    }.thenAnswer { invocation =>
      response = invocation.getArgument(1, classOf[JoinGroupResponse])
    }
    kafkaApis = createKafkaApis()
    kafkaApis.handle(
      requestChannelRequest,
      RequestLocal.noCaching
    )

    future.completeExceptionally(Errors.NOT_COORDINATOR.exception)

    // The exception expected here is the one thrown by `sendResponse`. As
    // `Exception` is not a Kafka errors, `UNKNOWN_SERVER_ERROR` is returned.
    assertEquals(Errors.UNKNOWN_SERVER_ERROR, response.error)
  }

  @ParameterizedTest
  @ApiKeyVersionsSource(apiKey = ApiKeys.SYNC_GROUP)
  def testHandleSyncGroupRequest(version: Short): Unit = {
    val syncGroupRequest = new SyncGroupRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setProtocolType("consumer")
      .setProtocolName("range")

    val requestChannelRequest = buildRequest(new SyncGroupRequest.Builder(syncGroupRequest).build(version))

    val expectedSyncGroupRequest = new SyncGroupRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setProtocolType(if (version >= 5) "consumer" else null)
      .setProtocolName(if (version >= 5) "range" else null)

    val future = new CompletableFuture[SyncGroupResponseData]()
    when(groupCoordinator.syncGroup(
      requestChannelRequest.context,
      expectedSyncGroupRequest,
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleSyncGroupRequest(
      requestChannelRequest,
      RequestLocal.noCaching
    )

    val expectedSyncGroupResponse = new SyncGroupResponseData()
      .setProtocolType(if (version >= 5 ) "consumer" else null)
      .setProtocolName(if (version >= 5 ) "range" else null)

    future.complete(expectedSyncGroupResponse)
    val response = verifyNoThrottling[SyncGroupResponse](requestChannelRequest)
    assertEquals(expectedSyncGroupResponse, response.data)
  }

  @Test
  def testHandleSyncGroupRequestFutureFailed(): Unit = {
    val syncGroupRequest = new SyncGroupRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setProtocolType("consumer")
      .setProtocolName("range")

    val requestChannelRequest = buildRequest(new SyncGroupRequest.Builder(syncGroupRequest).build())

    val expectedSyncGroupRequest = new SyncGroupRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setProtocolType("consumer")
      .setProtocolName("range")

    val future = new CompletableFuture[SyncGroupResponseData]()
    when(groupCoordinator.syncGroup(
      requestChannelRequest.context,
      expectedSyncGroupRequest,
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleSyncGroupRequest(
      requestChannelRequest,
      RequestLocal.noCaching
    )

    future.completeExceptionally(Errors.UNKNOWN_SERVER_ERROR.exception)
    val response = verifyNoThrottling[SyncGroupResponse](requestChannelRequest)
    assertEquals(Errors.UNKNOWN_SERVER_ERROR, response.error)
  }

  @Test
  def testHandleSyncGroupRequestAuthenticationFailed(): Unit = {
    val syncGroupRequest = new SyncGroupRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setProtocolType("consumer")
      .setProtocolName("range")

    val requestChannelRequest = buildRequest(new SyncGroupRequest.Builder(syncGroupRequest).build())

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenReturn(Seq(AuthorizationResult.DENIED).asJava)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleSyncGroupRequest(
      requestChannelRequest,
      RequestLocal.noCaching
    )

    val response = verifyNoThrottling[SyncGroupResponse](requestChannelRequest)
    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED, response.error)
  }

  @ParameterizedTest
  @ApiKeyVersionsSource(apiKey = ApiKeys.SYNC_GROUP)
  def testSyncGroupProtocolTypeAndNameAreMandatorySinceV5(version: Short): Unit = {
    val syncGroupRequest = new SyncGroupRequestData()
      .setGroupId("group")
      .setMemberId("member")

    val requestChannelRequest = buildRequest(new SyncGroupRequest.Builder(syncGroupRequest).build(version))

    val expectedSyncGroupRequest = new SyncGroupRequestData()
      .setGroupId("group")
      .setMemberId("member")

    val future = new CompletableFuture[SyncGroupResponseData]()
    when(groupCoordinator.syncGroup(
      requestChannelRequest.context,
      expectedSyncGroupRequest,
      RequestLocal.noCaching.bufferSupplier
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleSyncGroupRequest(
      requestChannelRequest,
      RequestLocal.noCaching
    )

    if (version < 5) {
      future.complete(new SyncGroupResponseData()
        .setProtocolType("consumer")
        .setProtocolName("range"))
    }

    val response = verifyNoThrottling[SyncGroupResponse](requestChannelRequest)

    if (version < 5) {
      assertEquals(Errors.NONE, response.error)
    } else {
      assertEquals(Errors.INCONSISTENT_GROUP_PROTOCOL, response.error)
    }
  }

  @ParameterizedTest
  @ApiKeyVersionsSource(apiKey = ApiKeys.HEARTBEAT)
  def testHandleHeartbeatRequest(version: Short): Unit = {
    val heartbeatRequest = new HeartbeatRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setGenerationId(0)

    val requestChannelRequest = buildRequest(new HeartbeatRequest.Builder(heartbeatRequest).build(version))

    val expectedHeartbeatRequest = new HeartbeatRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setGenerationId(0)

    val future = new CompletableFuture[HeartbeatResponseData]()
    when(groupCoordinator.heartbeat(
      requestChannelRequest.context,
      expectedHeartbeatRequest
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleHeartbeatRequest(requestChannelRequest)

    val expectedHeartbeatResponse = new HeartbeatResponseData()
    future.complete(expectedHeartbeatResponse)
    val response = verifyNoThrottling[HeartbeatResponse](requestChannelRequest)
    assertEquals(expectedHeartbeatResponse, response.data)
  }

  @Test
  def testHandleHeartbeatRequestFutureFailed(): Unit = {
    val heartbeatRequest = new HeartbeatRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setGenerationId(0)

    val requestChannelRequest = buildRequest(new HeartbeatRequest.Builder(heartbeatRequest).build())

    val expectedHeartbeatRequest = new HeartbeatRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setGenerationId(0)

    val future = new CompletableFuture[HeartbeatResponseData]()
    when(groupCoordinator.heartbeat(
      requestChannelRequest.context,
      expectedHeartbeatRequest
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleHeartbeatRequest(requestChannelRequest)

    future.completeExceptionally(Errors.UNKNOWN_SERVER_ERROR.exception)
    val response = verifyNoThrottling[HeartbeatResponse](requestChannelRequest)
    assertEquals(Errors.UNKNOWN_SERVER_ERROR, response.error)
  }

  @Test
  def testHandleHeartbeatRequestAuthenticationFailed(): Unit = {
    val heartbeatRequest = new HeartbeatRequestData()
      .setGroupId("group")
      .setMemberId("member")
      .setGenerationId(0)

    val requestChannelRequest = buildRequest(new HeartbeatRequest.Builder(heartbeatRequest).build())

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenReturn(Seq(AuthorizationResult.DENIED).asJava)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleHeartbeatRequest(
      requestChannelRequest
    )

    val response = verifyNoThrottling[HeartbeatResponse](requestChannelRequest)
    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED, response.error)
  }

  @ParameterizedTest
  @ApiKeyVersionsSource(apiKey = ApiKeys.LEAVE_GROUP)
  def testHandleLeaveGroupWithMultipleMembers(version: Short): Unit = {
    def makeRequest(version: Short): RequestChannel.Request = {
      buildRequest(new LeaveGroupRequest.Builder(
        "group",
        List(
          new MemberIdentity()
            .setMemberId("member-1")
            .setGroupInstanceId("instance-1"),
          new MemberIdentity()
            .setMemberId("member-2")
            .setGroupInstanceId("instance-2")
        ).asJava
      ).build(version))
    }

    if (version < 3) {
      // Request version earlier than version 3 do not support batching members.
      assertThrows(classOf[UnsupportedVersionException], () => makeRequest(version))
    } else {
      val requestChannelRequest = makeRequest(version)

      val expectedLeaveGroupRequest = new LeaveGroupRequestData()
        .setGroupId("group")
        .setMembers(List(
          new MemberIdentity()
            .setMemberId("member-1")
            .setGroupInstanceId("instance-1"),
          new MemberIdentity()
            .setMemberId("member-2")
            .setGroupInstanceId("instance-2")
        ).asJava)

      val future = new CompletableFuture[LeaveGroupResponseData]()
      when(groupCoordinator.leaveGroup(
        requestChannelRequest.context,
        expectedLeaveGroupRequest
      )).thenReturn(future)
      kafkaApis = createKafkaApis()
      kafkaApis.handleLeaveGroupRequest(requestChannelRequest)

      val expectedLeaveResponse = new LeaveGroupResponseData()
        .setErrorCode(Errors.NONE.code)
        .setMembers(List(
          new LeaveGroupResponseData.MemberResponse()
            .setMemberId("member-1")
            .setGroupInstanceId("instance-1"),
          new LeaveGroupResponseData.MemberResponse()
            .setMemberId("member-2")
            .setGroupInstanceId("instance-2"),
        ).asJava)

      future.complete(expectedLeaveResponse)
      val response = verifyNoThrottling[LeaveGroupResponse](requestChannelRequest)
      assertEquals(expectedLeaveResponse, response.data)
    }
  }

  @ParameterizedTest
  @ApiKeyVersionsSource(apiKey = ApiKeys.LEAVE_GROUP)
  def testHandleLeaveGroupWithSingleMember(version: Short): Unit = {
    val requestChannelRequest = buildRequest(new LeaveGroupRequest.Builder(
      "group",
      List(
        new MemberIdentity()
          .setMemberId("member-1")
          .setGroupInstanceId("instance-1")
      ).asJava
    ).build(version))

    val expectedLeaveGroupRequest = new LeaveGroupRequestData()
      .setGroupId("group")
      .setMembers(List(
        new MemberIdentity()
          .setMemberId("member-1")
          .setGroupInstanceId(if (version >= 3) "instance-1" else null)
      ).asJava)

    val future = new CompletableFuture[LeaveGroupResponseData]()
    when(groupCoordinator.leaveGroup(
      requestChannelRequest.context,
      expectedLeaveGroupRequest
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleLeaveGroupRequest(requestChannelRequest)

    val leaveGroupResponse = new LeaveGroupResponseData()
      .setErrorCode(Errors.NONE.code)
      .setMembers(List(
        new LeaveGroupResponseData.MemberResponse()
          .setMemberId("member-1")
          .setGroupInstanceId("instance-1")
      ).asJava)

    val expectedLeaveResponse = if (version >= 3) {
      new LeaveGroupResponseData()
        .setErrorCode(Errors.NONE.code)
        .setMembers(List(
          new LeaveGroupResponseData.MemberResponse()
            .setMemberId("member-1")
            .setGroupInstanceId("instance-1")
        ).asJava)
    } else {
      new LeaveGroupResponseData()
        .setErrorCode(Errors.NONE.code)
    }

    future.complete(leaveGroupResponse)
    val response = verifyNoThrottling[LeaveGroupResponse](requestChannelRequest)
    assertEquals(expectedLeaveResponse, response.data)
  }

  @Test
  def testHandleLeaveGroupFutureFailed(): Unit = {
    val requestChannelRequest = buildRequest(new LeaveGroupRequest.Builder(
      "group",
      List(
        new MemberIdentity()
          .setMemberId("member-1")
          .setGroupInstanceId("instance-1")
      ).asJava
    ).build(ApiKeys.LEAVE_GROUP.latestVersion))

    val expectedLeaveGroupRequest = new LeaveGroupRequestData()
      .setGroupId("group")
      .setMembers(List(
        new MemberIdentity()
          .setMemberId("member-1")
          .setGroupInstanceId("instance-1")
      ).asJava)

    val future = new CompletableFuture[LeaveGroupResponseData]()
    when(groupCoordinator.leaveGroup(
      requestChannelRequest.context,
      expectedLeaveGroupRequest
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleLeaveGroupRequest(requestChannelRequest)

    future.completeExceptionally(Errors.UNKNOWN_SERVER_ERROR.exception)
    val response = verifyNoThrottling[LeaveGroupResponse](requestChannelRequest)
    assertEquals(Errors.UNKNOWN_SERVER_ERROR, response.error)
  }

  @Test
  def testHandleLeaveGroupAuthenticationFailed(): Unit = {
    val requestChannelRequest = buildRequest(new LeaveGroupRequest.Builder(
      "group",
      List(
        new MemberIdentity()
          .setMemberId("member-1")
          .setGroupInstanceId("instance-1")
      ).asJava
    ).build(ApiKeys.LEAVE_GROUP.latestVersion))

    val expectedLeaveGroupRequest = new LeaveGroupRequestData()
      .setGroupId("group")
      .setMembers(List(
        new MemberIdentity()
          .setMemberId("member-1")
          .setGroupInstanceId("instance-1")
      ).asJava)

    val future = new CompletableFuture[LeaveGroupResponseData]()
    when(groupCoordinator.leaveGroup(
      requestChannelRequest.context,
      expectedLeaveGroupRequest
    )).thenReturn(future)

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenReturn(Seq(AuthorizationResult.DENIED).asJava)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleLeaveGroupRequest(requestChannelRequest)

    val response = verifyNoThrottling[LeaveGroupResponse](requestChannelRequest)
    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED, response.error)
  }

  @ParameterizedTest
  @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_FETCH)
  def testHandleOffsetFetchWithMultipleGroups(version: Short): Unit = {
    def makeRequest(version: Short): RequestChannel.Request = {
      val groups = Map(
        "group-1" -> List(
          new TopicPartition("foo", 0),
          new TopicPartition("foo", 1)
        ).asJava,
        "group-2" -> null,
        "group-3" -> null,
        "group-4" -> null,
      ).asJava
      buildRequest(new OffsetFetchRequest.Builder(groups, false, false).build(version))
    }

    if (version < 8) {
      // Request version earlier than version 8 do not support batching groups.
      assertThrows(classOf[UnsupportedVersionException], () => makeRequest(version))
    } else {
      val requestChannelRequest = makeRequest(version)

      val group1Future = new CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup]()
      when(groupCoordinator.fetchOffsets(
        requestChannelRequest.context,
        new OffsetFetchRequestData.OffsetFetchRequestGroup()
          .setGroupId("group-1")
          .setTopics(List(
            new OffsetFetchRequestData.OffsetFetchRequestTopics()
              .setName("foo")
              .setPartitionIndexes(List[Integer](0, 1).asJava)).asJava),
        false
      )).thenReturn(group1Future)

      val group2Future = new CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup]()
      when(groupCoordinator.fetchAllOffsets(
        requestChannelRequest.context,
        new OffsetFetchRequestData.OffsetFetchRequestGroup()
          .setGroupId("group-2")
          .setTopics(null),
        false
      )).thenReturn(group2Future)

      val group3Future = new CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup]()
      when(groupCoordinator.fetchAllOffsets(
        requestChannelRequest.context,
        new OffsetFetchRequestData.OffsetFetchRequestGroup()
          .setGroupId("group-3")
          .setTopics(null),
        false
      )).thenReturn(group3Future)

      val group4Future = new CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup]()
      when(groupCoordinator.fetchAllOffsets(
        requestChannelRequest.context,
        new OffsetFetchRequestData.OffsetFetchRequestGroup()
          .setGroupId("group-4")
          .setTopics(null),
        false
      )).thenReturn(group4Future)
      kafkaApis = createKafkaApis()
      kafkaApis.handleOffsetFetchRequest(requestChannelRequest)

      val group1Response = new OffsetFetchResponseData.OffsetFetchResponseGroup()
        .setGroupId("group-1")
        .setTopics(List(
          new OffsetFetchResponseData.OffsetFetchResponseTopics()
            .setName("foo")
            .setPartitions(List(
              new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                .setPartitionIndex(0)
                .setCommittedOffset(100)
                .setCommittedLeaderEpoch(1),
              new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                .setPartitionIndex(1)
                .setCommittedOffset(200)
                .setCommittedLeaderEpoch(2)
            ).asJava)
        ).asJava)

      val group2Response = new OffsetFetchResponseData.OffsetFetchResponseGroup()
        .setGroupId("group-2")
        .setTopics(List(
          new OffsetFetchResponseData.OffsetFetchResponseTopics()
            .setName("bar")
            .setPartitions(List(
              new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                .setPartitionIndex(0)
                .setCommittedOffset(100)
                .setCommittedLeaderEpoch(1),
              new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                .setPartitionIndex(1)
                .setCommittedOffset(200)
                .setCommittedLeaderEpoch(2),
              new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                .setPartitionIndex(2)
                .setCommittedOffset(300)
                .setCommittedLeaderEpoch(3)
            ).asJava)
        ).asJava)

      val group3Response = new OffsetFetchResponseData.OffsetFetchResponseGroup()
        .setGroupId("group-3")
        .setErrorCode(Errors.INVALID_GROUP_ID.code)

      val group4Response = new OffsetFetchResponseData.OffsetFetchResponseGroup()
        .setGroupId("group-4")
        .setErrorCode(Errors.INVALID_GROUP_ID.code)

      val expectedGroups = List(group1Response, group2Response, group3Response, group4Response)

      group1Future.complete(group1Response)
      group2Future.complete(group2Response)
      group3Future.completeExceptionally(Errors.INVALID_GROUP_ID.exception)
      group4Future.complete(group4Response)

      val response = verifyNoThrottling[OffsetFetchResponse](requestChannelRequest)
      assertEquals(expectedGroups.toSet, response.data.groups().asScala.toSet)
    }
  }

  @ParameterizedTest
  @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_FETCH)
  def testHandleOffsetFetchWithSingleGroup(version: Short): Unit = {
    def makeRequest(version: Short): RequestChannel.Request = {
      buildRequest(new OffsetFetchRequest.Builder(
        "group-1",
        false,
        List(
          new TopicPartition("foo", 0),
          new TopicPartition("foo", 1)
        ).asJava,
        false
      ).build(version))
    }

    val requestChannelRequest = makeRequest(version)

    val future = new CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup]()
    when(groupCoordinator.fetchOffsets(
      requestChannelRequest.context,
      new OffsetFetchRequestData.OffsetFetchRequestGroup()
        .setGroupId("group-1")
        .setTopics(List(new OffsetFetchRequestData.OffsetFetchRequestTopics()
          .setName("foo")
          .setPartitionIndexes(List[Integer](0, 1).asJava)).asJava),
      false
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleOffsetFetchRequest(requestChannelRequest)

    val group1Response = new OffsetFetchResponseData.OffsetFetchResponseGroup()
      .setGroupId("group-1")
      .setTopics(List(
        new OffsetFetchResponseData.OffsetFetchResponseTopics()
          .setName("foo")
          .setPartitions(List(
            new OffsetFetchResponseData.OffsetFetchResponsePartitions()
              .setPartitionIndex(0)
              .setCommittedOffset(100)
              .setCommittedLeaderEpoch(1),
            new OffsetFetchResponseData.OffsetFetchResponsePartitions()
              .setPartitionIndex(1)
              .setCommittedOffset(200)
              .setCommittedLeaderEpoch(2)
          ).asJava)
      ).asJava)

    val expectedOffsetFetchResponse = if (version >= 8) {
      new OffsetFetchResponseData()
        .setGroups(List(group1Response).asJava)
    } else {
      new OffsetFetchResponseData()
        .setTopics(List(
          new OffsetFetchResponseData.OffsetFetchResponseTopic()
            .setName("foo")
            .setPartitions(List(
              new OffsetFetchResponseData.OffsetFetchResponsePartition()
                .setPartitionIndex(0)
                .setCommittedOffset(100)
                .setCommittedLeaderEpoch(if (version >= 5) 1 else -1),
              new OffsetFetchResponseData.OffsetFetchResponsePartition()
                .setPartitionIndex(1)
                .setCommittedOffset(200)
                .setCommittedLeaderEpoch(if (version >= 5) 2 else -1)
            ).asJava)
        ).asJava)
    }

    future.complete(group1Response)

    val response = verifyNoThrottling[OffsetFetchResponse](requestChannelRequest)
    assertEquals(expectedOffsetFetchResponse, response.data)
  }

  @ParameterizedTest
  @ApiKeyVersionsSource(apiKey = ApiKeys.OFFSET_FETCH)
  def testHandleOffsetFetchAllOffsetsWithSingleGroup(version: Short): Unit = {
    // Version 0 gets offsets from Zookeeper. Version 1 does not support fetching all
    // offsets request. We are not interested in testing these here.
    if (version < 2) return

    def makeRequest(version: Short): RequestChannel.Request = {
      buildRequest(new OffsetFetchRequest.Builder(
        "group-1",
        false,
        null, // all offsets.
        false
      ).build(version))
    }

    val requestChannelRequest = makeRequest(version)

    val future = new CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup]()
    when(groupCoordinator.fetchAllOffsets(
      requestChannelRequest.context,
      new OffsetFetchRequestData.OffsetFetchRequestGroup()
        .setGroupId("group-1")
        .setTopics(null),
      false
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleOffsetFetchRequest(requestChannelRequest)

    val group1Response = new OffsetFetchResponseData.OffsetFetchResponseGroup()
      .setGroupId("group-1")
      .setTopics(List(
        new OffsetFetchResponseData.OffsetFetchResponseTopics()
          .setName("foo")
          .setPartitions(List(
            new OffsetFetchResponseData.OffsetFetchResponsePartitions()
              .setPartitionIndex(0)
              .setCommittedOffset(100)
              .setCommittedLeaderEpoch(1),
            new OffsetFetchResponseData.OffsetFetchResponsePartitions()
              .setPartitionIndex(1)
              .setCommittedOffset(200)
              .setCommittedLeaderEpoch(2)
          ).asJava)
      ).asJava)

    val expectedOffsetFetchResponse = if (version >= 8) {
      new OffsetFetchResponseData()
        .setGroups(List(group1Response).asJava)
    } else {
      new OffsetFetchResponseData()
        .setTopics(List(
          new OffsetFetchResponseData.OffsetFetchResponseTopic()
            .setName("foo")
            .setPartitions(List(
              new OffsetFetchResponseData.OffsetFetchResponsePartition()
                .setPartitionIndex(0)
                .setCommittedOffset(100)
                .setCommittedLeaderEpoch(if (version >= 5) 1 else -1),
              new OffsetFetchResponseData.OffsetFetchResponsePartition()
                .setPartitionIndex(1)
                .setCommittedOffset(200)
                .setCommittedLeaderEpoch(if (version >= 5) 2 else -1)
            ).asJava)
        ).asJava)
    }

    future.complete(group1Response)

    val response = verifyNoThrottling[OffsetFetchResponse](requestChannelRequest)
    assertEquals(expectedOffsetFetchResponse, response.data)
  }

  @Test
  def testHandleOffsetFetchAuthorization(): Unit = {
    def makeRequest(version: Short): RequestChannel.Request = {
      val groups = Map(
        "group-1" -> List(
          new TopicPartition("foo", 0),
          new TopicPartition("bar", 0)
        ).asJava,
        "group-2" -> List(
          new TopicPartition("foo", 0),
          new TopicPartition("bar", 0)
        ).asJava,
        "group-3" -> null,
        "group-4" -> null,
      ).asJava
      buildRequest(new OffsetFetchRequest.Builder(groups, false, false).build(version))
    }

    val requestChannelRequest = makeRequest(ApiKeys.OFFSET_FETCH.latestVersion)

    val authorizer: Authorizer = mock(classOf[Authorizer])

    val acls = Map(
      "group-1" -> AuthorizationResult.ALLOWED,
      "group-2" -> AuthorizationResult.DENIED,
      "group-3" -> AuthorizationResult.ALLOWED,
      "group-4" -> AuthorizationResult.DENIED,
      "foo" -> AuthorizationResult.DENIED,
      "bar" -> AuthorizationResult.ALLOWED
    )

    when(authorizer.authorize(
      any[RequestContext],
      any[util.List[Action]]
    )).thenAnswer { invocation =>
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      actions.asScala.map { action =>
        acls.getOrElse(action.resourcePattern.name, AuthorizationResult.DENIED)
      }.asJava
    }

    // group-1 is allowed and bar is allowed.
    val group1Future = new CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup]()
    when(groupCoordinator.fetchOffsets(
      requestChannelRequest.context,
      new OffsetFetchRequestData.OffsetFetchRequestGroup()
        .setGroupId("group-1")
        .setTopics(List(new OffsetFetchRequestData.OffsetFetchRequestTopics()
          .setName("bar")
          .setPartitionIndexes(List[Integer](0).asJava)).asJava),
      false
    )).thenReturn(group1Future)

    // group-3 is allowed and bar is allowed.
    val group3Future = new CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup]()
    when(groupCoordinator.fetchAllOffsets(
      requestChannelRequest.context,
      new OffsetFetchRequestData.OffsetFetchRequestGroup()
        .setGroupId("group-3")
        .setTopics(null),
      false
    )).thenReturn(group3Future)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val group1ResponseFromCoordinator = new OffsetFetchResponseData.OffsetFetchResponseGroup()
      .setGroupId("group-1")
      .setTopics(List(
        new OffsetFetchResponseData.OffsetFetchResponseTopics()
          .setName("bar")
          .setPartitions(List(
            new OffsetFetchResponseData.OffsetFetchResponsePartitions()
              .setPartitionIndex(0)
              .setCommittedOffset(100)
              .setCommittedLeaderEpoch(1)
          ).asJava)
      ).asJava)

    val group3ResponseFromCoordinator = new OffsetFetchResponseData.OffsetFetchResponseGroup()
      .setGroupId("group-3")
      .setTopics(List(
        // foo should be filtered out.
        new OffsetFetchResponseData.OffsetFetchResponseTopics()
          .setName("foo")
          .setPartitions(List(
            new OffsetFetchResponseData.OffsetFetchResponsePartitions()
              .setPartitionIndex(0)
              .setCommittedOffset(100)
              .setCommittedLeaderEpoch(1)
          ).asJava),
        new OffsetFetchResponseData.OffsetFetchResponseTopics()
          .setName("bar")
          .setPartitions(List(
            new OffsetFetchResponseData.OffsetFetchResponsePartitions()
              .setPartitionIndex(0)
              .setCommittedOffset(100)
              .setCommittedLeaderEpoch(1)
          ).asJava)
      ).asJava)

    val expectedOffsetFetchResponse = new OffsetFetchResponseData()
      .setGroups(List(
        // group-1 is authorized but foo is not.
        new OffsetFetchResponseData.OffsetFetchResponseGroup()
          .setGroupId("group-1")
          .setTopics(List(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
              .setName("bar")
              .setPartitions(List(
                new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                  .setPartitionIndex(0)
                  .setCommittedOffset(100)
                  .setCommittedLeaderEpoch(1)
              ).asJava),
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
              .setName("foo")
              .setPartitions(List(
                new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                  .setPartitionIndex(0)
                  .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code)
                  .setCommittedOffset(-1)
              ).asJava)
          ).asJava),
        // group-2 is not authorized.
        new OffsetFetchResponseData.OffsetFetchResponseGroup()
          .setGroupId("group-2")
          .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code),
        // group-3 is authorized but foo is not.
        new OffsetFetchResponseData.OffsetFetchResponseGroup()
          .setGroupId("group-3")
          .setTopics(List(
            new OffsetFetchResponseData.OffsetFetchResponseTopics()
              .setName("bar")
              .setPartitions(List(
                new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                  .setPartitionIndex(0)
                  .setCommittedOffset(100)
                  .setCommittedLeaderEpoch(1)
              ).asJava)
          ).asJava),
        // group-4 is not authorized.
        new OffsetFetchResponseData.OffsetFetchResponseGroup()
          .setGroupId("group-4")
          .setErrorCode(Errors.GROUP_AUTHORIZATION_FAILED.code),
      ).asJava)

    group1Future.complete(group1ResponseFromCoordinator)
    group3Future.complete(group3ResponseFromCoordinator)

    val response = verifyNoThrottling[OffsetFetchResponse](requestChannelRequest)
    assertEquals(expectedOffsetFetchResponse, response.data)
  }

  @Test
  def testHandleOffsetFetchWithUnauthorizedTopicAndTopLevelError(): Unit = {
    def makeRequest(version: Short): RequestChannel.Request = {
      val groups = Map(
        "group-1" -> List(
          new TopicPartition("foo", 0),
          new TopicPartition("bar", 0)
        ).asJava,
        "group-2" -> List(
          new TopicPartition("foo", 0),
          new TopicPartition("bar", 0)
        ).asJava
      ).asJava
      buildRequest(new OffsetFetchRequest.Builder(groups, false, false).build(version))
    }

    val requestChannelRequest = makeRequest(ApiKeys.OFFSET_FETCH.latestVersion)

    val authorizer: Authorizer = mock(classOf[Authorizer])

    val acls = Map(
      "group-1" -> AuthorizationResult.ALLOWED,
      "group-2" -> AuthorizationResult.ALLOWED,
      "foo" -> AuthorizationResult.DENIED,
      "bar" -> AuthorizationResult.ALLOWED
    )

    when(authorizer.authorize(
      any[RequestContext],
      any[util.List[Action]]
    )).thenAnswer { invocation =>
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      actions.asScala.map { action =>
        acls.getOrElse(action.resourcePattern.name, AuthorizationResult.DENIED)
      }.asJava
    }

    // group-1 and group-2 are allowed and bar is allowed.
    val group1Future = new CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup]()
    when(groupCoordinator.fetchOffsets(
      requestChannelRequest.context,
      new OffsetFetchRequestData.OffsetFetchRequestGroup()
        .setGroupId("group-1")
        .setTopics(List(new OffsetFetchRequestData.OffsetFetchRequestTopics()
          .setName("bar")
          .setPartitionIndexes(List[Integer](0).asJava)).asJava),
      false
    )).thenReturn(group1Future)

    val group2Future = new CompletableFuture[OffsetFetchResponseData.OffsetFetchResponseGroup]()
    when(groupCoordinator.fetchOffsets(
      requestChannelRequest.context,
      new OffsetFetchRequestData.OffsetFetchRequestGroup()
        .setGroupId("group-2")
        .setTopics(List(new OffsetFetchRequestData.OffsetFetchRequestTopics()
          .setName("bar")
          .setPartitionIndexes(List[Integer](0).asJava)).asJava),
      false
    )).thenReturn(group1Future)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    // group-2 mocks using the new group coordinator.
    // When the coordinator is not active, a response with top-level error code is returned
    // despite that the requested topic is not authorized and fails.
    val group2ResponseFromCoordinator = new OffsetFetchResponseData.OffsetFetchResponseGroup()
      .setGroupId("group-2")
      .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code)

    val expectedOffsetFetchResponse = new OffsetFetchResponseData()
      .setGroups(List(
        new OffsetFetchResponseData.OffsetFetchResponseGroup()
          .setGroupId("group-1")
          .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code),
        group2ResponseFromCoordinator
      ).asJava)

    group1Future.completeExceptionally(Errors.COORDINATOR_NOT_AVAILABLE.exception)
    group2Future.complete(group2ResponseFromCoordinator)

    val response = verifyNoThrottling[OffsetFetchResponse](requestChannelRequest)
    assertEquals(expectedOffsetFetchResponse, response.data)
  }

  @Test
  def testReassignmentAndReplicationBytesOutRateWhenReassigning(): Unit = {
    assertReassignmentAndReplicationBytesOutPerSec(true)
  }

  @Test
  def testReassignmentAndReplicationBytesOutRateWhenNotReassigning(): Unit = {
    assertReassignmentAndReplicationBytesOutPerSec(false)
  }

  private def assertReassignmentAndReplicationBytesOutPerSec(isReassigning: Boolean): Unit = {
    val leaderEpoch = 0
    val tp0 = new TopicPartition("tp", 0)
    val topicId = Uuid.randomUuid()
    val tidp0 = new TopicIdPartition(topicId, tp0)

    setupBasicMetadataCache(tp0.topic, numPartitions = 1, 1, topicId)
    val hw = 3

    val fetchDataBuilder = Collections.singletonMap(tp0, new FetchRequest.PartitionData(Uuid.ZERO_UUID, 0, 0, Int.MaxValue, Optional.of(leaderEpoch)))
    val fetchData = Collections.singletonMap(tidp0, new FetchRequest.PartitionData(Uuid.ZERO_UUID, 0, 0, Int.MaxValue, Optional.of(leaderEpoch)))
    val fetchFromFollower = buildRequest(new FetchRequest.Builder(
      ApiKeys.FETCH.oldestVersion(), ApiKeys.FETCH.latestVersion(), 1, 1, 1000, 0, fetchDataBuilder).build())

    val records = MemoryRecords.withRecords(Compression.NONE,
      new SimpleRecord(1000, "foo".getBytes(StandardCharsets.UTF_8)))
    when(replicaManager.fetchMessages(
      any[FetchParams],
      any[Seq[(TopicIdPartition, FetchRequest.PartitionData)]],
      any[ReplicaQuota],
      any[Seq[(TopicIdPartition, FetchPartitionData)] => Unit]()
    )).thenAnswer(invocation => {
      val callback = invocation.getArgument(3).asInstanceOf[Seq[(TopicIdPartition, FetchPartitionData)] => Unit]
      callback(Seq(tidp0 -> new FetchPartitionData(Errors.NONE, hw, 0, records,
        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), isReassigning)))
    })

    val fetchMetadata = new JFetchMetadata(0, 0)
    val fetchContext = new FullFetchContext(time, new FetchSessionCacheShard(1000, 100),
      fetchMetadata, fetchData, true, true)
    when(fetchManager.newContext(
      any[Short],
      any[JFetchMetadata],
      any[Boolean],
      any[util.Map[TopicIdPartition, FetchRequest.PartitionData]],
      any[util.List[TopicIdPartition]],
      any[util.Map[Uuid, String]])).thenReturn(fetchContext)

    when(replicaManager.getLogConfig(ArgumentMatchers.eq(tp0))).thenReturn(None)
    when(replicaManager.isAddingReplica(any(), anyInt)).thenReturn(isReassigning)
    kafkaApis = createKafkaApis()
    kafkaApis.handle(fetchFromFollower, RequestLocal.withThreadConfinedCaching)
    verify(replicaQuotaManager).record(anyLong)

    if (isReassigning)
      assertEquals(records.sizeInBytes(), brokerTopicStats.allTopicsStats.reassignmentBytesOutPerSec.get.count())
    else
      assertEquals(0, brokerTopicStats.allTopicsStats.reassignmentBytesOutPerSec.get.count())
    assertEquals(records.sizeInBytes(), brokerTopicStats.allTopicsStats.replicationBytesOutRate.get.count())
  }

  @ParameterizedTest
  @ApiKeyVersionsSource(apiKey = ApiKeys.LIST_GROUPS)
  def testListGroupsRequest(version: Short): Unit = {
    val listGroupsRequest = new ListGroupsRequestData()
      .setStatesFilter(if (version >= 4) List("Stable", "Empty").asJava else List.empty.asJava)
      .setTypesFilter(if (version >= 5) List("classic", "consumer").asJava else List.empty.asJava)

    val requestChannelRequest = buildRequest(new ListGroupsRequest.Builder(listGroupsRequest).build(version))

    val expectedListGroupsRequest = new ListGroupsRequestData()
      .setStatesFilter(if (version >= 4) List("Stable", "Empty").asJava else List.empty.asJava)
      .setTypesFilter(if (version >= 5) List("classic", "consumer").asJava else List.empty.asJava)

    val future = new CompletableFuture[ListGroupsResponseData]()
    when(groupCoordinator.listGroups(
      requestChannelRequest.context,
      expectedListGroupsRequest
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleListGroupsRequest(requestChannelRequest)

    val expectedListGroupsResponse = new ListGroupsResponseData()
      .setGroups(List(
        new ListGroupsResponseData.ListedGroup()
          .setGroupId("group1")
          .setProtocolType("protocol1")
          .setGroupState(if (version >= 4) "Stable" else "")
          .setGroupType(if (version >= 5) "consumer" else ""),
        new ListGroupsResponseData.ListedGroup()
          .setGroupId("group2")
          .setProtocolType("protocol2")
          .setGroupState(if (version >= 4) "Empty" else "")
          .setGroupType(if (version >= 5) "classic" else ""),
        new ListGroupsResponseData.ListedGroup()
          .setGroupId("group3")
          .setProtocolType("protocol3")
          .setGroupState(if (version >= 4) "Stable" else "")
          .setGroupType(if (version >= 5) "classic" else ""),
      ).asJava)

    future.complete(expectedListGroupsResponse)
    val response = verifyNoThrottling[ListGroupsResponse](requestChannelRequest)
    assertEquals(expectedListGroupsResponse, response.data)
  }

  @Test
  def testListGroupsRequestFutureFailed(): Unit = {
    val listGroupsRequest = new ListGroupsRequestData()
      .setStatesFilter(List("Stable", "Empty").asJava)
      .setTypesFilter(List("classic", "consumer").asJava)

    val requestChannelRequest = buildRequest(new ListGroupsRequest.Builder(listGroupsRequest).build())

    val expectedListGroupsRequest = new ListGroupsRequestData()
      .setStatesFilter(List("Stable", "Empty").asJava)
      .setTypesFilter(List("classic", "consumer").asJava)

    val future = new CompletableFuture[ListGroupsResponseData]()
    when(groupCoordinator.listGroups(
      requestChannelRequest.context,
      expectedListGroupsRequest
    )).thenReturn(future)
    kafkaApis = createKafkaApis()
    kafkaApis.handleListGroupsRequest(requestChannelRequest)

    future.completeExceptionally(Errors.UNKNOWN_SERVER_ERROR.exception)
    val response = verifyNoThrottling[ListGroupsResponse](requestChannelRequest)
    assertEquals(Errors.UNKNOWN_SERVER_ERROR.code, response.data.errorCode)
  }

  @Test
  def testListGroupsRequestFiltersUnauthorizedGroupsWithDescribeCluster(): Unit = {
    val authorizer: Authorizer = mock(classOf[Authorizer])

    authorizeResource(
      authorizer,
      AclOperation.DESCRIBE,
      ResourceType.GROUP,
      "group1",
      AuthorizationResult.DENIED,
      logIfDenied = false
    )
    authorizeResource(
      authorizer,
      AclOperation.DESCRIBE,
      ResourceType.GROUP,
      "group2",
      AuthorizationResult.DENIED,
      logIfDenied = false
    )
    authorizeResource(
      authorizer,
      AclOperation.DESCRIBE,
      ResourceType.CLUSTER,
      Resource.CLUSTER_NAME,
      AuthorizationResult.ALLOWED,
      logIfDenied = false
    )

    testListGroupsRequestFiltersUnauthorizedGroups(
      authorizer,
      List("group1", "group2"),
      List("group1", "group2")
    )
  }

  @Test
  def testListGroupsRequestFiltersUnauthorizedGroupsWithDescribeGroups(): Unit = {
    val authorizer: Authorizer = mock(classOf[Authorizer])

    authorizeResource(
      authorizer,
      AclOperation.DESCRIBE,
      ResourceType.GROUP,
      "group1",
      AuthorizationResult.DENIED,
      logIfDenied = false
    )
    authorizeResource(
      authorizer,
      AclOperation.DESCRIBE,
      ResourceType.GROUP,
      "group2",
      AuthorizationResult.ALLOWED,
      logIfDenied = false
    )
    authorizeResource(
      authorizer,
      AclOperation.DESCRIBE,
      ResourceType.CLUSTER,
      Resource.CLUSTER_NAME,
      AuthorizationResult.DENIED,
      logIfDenied = false
    )

    testListGroupsRequestFiltersUnauthorizedGroups(
      authorizer,
      List("group1", "group2"),
      List("group2")
    )
  }

  def testListGroupsRequestFiltersUnauthorizedGroups(
    authorizer: Authorizer,
    groups: List[String],
    expectedGroups: List[String],
  ): Unit = {
    val listGroupsRequest = new ListGroupsRequestData()

    val requestChannelRequest = buildRequest(new ListGroupsRequest.Builder(listGroupsRequest).build())

    val expectedListGroupsRequest = new ListGroupsRequestData()

    val future = new CompletableFuture[ListGroupsResponseData]()
    when(groupCoordinator.listGroups(
      requestChannelRequest.context,
      expectedListGroupsRequest
    )).thenReturn(future)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleListGroupsRequest(requestChannelRequest)

    val listGroupsResponse = new ListGroupsResponseData()
    groups.foreach { groupId =>
      listGroupsResponse.groups.add(new ListGroupsResponseData.ListedGroup()
        .setGroupId(groupId)
      )
    }

    val expectedListGroupsResponse = new ListGroupsResponseData()
    expectedGroups.foreach { groupId =>
      expectedListGroupsResponse.groups.add(new ListGroupsResponseData.ListedGroup()
        .setGroupId(groupId)
      )
    }

    future.complete(listGroupsResponse)
    val response = verifyNoThrottling[ListGroupsResponse](requestChannelRequest)
    assertEquals(expectedListGroupsResponse, response.data)
  }

  @Test
  def testDescribeClusterRequest(): Unit = {
    val plaintextListener = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)
    val endpoints = new BrokerEndpointCollection()
    endpoints.add(
      new BrokerEndpoint()
        .setHost("broker0")
        .setPort(9092)
        .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
        .setName(plaintextListener.value)
    )
    endpoints.add(
      new BrokerEndpoint()
        .setHost("broker1")
        .setPort(9092)
        .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
        .setName(plaintextListener.value)
    )

    MetadataCacheTest.updateCache(metadataCache,
      Seq(new RegisterBrokerRecord()
        .setBrokerId(brokerId)
        .setRack("rack")
        .setFenced(false)
        .setEndPoints(endpoints)))

    val describeClusterRequest = new DescribeClusterRequest.Builder(new DescribeClusterRequestData()
      .setIncludeClusterAuthorizedOperations(true)).build()

    val request = buildRequest(describeClusterRequest, plaintextListener)
    kafkaApis = createKafkaApis()
    kafkaApis.handleDescribeCluster(request)

    val describeClusterResponse = verifyNoThrottling[DescribeClusterResponse](request)

    assertEquals(clusterId, describeClusterResponse.data.clusterId)
    assertEquals(8096, describeClusterResponse.data.clusterAuthorizedOperations)
    assertEquals(metadataCache.getAliveBrokerNodes(plaintextListener).toSet,
      describeClusterResponse.nodes.asScala.values.toSet)
  }

  /**
   * Return pair of listener names in the metadataCache: PLAINTEXT and LISTENER2 respectively.
   */
  private def updateMetadataCacheWithInconsistentListeners(): (ListenerName, ListenerName) = {
    val plaintextListener = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)
    val anotherListener = new ListenerName("LISTENER2")

    val endpoints0 = new BrokerEndpointCollection()
    endpoints0.add(
      new BrokerEndpoint()
        .setHost("broker0")
        .setPort(9092)
        .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
        .setName(plaintextListener.value)
    )
    endpoints0.add(
      new BrokerEndpoint()
        .setHost("broker0")
        .setPort(9093)
        .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
        .setName(anotherListener.value)
    )

    val endpoints1 = new BrokerEndpointCollection()
    endpoints1.add(
      new BrokerEndpoint()
        .setHost("broker1")
        .setPort(9092)
        .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
        .setName(plaintextListener.value)
    )

    MetadataCacheTest.updateCache(metadataCache,
      Seq(new RegisterBrokerRecord().setBrokerId(0).setRack("rack").setFenced(false).setEndPoints(endpoints0),
      new RegisterBrokerRecord().setBrokerId(1).setRack("rack").setFenced(false).setEndPoints(endpoints1))
    )

    (plaintextListener, anotherListener)
  }

  private def sendMetadataRequestWithInconsistentListeners(requestListener: ListenerName): MetadataResponse = {
    val metadataRequest = MetadataRequest.Builder.allTopics.build()
    val requestChannelRequest = buildRequest(metadataRequest, requestListener)
    kafkaApis = createKafkaApis()
    kafkaApis.handleTopicMetadataRequest(requestChannelRequest)

    verifyNoThrottling[MetadataResponse](requestChannelRequest)
  }

  private def testConsumerListOffsetWithUnsupportedVersion(timestamp: Long, version: Short): Unit = {
    val tp = new TopicPartition("foo", 0)
    val targetTimes = List(new ListOffsetsTopic()
      .setName(tp.topic)
      .setPartitions(List(new ListOffsetsPartition()
        .setPartitionIndex(tp.partition)
        .setTimestamp(timestamp)).asJava)).asJava

    when(replicaManager.fetchOffset(
      ArgumentMatchers.any[Seq[ListOffsetsTopic]](),
      ArgumentMatchers.eq(Set.empty[TopicPartition]),
      ArgumentMatchers.eq(IsolationLevel.READ_UNCOMMITTED),
      ArgumentMatchers.eq(ListOffsetsRequest.CONSUMER_REPLICA_ID),
      ArgumentMatchers.eq[String](clientId),
      ArgumentMatchers.anyInt(), // correlationId
      ArgumentMatchers.anyShort(), // version
      ArgumentMatchers.any[(Errors, ListOffsetsPartition) => ListOffsetsPartitionResponse](),
      ArgumentMatchers.any[List[ListOffsetsTopicResponse] => Unit](),
      ArgumentMatchers.anyInt() // timeoutMs
    )).thenAnswer(ans => {
      val version = ans.getArgument[Short](6)
      val callback = ans.getArgument[List[ListOffsetsTopicResponse] => Unit](8)
      val errorCode = if (ReplicaManager.isListOffsetsTimestampUnsupported(timestamp, version))
        Errors.UNSUPPORTED_VERSION.code()
      else
        Errors.INVALID_REQUEST.code()
      val partitionResponse = new ListOffsetsPartitionResponse()
        .setErrorCode(errorCode)
        .setOffset(ListOffsetsResponse.UNKNOWN_OFFSET)
        .setTimestamp(ListOffsetsResponse.UNKNOWN_TIMESTAMP)
        .setPartitionIndex(tp.partition())
      callback(List(new ListOffsetsTopicResponse().setName(tp.topic()).setPartitions(List(partitionResponse).asJava)))
    })

    val data = new ListOffsetsRequestData().setTopics(targetTimes).setReplicaId(ListOffsetsRequest.CONSUMER_REPLICA_ID)
    val listOffsetRequest = ListOffsetsRequest.parse(MessageUtil.toByteBuffer(data, version), version)
    val request = buildRequest(listOffsetRequest)

    kafkaApis = createKafkaApis()
    kafkaApis.handleListOffsetRequest(request)

    val response = verifyNoThrottling[ListOffsetsResponse](request)
    val partitionDataOptional = response.topics.asScala.find(_.name == tp.topic).get
      .partitions.asScala.find(_.partitionIndex == tp.partition)
    assertTrue(partitionDataOptional.isDefined)

    val partitionData = partitionDataOptional.get
    assertEquals(Errors.UNSUPPORTED_VERSION.code, partitionData.errorCode)
  }

  private def testConsumerListOffsetLatest(isolationLevel: IsolationLevel): Unit = {
    val tp = new TopicPartition("foo", 0)
    val latestOffset = 15L

    val targetTimes = List(new ListOffsetsTopic()
      .setName(tp.topic)
      .setPartitions(List(new ListOffsetsPartition()
        .setPartitionIndex(tp.partition)
        .setTimestamp(ListOffsetsRequest.LATEST_TIMESTAMP)).asJava)).asJava

    when(replicaManager.fetchOffset(
      ArgumentMatchers.eq(targetTimes.asScala.toSeq),
      ArgumentMatchers.eq(Set.empty[TopicPartition]),
      ArgumentMatchers.eq(isolationLevel),
      ArgumentMatchers.eq(ListOffsetsRequest.CONSUMER_REPLICA_ID),
      ArgumentMatchers.eq[String](clientId),
      ArgumentMatchers.anyInt(), // correlationId
      ArgumentMatchers.anyShort(), // version
      ArgumentMatchers.any[(Errors, ListOffsetsPartition) => ListOffsetsPartitionResponse](),
      ArgumentMatchers.any[List[ListOffsetsTopicResponse] => Unit](),
      ArgumentMatchers.anyInt() // timeoutMs
    )).thenAnswer(ans => {
      val callback = ans.getArgument[List[ListOffsetsTopicResponse] => Unit](8)
      val partitionResponse = new ListOffsetsPartitionResponse()
        .setErrorCode(Errors.NONE.code())
        .setOffset(latestOffset)
        .setTimestamp(ListOffsetsResponse.UNKNOWN_TIMESTAMP)
        .setPartitionIndex(tp.partition())
      callback(List(new ListOffsetsTopicResponse().setName(tp.topic()).setPartitions(List(partitionResponse).asJava)))
    })

    val listOffsetRequest = ListOffsetsRequest.Builder.forConsumer(true, isolationLevel)
      .setTargetTimes(targetTimes).build()
    val request = buildRequest(listOffsetRequest)
    kafkaApis = createKafkaApis()
    kafkaApis.handleListOffsetRequest(request)

    val response = verifyNoThrottling[ListOffsetsResponse](request)
    val partitionDataOptional = response.topics.asScala.find(_.name == tp.topic).get
      .partitions.asScala.find(_.partitionIndex == tp.partition)
    assertTrue(partitionDataOptional.isDefined)

    val partitionData = partitionDataOptional.get
    assertEquals(Errors.NONE.code, partitionData.errorCode)
    assertEquals(latestOffset, partitionData.offset)
    assertEquals(ListOffsetsResponse.UNKNOWN_TIMESTAMP, partitionData.timestamp)
  }

  private def createWriteTxnMarkersRequest(partitions: util.List[TopicPartition]) = {
    val writeTxnMarkersRequest = new WriteTxnMarkersRequest.Builder(
      asList(new TxnMarkerEntry(1, 1.toShort, 0, TransactionResult.COMMIT, partitions))).build()
    (writeTxnMarkersRequest, buildRequest(writeTxnMarkersRequest))
  }

  private def buildRequest(request: AbstractRequest,
                           listenerName: ListenerName = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
                           fromPrivilegedListener: Boolean = false,
                           requestHeader: Option[RequestHeader] = None,
                           requestMetrics: RequestChannelMetrics = requestChannelMetrics): RequestChannel.Request = {
    val buffer = request.serializeWithHeader(
      requestHeader.getOrElse(new RequestHeader(request.apiKey, request.version, clientId, 0)))

    // read the header from the buffer first so that the body can be read next from the Request constructor
    val header = RequestHeader.parse(buffer)
    // DelegationTokens require the context authenticated to be non SecurityProtocol.PLAINTEXT
    // and have a non KafkaPrincipal.ANONYMOUS principal. This test is done before the check
    // for forwarding because after forwarding the context will have a different context.
    // We validate the context authenticated failure case in other integration tests.
    val context = new RequestContext(header, "1", InetAddress.getLocalHost, Optional.empty(),
      new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice"), listenerName, SecurityProtocol.SSL,
      ClientInformation.EMPTY, fromPrivilegedListener, Optional.of(kafkaPrincipalSerde))
    new RequestChannel.Request(processor = 1, context = context, startTimeNanos = 0, MemoryPool.NONE, buffer,
      requestMetrics, envelope = None)
  }

  private def verifyNoThrottling[T <: AbstractResponse](
    request: RequestChannel.Request
  ): T = {
    val capturedResponse: ArgumentCaptor[AbstractResponse] = ArgumentCaptor.forClass(classOf[AbstractResponse])
    verify(requestChannel).sendResponse(
      ArgumentMatchers.eq(request),
      capturedResponse.capture(),
      any()
    )
    val response = capturedResponse.getValue
    val buffer = MessageUtil.toByteBuffer(
      response.data,
      request.context.header.apiVersion
    )
    AbstractResponse.parseResponse(
      request.context.header.apiKey,
      buffer,
      request.context.header.apiVersion,
    ).asInstanceOf[T]
  }

  private def verifyNoThrottlingAndUpdateMetrics[T <: AbstractResponse](
    request: RequestChannel.Request
  ): T = {
    val capturedResponse: ArgumentCaptor[AbstractResponse] = ArgumentCaptor.forClass(classOf[AbstractResponse])
    verify(requestChannel).sendResponse(
      ArgumentMatchers.eq(request),
      capturedResponse.capture(),
      any()
    )
    val response = capturedResponse.getValue
    val buffer = MessageUtil.toByteBuffer(
      response.data,
      request.context.header.apiVersion
    )

    // Create the RequestChannel.Response that is created when sendResponse is called in order to update the metrics.
    val sendResponse = new RequestChannel.SendResponse(
      request,
      request.buildResponseSend(response),
      request.responseNode(response),
      None
    )
    request.updateRequestMetrics(time.milliseconds(), sendResponse)

    AbstractResponse.parseResponse(
      request.context.header.apiKey,
      buffer,
      request.context.header.apiVersion,
    ).asInstanceOf[T]
  }

  private def createBasicMetadata(topic: String,
                                  numPartitions: Int,
                                  brokerEpoch: Long,
                                  numBrokers: Int,
                                  topicId: Uuid): Seq[ApiMessage] = {

    val results = new mutable.ArrayBuffer[ApiMessage]()
    val topicRecord = new TopicRecord().setName(topic).setTopicId(topicId)
    results += topicRecord

    val replicas = List(0.asInstanceOf[Integer]).asJava

    def createPartitionRecord(partition: Int) = new PartitionRecord()
      .setTopicId(topicId)
      .setPartitionId(partition)
      .setLeader(0)
      .setLeaderEpoch(1)
      .setReplicas(replicas)
      .setIsr(replicas)

    val plaintextListener = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)
    val partitionRecords = (0 until numPartitions).map(createPartitionRecord)
    val liveBrokers = (0 until numBrokers).map(
      brokerId => createMetadataBroker(brokerId, plaintextListener, brokerEpoch))
    partitionRecords.foreach(record => results += record)
    liveBrokers.foreach(record => results +=record)

    results.toSeq
  }

  private def setupBasicMetadataCache(topic: String, numPartitions: Int, numBrokers: Int, topicId: Uuid): Unit = {
    val updateMetadata = createBasicMetadata(topic, numPartitions, 0, numBrokers, topicId)
    MetadataCacheTest.updateCache(metadataCache, updateMetadata)
  }

  private def addTopicToMetadataCache(topic: String, numPartitions: Int, numBrokers: Int = 1, topicId: Uuid = Uuid.ZERO_UUID): Unit = {
    val updateMetadata = createBasicMetadata(topic, numPartitions, 0, numBrokers, topicId)
    MetadataCacheTest.updateCache(metadataCache, updateMetadata)
  }

  private def createMetadataBroker(brokerId: Int,
                                   listener: ListenerName,
                                   brokerEpoch: Long): RegisterBrokerRecord = {
    val endpoints = new BrokerEndpointCollection()
    endpoints.add(
      new BrokerEndpoint()
        .setHost("broker" + brokerId)
        .setPort(9092)
        .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)
        .setName(listener.value)
    )

    new RegisterBrokerRecord()
      .setBrokerId(brokerId)
      .setRack("rack")
      .setFenced(false)
      .setEndPoints(endpoints)
      .setBrokerEpoch(brokerEpoch)
  }

  @Test
  def testAlterReplicaLogDirs(): Unit = {
    val data = new AlterReplicaLogDirsRequestData()
    val dir = new AlterReplicaLogDirsRequestData.AlterReplicaLogDir()
      .setPath("/foo")
    dir.topics().add(new AlterReplicaLogDirsRequestData.AlterReplicaLogDirTopic().setName("t0").setPartitions(asList(0, 1, 2)))
    data.dirs().add(dir)
    val alterReplicaLogDirsRequest = new AlterReplicaLogDirsRequest.Builder(
      data
    ).build()
    val request = buildRequest(alterReplicaLogDirsRequest)

    reset(replicaManager, clientRequestQuotaManager, requestChannel)

    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    val t0p0 = new TopicPartition("t0", 0)
    val t0p1 = new TopicPartition("t0", 1)
    val t0p2 = new TopicPartition("t0", 2)
    val partitionResults = Map(
      t0p0 -> Errors.NONE,
      t0p1 -> Errors.LOG_DIR_NOT_FOUND,
      t0p2 -> Errors.INVALID_TOPIC_EXCEPTION)
    when(replicaManager.alterReplicaLogDirs(ArgumentMatchers.eq(Map(
      t0p0 -> "/foo",
      t0p1 -> "/foo",
      t0p2 -> "/foo"))))
    .thenReturn(partitionResults)
    kafkaApis = createKafkaApis()
    kafkaApis.handleAlterReplicaLogDirsRequest(request)

    val response = verifyNoThrottling[AlterReplicaLogDirsResponse](request)
    assertEquals(partitionResults, response.data.results.asScala.flatMap { tr =>
      tr.partitions().asScala.map { pr =>
        new TopicPartition(tr.topicName, pr.partitionIndex) -> Errors.forCode(pr.errorCode)
      }
    }.toMap)
    assertEquals(Map(Errors.NONE -> 1,
      Errors.LOG_DIR_NOT_FOUND -> 1,
      Errors.INVALID_TOPIC_EXCEPTION -> 1).asJava, response.errorCounts)
  }

  @Test
  def testSizeOfThrottledPartitions(): Unit = {
    val topicNames = new util.HashMap[Uuid, String]
    val topicIds = new util.HashMap[String, Uuid]()
    def fetchResponse(data: Map[TopicIdPartition, String]): FetchResponse = {
      val responseData = new util.LinkedHashMap[TopicIdPartition, FetchResponseData.PartitionData](
        data.map { case (tp, raw) =>
          tp -> new FetchResponseData.PartitionData()
            .setPartitionIndex(tp.topicPartition.partition)
            .setHighWatermark(105)
            .setLastStableOffset(105)
            .setLogStartOffset(0)
            .setRecords(MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(100, raw.getBytes(StandardCharsets.UTF_8))))
      }.toMap.asJava)

      data.foreach{case (tp, _) =>
        topicIds.put(tp.topicPartition.topic, tp.topicId)
        topicNames.put(tp.topicId, tp.topicPartition.topic)
      }
      FetchResponse.of(Errors.NONE, 100, 100, responseData)
    }

    val throttledPartition = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("throttledData", 0))
    val throttledData = Map(throttledPartition -> "throttledData")
    val expectedSize = FetchResponse.sizeOf(FetchResponseData.HIGHEST_SUPPORTED_VERSION,
      fetchResponse(throttledData).responseData(topicNames, FetchResponseData.HIGHEST_SUPPORTED_VERSION).entrySet.asScala.map( entry =>
      (new TopicIdPartition(Uuid.ZERO_UUID, entry.getKey), entry.getValue)).toMap.asJava.entrySet.iterator)

    val response = fetchResponse(throttledData ++ Map(new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("nonThrottledData", 0)) -> "nonThrottledData"))

    val quota = Mockito.mock(classOf[ReplicationQuotaManager])
    Mockito.when(quota.isThrottled(ArgumentMatchers.any(classOf[TopicPartition])))
      .thenAnswer(invocation => throttledPartition.topicPartition == invocation.getArgument(0).asInstanceOf[TopicPartition])

    assertEquals(expectedSize, KafkaApis.sizeOfThrottledPartitions(FetchResponseData.HIGHEST_SUPPORTED_VERSION, response, quota))
  }

  @Test
  def testDescribeProducers(): Unit = {
    val tp1 = new TopicPartition("foo", 0)
    val tp2 = new TopicPartition("bar", 3)
    val tp3 = new TopicPartition("baz", 1)
    val tp4 = new TopicPartition("invalid;topic", 1)

    val authorizer: Authorizer = mock(classOf[Authorizer])
    val data = new DescribeProducersRequestData().setTopics(List(
      new DescribeProducersRequestData.TopicRequest()
        .setName(tp1.topic)
        .setPartitionIndexes(List(Int.box(tp1.partition)).asJava),
      new DescribeProducersRequestData.TopicRequest()
        .setName(tp2.topic)
        .setPartitionIndexes(List(Int.box(tp2.partition)).asJava),
      new DescribeProducersRequestData.TopicRequest()
        .setName(tp3.topic)
        .setPartitionIndexes(List(Int.box(tp3.partition)).asJava),
      new DescribeProducersRequestData.TopicRequest()
        .setName(tp4.topic)
        .setPartitionIndexes(List(Int.box(tp4.partition)).asJava)
    ).asJava)

    def buildExpectedActions(topic: String): util.List[Action] = {
      val pattern = new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL)
      val action = new Action(AclOperation.READ, pattern, 1, true, true)
      Collections.singletonList(action)
    }

    // Topic `foo` is authorized and present in the metadata
    addTopicToMetadataCache(tp1.topic, 4) // We will only access the first topic
    when(authorizer.authorize(any[RequestContext], ArgumentMatchers.eq(buildExpectedActions(tp1.topic))))
      .thenReturn(Seq(AuthorizationResult.ALLOWED).asJava)

    // Topic `bar` is not authorized
    when(authorizer.authorize(any[RequestContext], ArgumentMatchers.eq(buildExpectedActions(tp2.topic))))
      .thenReturn(Seq(AuthorizationResult.DENIED).asJava)

    // Topic `baz` is authorized, but not present in the metadata
    when(authorizer.authorize(any[RequestContext], ArgumentMatchers.eq(buildExpectedActions(tp3.topic))))
      .thenReturn(Seq(AuthorizationResult.ALLOWED).asJava)

    when(replicaManager.activeProducerState(tp1))
      .thenReturn(new DescribeProducersResponseData.PartitionResponse()
        .setErrorCode(Errors.NONE.code)
        .setPartitionIndex(tp1.partition)
        .setActiveProducers(List(
          new DescribeProducersResponseData.ProducerState()
            .setProducerId(12345L)
            .setProducerEpoch(15)
            .setLastSequence(100)
            .setLastTimestamp(time.milliseconds())
            .setCurrentTxnStartOffset(-1)
            .setCoordinatorEpoch(200)
        ).asJava))

    val describeProducersRequest = new DescribeProducersRequest.Builder(data).build()
    val request = buildRequest(describeProducersRequest)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleDescribeProducersRequest(request)

    val response = verifyNoThrottling[DescribeProducersResponse](request)
    assertEquals(Set("foo", "bar", "baz", "invalid;topic"), response.data.topics.asScala.map(_.name).toSet)

    def assertPartitionError(
      topicPartition: TopicPartition,
      error: Errors
    ): DescribeProducersResponseData.PartitionResponse = {
      val topicData = response.data.topics.asScala.find(_.name == topicPartition.topic).get
      val partitionData = topicData.partitions.asScala.find(_.partitionIndex == topicPartition.partition).get
      assertEquals(error, Errors.forCode(partitionData.errorCode))
      partitionData
    }

    val fooPartition = assertPartitionError(tp1, Errors.NONE)
    assertEquals(Errors.NONE, Errors.forCode(fooPartition.errorCode))
    assertEquals(1, fooPartition.activeProducers.size)
    val fooProducer = fooPartition.activeProducers.get(0)
    assertEquals(12345L, fooProducer.producerId)
    assertEquals(15, fooProducer.producerEpoch)
    assertEquals(100, fooProducer.lastSequence)
    assertEquals(time.milliseconds(), fooProducer.lastTimestamp)
    assertEquals(-1, fooProducer.currentTxnStartOffset)
    assertEquals(200, fooProducer.coordinatorEpoch)

    assertPartitionError(tp2, Errors.TOPIC_AUTHORIZATION_FAILED)
    assertPartitionError(tp3, Errors.UNKNOWN_TOPIC_OR_PARTITION)
    assertPartitionError(tp4, Errors.INVALID_TOPIC_EXCEPTION)
  }

  @Test
  def testDescribeTransactions(): Unit = {
    val authorizer: Authorizer = mock(classOf[Authorizer])
    val data = new DescribeTransactionsRequestData()
      .setTransactionalIds(List("foo", "bar").asJava)
    val describeTransactionsRequest = new DescribeTransactionsRequest.Builder(data).build()
    val request = buildRequest(describeTransactionsRequest)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    def buildExpectedActions(transactionalId: String): util.List[Action] = {
      val pattern = new ResourcePattern(ResourceType.TRANSACTIONAL_ID, transactionalId, PatternType.LITERAL)
      val action = new Action(AclOperation.DESCRIBE, pattern, 1, true, true)
      Collections.singletonList(action)
    }

    when(txnCoordinator.handleDescribeTransactions("foo"))
      .thenReturn(new DescribeTransactionsResponseData.TransactionState()
        .setErrorCode(Errors.NONE.code)
        .setTransactionalId("foo")
        .setProducerId(12345L)
        .setProducerEpoch(15)
        .setTransactionStartTimeMs(time.milliseconds())
        .setTransactionState("CompleteCommit")
        .setTransactionTimeoutMs(10000))

    when(authorizer.authorize(any[RequestContext], ArgumentMatchers.eq(buildExpectedActions("foo"))))
      .thenReturn(Seq(AuthorizationResult.ALLOWED).asJava)

    when(authorizer.authorize(any[RequestContext], ArgumentMatchers.eq(buildExpectedActions("bar"))))
      .thenReturn(Seq(AuthorizationResult.DENIED).asJava)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleDescribeTransactionsRequest(request)

    val response = verifyNoThrottling[DescribeTransactionsResponse](request)
    assertEquals(2, response.data.transactionStates.size)

    val fooState = response.data.transactionStates.asScala.find(_.transactionalId == "foo").get
    assertEquals(Errors.NONE.code, fooState.errorCode)
    assertEquals(12345L, fooState.producerId)
    assertEquals(15, fooState.producerEpoch)
    assertEquals(time.milliseconds(), fooState.transactionStartTimeMs)
    assertEquals("CompleteCommit", fooState.transactionState)
    assertEquals(10000, fooState.transactionTimeoutMs)
    assertEquals(List.empty, fooState.topics.asScala.toList)

    val barState = response.data.transactionStates.asScala.find(_.transactionalId == "bar").get
    assertEquals(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.code, barState.errorCode)
  }

  @Test
  def testDescribeTransactionsFiltersUnauthorizedTopics(): Unit = {
    val authorizer: Authorizer = mock(classOf[Authorizer])
    val transactionalId = "foo"
    val data = new DescribeTransactionsRequestData()
      .setTransactionalIds(List(transactionalId).asJava)
    val describeTransactionsRequest = new DescribeTransactionsRequest.Builder(data).build()
    val request = buildRequest(describeTransactionsRequest)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    def expectDescribe(
      resourceType: ResourceType,
      transactionalId: String,
      result: AuthorizationResult
    ): Unit = {
      val pattern = new ResourcePattern(resourceType, transactionalId, PatternType.LITERAL)
      val action = new Action(AclOperation.DESCRIBE, pattern, 1, true, true)
      val actions = Collections.singletonList(action)

      when(authorizer.authorize(any[RequestContext], ArgumentMatchers.eq(actions)))
        .thenReturn(Seq(result).asJava)
    }

    // Principal is authorized to one of the two topics. The second topic should be
    // filtered from the result.
    expectDescribe(ResourceType.TRANSACTIONAL_ID, transactionalId, AuthorizationResult.ALLOWED)
    expectDescribe(ResourceType.TOPIC, "foo", AuthorizationResult.ALLOWED)
    expectDescribe(ResourceType.TOPIC, "bar", AuthorizationResult.DENIED)

    def mkTopicData(
      topic: String,
      partitions: Seq[Int]
    ): DescribeTransactionsResponseData.TopicData = {
      new DescribeTransactionsResponseData.TopicData()
        .setTopic(topic)
        .setPartitions(partitions.map(Int.box).asJava)
    }

    val describeTransactionsResponse = new DescribeTransactionsResponseData.TransactionState()
      .setErrorCode(Errors.NONE.code)
      .setTransactionalId(transactionalId)
      .setProducerId(12345L)
      .setProducerEpoch(15)
      .setTransactionStartTimeMs(time.milliseconds())
      .setTransactionState("Ongoing")
      .setTransactionTimeoutMs(10000)

    describeTransactionsResponse.topics.add(mkTopicData(topic = "foo", Seq(1, 2)))
    describeTransactionsResponse.topics.add(mkTopicData(topic = "bar", Seq(3, 4)))

    when(txnCoordinator.handleDescribeTransactions("foo"))
      .thenReturn(describeTransactionsResponse)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleDescribeTransactionsRequest(request)

    val response = verifyNoThrottling[DescribeTransactionsResponse](request)
    assertEquals(1, response.data.transactionStates.size)

    val fooState = response.data.transactionStates.asScala.find(_.transactionalId == "foo").get
    assertEquals(Errors.NONE.code, fooState.errorCode)
    assertEquals(12345L, fooState.producerId)
    assertEquals(15, fooState.producerEpoch)
    assertEquals(time.milliseconds(), fooState.transactionStartTimeMs)
    assertEquals("Ongoing", fooState.transactionState)
    assertEquals(10000, fooState.transactionTimeoutMs)
    assertEquals(List(mkTopicData(topic = "foo", Seq(1, 2))), fooState.topics.asScala.toList)
  }

  @Test
  def testListTransactionsErrorResponse(): Unit = {
    val data = new ListTransactionsRequestData()
    val listTransactionsRequest = new ListTransactionsRequest.Builder(data).build()
    val request = buildRequest(listTransactionsRequest)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    when(txnCoordinator.handleListTransactions(Set.empty[Long], Set.empty[String], -1L))
      .thenReturn(new ListTransactionsResponseData()
        .setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code))
    kafkaApis = createKafkaApis()
    kafkaApis.handleListTransactionsRequest(request)

    val response = verifyNoThrottling[ListTransactionsResponse](request)
    assertEquals(0, response.data.transactionStates.size)
    assertEquals(Errors.COORDINATOR_LOAD_IN_PROGRESS, Errors.forCode(response.data.errorCode))
  }

  @Test
  def testListTransactionsAuthorization(): Unit = {
    val authorizer: Authorizer = mock(classOf[Authorizer])
    val data = new ListTransactionsRequestData()
    val listTransactionsRequest = new ListTransactionsRequest.Builder(data).build()
    val request = buildRequest(listTransactionsRequest)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)

    val transactionStates = new util.ArrayList[ListTransactionsResponseData.TransactionState]()
    transactionStates.add(new ListTransactionsResponseData.TransactionState()
      .setTransactionalId("foo")
      .setProducerId(12345L)
      .setTransactionState("Ongoing"))
    transactionStates.add(new ListTransactionsResponseData.TransactionState()
      .setTransactionalId("bar")
      .setProducerId(98765)
      .setTransactionState("PrepareAbort"))

    when(txnCoordinator.handleListTransactions(Set.empty[Long], Set.empty[String], -1L))
      .thenReturn(new ListTransactionsResponseData()
        .setErrorCode(Errors.NONE.code)
        .setTransactionStates(transactionStates))

    def buildExpectedActions(transactionalId: String): util.List[Action] = {
      val pattern = new ResourcePattern(ResourceType.TRANSACTIONAL_ID, transactionalId, PatternType.LITERAL)
      val action = new Action(AclOperation.DESCRIBE, pattern, 1, true, true)
      Collections.singletonList(action)
    }

    when(authorizer.authorize(any[RequestContext], ArgumentMatchers.eq(buildExpectedActions("foo"))))
      .thenReturn(Seq(AuthorizationResult.ALLOWED).asJava)

    when(authorizer.authorize(any[RequestContext], ArgumentMatchers.eq(buildExpectedActions("bar"))))
      .thenReturn(Seq(AuthorizationResult.DENIED).asJava)
    kafkaApis = createKafkaApis(authorizer = Some(authorizer))
    kafkaApis.handleListTransactionsRequest(request)

    val response = verifyNoThrottling[ListTransactionsResponse](request)
    assertEquals(1, response.data.transactionStates.size())
    val transactionState = response.data.transactionStates.get(0)
    assertEquals("foo", transactionState.transactionalId)
    assertEquals(12345L, transactionState.producerId)
    assertEquals("Ongoing", transactionState.transactionState)
  }

  @Test
  def testEmptyLegacyAlterConfigsRequestWithKRaft(): Unit = {
    val request = buildRequest(new AlterConfigsRequest(new AlterConfigsRequestData(), 1.toShort))
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    kafkaApis = createKafkaApis()
    kafkaApis.handleAlterConfigsRequest(request)
    val response = verifyNoThrottling[AlterConfigsResponse](request)
    assertEquals(new AlterConfigsResponseData(), response.data())
  }

  @Test
  def testInvalidLegacyAlterConfigsRequestWithKRaft(): Unit = {
    val request = buildRequest(new AlterConfigsRequest(new AlterConfigsRequestData().
      setValidateOnly(true).
      setResources(new LAlterConfigsResourceCollection(asList(
        new LAlterConfigsResource().
          setResourceName(brokerId.toString).
          setResourceType(BROKER.id()).
          setConfigs(new LAlterableConfigCollection(asList(new LAlterableConfig().
            setName("foo").
            setValue(null)).iterator()))).iterator())), 1.toShort))
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    kafkaApis = createKafkaApis()
    kafkaApis.handleAlterConfigsRequest(request)
    val response = verifyNoThrottling[AlterConfigsResponse](request)
    assertEquals(new AlterConfigsResponseData().setResponses(asList(
      new LAlterConfigsResourceResponse().
        setErrorCode(Errors.INVALID_REQUEST.code()).
        setErrorMessage("Null value not supported for : foo").
        setResourceName(brokerId.toString).
        setResourceType(BROKER.id()))),
      response.data())
  }

  @Test
  def testEmptyIncrementalAlterConfigsRequestWithKRaft(): Unit = {
    val request = buildRequest(new IncrementalAlterConfigsRequest(new IncrementalAlterConfigsRequestData(), 1.toShort))
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    kafkaApis = createKafkaApis()
    kafkaApis.handleIncrementalAlterConfigsRequest(request)
    val response = verifyNoThrottling[IncrementalAlterConfigsResponse](request)
    assertEquals(new IncrementalAlterConfigsResponseData(), response.data())
  }

  @Test
  def testLog4jIncrementalAlterConfigsRequestWithKRaft(): Unit = {
    val request = buildRequest(new IncrementalAlterConfigsRequest(new IncrementalAlterConfigsRequestData().
      setValidateOnly(true).
      setResources(new IAlterConfigsResourceCollection(asList(new IAlterConfigsResource().
        setResourceName(brokerId.toString).
        setResourceType(BROKER_LOGGER.id()).
        setConfigs(new IAlterableConfigCollection(asList(new IAlterableConfig().
          setName(LoggingController.ROOT_LOGGER).
          setValue("TRACE")).iterator()))).iterator())),
        1.toShort))
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    when(clientRequestQuotaManager.maybeRecordAndGetThrottleTimeMs(any[RequestChannel.Request](),
      any[Long])).thenReturn(0)
    kafkaApis = createKafkaApis()
    kafkaApis.handleIncrementalAlterConfigsRequest(request)
    val response = verifyNoThrottling[IncrementalAlterConfigsResponse](request)
    assertEquals(new IncrementalAlterConfigsResponseData().setResponses(asList(
      new IAlterConfigsResourceResponse().
        setErrorCode(0.toShort).
        setErrorMessage(null).
        setResourceName(brokerId.toString).
        setResourceType(BROKER_LOGGER.id()))),
      response.data())
  }

  @Test
  def testConsumerGroupHeartbeatReturnsUnsupportedVersion(): Unit = {
    val consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequestData().setGroupId("group")

    val requestChannelRequest = buildRequest(new ConsumerGroupHeartbeatRequest.Builder(consumerGroupHeartbeatRequest).build())
    metadataCache = {
      val cache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_1)
      val delta = new MetadataDelta(MetadataImage.EMPTY);
      delta.replay(new FeatureLevelRecord()
        .setName(MetadataVersion.FEATURE_NAME)
        .setFeatureLevel(MetadataVersion.MINIMUM_VERSION.featureLevel())
      )
      cache.setImage(delta.apply(MetadataProvenance.EMPTY))
      cache
    }
    kafkaApis = createKafkaApis()
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val expectedHeartbeatResponse = new ConsumerGroupHeartbeatResponseData()
      .setErrorCode(Errors.UNSUPPORTED_VERSION.code)
    val response = verifyNoThrottling[ConsumerGroupHeartbeatResponse](requestChannelRequest)
    assertEquals(expectedHeartbeatResponse, response.data)
  }

  @Test
  def testConsumerGroupHeartbeatRequest(): Unit = {
    metadataCache = mock(classOf[KRaftMetadataCache])

    val consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequestData().setGroupId("group")

    val requestChannelRequest = buildRequest(new ConsumerGroupHeartbeatRequest.Builder(consumerGroupHeartbeatRequest).build())

    val future = new CompletableFuture[ConsumerGroupHeartbeatResponseData]()
    when(groupCoordinator.consumerGroupHeartbeat(
      requestChannelRequest.context,
      consumerGroupHeartbeatRequest
    )).thenReturn(future)
    kafkaApis = createKafkaApis(
      featureVersions = Seq(GroupVersion.GV_1)
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val consumerGroupHeartbeatResponse = new ConsumerGroupHeartbeatResponseData()
      .setMemberId("member")

    future.complete(consumerGroupHeartbeatResponse)
    val response = verifyNoThrottling[ConsumerGroupHeartbeatResponse](requestChannelRequest)
    assertEquals(consumerGroupHeartbeatResponse, response.data)
  }

  @Test
  def testConsumerGroupHeartbeatRequestFutureFailed(): Unit = {
    metadataCache = mock(classOf[KRaftMetadataCache])

    val consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequestData().setGroupId("group")

    val requestChannelRequest = buildRequest(new ConsumerGroupHeartbeatRequest.Builder(consumerGroupHeartbeatRequest).build())

    val future = new CompletableFuture[ConsumerGroupHeartbeatResponseData]()
    when(groupCoordinator.consumerGroupHeartbeat(
      requestChannelRequest.context,
      consumerGroupHeartbeatRequest
    )).thenReturn(future)
    kafkaApis = createKafkaApis(
      featureVersions = Seq(GroupVersion.GV_1)
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    future.completeExceptionally(Errors.FENCED_MEMBER_EPOCH.exception)
    val response = verifyNoThrottling[ConsumerGroupHeartbeatResponse](requestChannelRequest)
    assertEquals(Errors.FENCED_MEMBER_EPOCH.code, response.data.errorCode)
  }

  @Test
  def testConsumerGroupHeartbeatRequestGroupAuthorizationFailed(): Unit = {
    metadataCache = mock(classOf[KRaftMetadataCache])

    val consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequestData().setGroupId("group")

    val requestChannelRequest = buildRequest(new ConsumerGroupHeartbeatRequest.Builder(consumerGroupHeartbeatRequest).build())

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenReturn(Seq(AuthorizationResult.DENIED).asJava)
    kafkaApis = createKafkaApis(
      authorizer = Some(authorizer),
      featureVersions = Seq(GroupVersion.GV_1)
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val response = verifyNoThrottling[ConsumerGroupHeartbeatResponse](requestChannelRequest)
    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code, response.data.errorCode)
  }

  @Test
  def testConsumerGroupHeartbeatRequestTopicAuthorizationFailed(): Unit = {
    metadataCache = mock(classOf[KRaftMetadataCache])
    val groupId = "group"
    val fooTopicName = "foo"
    val barTopicName = "bar"
    val zarTopicName = "zar"

    val consumerGroupHeartbeatRequest = new ConsumerGroupHeartbeatRequestData()
      .setGroupId(groupId)
      .setSubscribedTopicNames(List(fooTopicName, barTopicName, zarTopicName).asJava)

    val requestChannelRequest = buildRequest(new ConsumerGroupHeartbeatRequest.Builder(consumerGroupHeartbeatRequest).build())

    val authorizer: Authorizer = mock(classOf[Authorizer])
    val acls = Map(
      groupId -> AuthorizationResult.ALLOWED,
      fooTopicName -> AuthorizationResult.ALLOWED,
      barTopicName -> AuthorizationResult.DENIED,
    )
    when(authorizer.authorize(
      any[RequestContext],
      any[util.List[Action]]
    )).thenAnswer { invocation =>
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      actions.asScala.map { action =>
        acls.getOrElse(action.resourcePattern.name, AuthorizationResult.DENIED)
      }.asJava
    }

    kafkaApis = createKafkaApis(
      authorizer = Some(authorizer),
      featureVersions = Seq(GroupVersion.GV_1)
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val response = verifyNoThrottling[ConsumerGroupHeartbeatResponse](requestChannelRequest)
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code, response.data.errorCode)
  }

  @Test
  def testStreamsGroupHeartbeatReturnsUnsupportedVersion(): Unit = {
    val streamsGroupHeartbeatRequest = new StreamsGroupHeartbeatRequestData().setGroupId("group")

    val requestChannelRequest = buildRequest(new StreamsGroupHeartbeatRequest.Builder(streamsGroupHeartbeatRequest).build())
    metadataCache = {
      val cache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_1)
      val delta = new MetadataDelta(MetadataImage.EMPTY);
      delta.replay(new FeatureLevelRecord()
        .setName(MetadataVersion.FEATURE_NAME)
        .setFeatureLevel(MetadataVersion.MINIMUM_VERSION.featureLevel())
      )
      cache.setImage(delta.apply(MetadataProvenance.EMPTY))
      cache
    }
    kafkaApis = createKafkaApis()
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val expectedHeartbeatResponse = new StreamsGroupHeartbeatResponseData()
      .setErrorCode(Errors.UNSUPPORTED_VERSION.code)
    val response = verifyNoThrottling[StreamsGroupHeartbeatResponse](requestChannelRequest)
    assertEquals(expectedHeartbeatResponse, response.data)
  }

  @Test
  def testStreamsGroupHeartbeatRequest(): Unit = {
    metadataCache = mock(classOf[KRaftMetadataCache])

    val streamsGroupHeartbeatRequest = new StreamsGroupHeartbeatRequestData().setGroupId("group")

    val requestChannelRequest = buildRequest(new StreamsGroupHeartbeatRequest.Builder(streamsGroupHeartbeatRequest, true).build())

    val future = new CompletableFuture[StreamsGroupHeartbeatResult]()
    when(groupCoordinator.streamsGroupHeartbeat(
      requestChannelRequest.context,
      streamsGroupHeartbeatRequest
    )).thenReturn(future)
    kafkaApis = createKafkaApis(
      overrideProperties = Map(GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG -> "classic,streams")
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val streamsGroupHeartbeatResponse = new StreamsGroupHeartbeatResponseData()
      .setMemberId("member")

    future.complete(new StreamsGroupHeartbeatResult(streamsGroupHeartbeatResponse, Collections.emptyMap()))
    val response = verifyNoThrottling[StreamsGroupHeartbeatResponse](requestChannelRequest)
    assertEquals(streamsGroupHeartbeatResponse, response.data)
  }

  @Test
  def testStreamsGroupHeartbeatRequestFutureFailed(): Unit = {
    metadataCache = mock(classOf[KRaftMetadataCache])

    val streamsGroupHeartbeatRequest = new StreamsGroupHeartbeatRequestData().setGroupId("group")

    val requestChannelRequest = buildRequest(new StreamsGroupHeartbeatRequest.Builder(streamsGroupHeartbeatRequest, true).build())

    val future = new CompletableFuture[StreamsGroupHeartbeatResult]()
    when(groupCoordinator.streamsGroupHeartbeat(
      requestChannelRequest.context,
      streamsGroupHeartbeatRequest
    )).thenReturn(future)
    kafkaApis = createKafkaApis(
      overrideProperties = Map(GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG -> "classic,streams")
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    future.completeExceptionally(Errors.FENCED_MEMBER_EPOCH.exception)
    val response = verifyNoThrottling[StreamsGroupHeartbeatResponse](requestChannelRequest)
    assertEquals(Errors.FENCED_MEMBER_EPOCH.code, response.data.errorCode)
  }

  @Test
  def testStreamsGroupHeartbeatRequestAuthorizationFailed(): Unit = {
    metadataCache = mock(classOf[KRaftMetadataCache])

    val streamsGroupHeartbeatRequest = new StreamsGroupHeartbeatRequestData().setGroupId("group")

    val requestChannelRequest = buildRequest(new StreamsGroupHeartbeatRequest.Builder(streamsGroupHeartbeatRequest, true).build())

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenReturn(Seq(AuthorizationResult.DENIED).asJava)
    kafkaApis = createKafkaApis(
      authorizer = Some(authorizer),
      overrideProperties = Map(GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG -> "classic,streams")
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val response = verifyNoThrottling[StreamsGroupHeartbeatResponse](requestChannelRequest)
    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code, response.data.errorCode)
  }

  @Test
  def testStreamsGroupHeartbeatRequestProtocolDisabled(): Unit = {
    metadataCache = mock(classOf[KRaftMetadataCache])

    val streamsGroupHeartbeatRequest = new StreamsGroupHeartbeatRequestData().setGroupId("group")

    val requestChannelRequest = buildRequest(new StreamsGroupHeartbeatRequest.Builder(streamsGroupHeartbeatRequest, true).build())

    kafkaApis = createKafkaApis(
      overrideProperties = Map(GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG -> "classic,consumer")
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val response = verifyNoThrottling[StreamsGroupHeartbeatResponse](requestChannelRequest)
    assertEquals(Errors.UNSUPPORTED_VERSION.code, response.data.errorCode)
  }

  @Test
  def testStreamsGroupHeartbeatRequestInvalidTopicNames(): Unit = {
    metadataCache = mock(classOf[KRaftMetadataCache])

    val streamsGroupHeartbeatRequest = new StreamsGroupHeartbeatRequestData().setGroupId("group").setTopology(
      new StreamsGroupHeartbeatRequestData.Topology()
        .setEpoch(3)
        .setSubtopologies(
          Collections.singletonList(new StreamsGroupHeartbeatRequestData.Subtopology().setSubtopologyId("subtopology")
            .setSourceTopics(Collections.singletonList("a "))
            .setRepartitionSinkTopics(Collections.singletonList("b?"))
            .setRepartitionSourceTopics(Collections.singletonList(new StreamsGroupHeartbeatRequestData.TopicInfo().setName("c!")))
            .setStateChangelogTopics(Collections.singletonList(new StreamsGroupHeartbeatRequestData.TopicInfo().setName("d/")))
          )
        )
    )

    val requestChannelRequest = buildRequest(new StreamsGroupHeartbeatRequest.Builder(streamsGroupHeartbeatRequest, true).build())

    kafkaApis = createKafkaApis(
      overrideProperties = Map(GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG -> "classic,streams")
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val response = verifyNoThrottling[StreamsGroupHeartbeatResponse](requestChannelRequest)
    assertEquals(Errors.STREAMS_INVALID_TOPOLOGY.code, response.data.errorCode)
    assertEquals("Topic names a ,b?,c!,d/ are not valid topic names.", response.data.errorMessage())
  }

  @Test
  def testStreamsGroupHeartbeatRequestInternalTopicNames(): Unit = {
    metadataCache = mock(classOf[KRaftMetadataCache])

    val streamsGroupHeartbeatRequest = new StreamsGroupHeartbeatRequestData().setGroupId("group").setTopology(
      new StreamsGroupHeartbeatRequestData.Topology()
        .setEpoch(3)
        .setSubtopologies(
          Collections.singletonList(new StreamsGroupHeartbeatRequestData.Subtopology().setSubtopologyId("subtopology")
            .setSourceTopics(Collections.singletonList("__consumer_offsets"))
            .setRepartitionSinkTopics(Collections.singletonList("__transaction_state"))
            .setRepartitionSourceTopics(Collections.singletonList(new StreamsGroupHeartbeatRequestData.TopicInfo().setName("__share_group_state")))
          )
        )
    )

    val requestChannelRequest = buildRequest(new StreamsGroupHeartbeatRequest.Builder(streamsGroupHeartbeatRequest, true).build())

    kafkaApis = createKafkaApis(
      overrideProperties = Map(GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG -> "classic,streams")
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val response = verifyNoThrottling[StreamsGroupHeartbeatResponse](requestChannelRequest)
    assertEquals(Errors.STREAMS_INVALID_TOPOLOGY.code, response.data.errorCode)
    assertEquals("Use of Kafka internal topics __consumer_offsets,__transaction_state,__share_group_state in a Kafka Streams topology is prohibited.", response.data.errorMessage())
  }

  @Test
  def testStreamsGroupHeartbeatRequestWithInternalTopicsToCreate(): Unit = {
    metadataCache = mock(classOf[KRaftMetadataCache])

    val streamsGroupHeartbeatRequest = new StreamsGroupHeartbeatRequestData().setGroupId("group");

    val requestChannelRequest = buildRequest(new StreamsGroupHeartbeatRequest.Builder(streamsGroupHeartbeatRequest, true).build())

    val future = new CompletableFuture[StreamsGroupHeartbeatResult]()
    when(groupCoordinator.streamsGroupHeartbeat(
      requestChannelRequest.context,
      streamsGroupHeartbeatRequest
    )).thenReturn(future)

    kafkaApis = createKafkaApis(
      overrideProperties = Map(GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG -> "classic,streams")
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val missingTopics = Map("test" -> new CreatableTopic())
    val streamsGroupHeartbeatResponse = new StreamsGroupHeartbeatResponseData()
      .setMemberId("member")

    future.complete(new StreamsGroupHeartbeatResult(streamsGroupHeartbeatResponse, missingTopics.asJava))
    val response = verifyNoThrottling[StreamsGroupHeartbeatResponse](requestChannelRequest)
    assertEquals(streamsGroupHeartbeatResponse, response.data)
    verify(autoTopicCreationManager).createStreamsInternalTopics(missingTopics, requestChannelRequest.context)
  }

  @Test
  def testStreamsGroupHeartbeatRequestWithInternalTopicsToCreateMissingCreateACL(): Unit = {
    metadataCache = mock(classOf[KRaftMetadataCache])

    val streamsGroupHeartbeatRequest = new StreamsGroupHeartbeatRequestData().setGroupId("group");

    val requestChannelRequest = buildRequest(new StreamsGroupHeartbeatRequest.Builder(streamsGroupHeartbeatRequest, true).build())

    val future = new CompletableFuture[StreamsGroupHeartbeatResult]()
    when(groupCoordinator.streamsGroupHeartbeat(
      requestChannelRequest.context,
      streamsGroupHeartbeatRequest
    )).thenReturn(future)

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], isNotNull[util.List[Action]])).thenAnswer(invocation => {
      val actions = invocation.getArgument(1).asInstanceOf[util.List[Action]]
      actions.asScala.map { action =>
        if (action.resourcePattern.name.equals("test") && action.operation() == AclOperation.CREATE && action.resourcePattern().resourceType() == ResourceType.TOPIC) {
          AuthorizationResult.DENIED
        } else if (action.operation() == AclOperation.CREATE && action.resourcePattern().resourceType() == ResourceType.CLUSTER) {
          AuthorizationResult.DENIED
        } else {
          AuthorizationResult.ALLOWED
        }
      }.asJava
    })
    kafkaApis = createKafkaApis(
      authorizer = Some(authorizer),
      overrideProperties = Map(GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG -> "classic,streams")
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val missingTopics = Map("test" -> new CreatableTopic())
    val streamsGroupHeartbeatResponse = new StreamsGroupHeartbeatResponseData()
      .setMemberId("member")

    future.complete(new StreamsGroupHeartbeatResult(streamsGroupHeartbeatResponse, missingTopics.asJava))
    val response = verifyNoThrottling[StreamsGroupHeartbeatResponse](requestChannelRequest)
    assertEquals(Errors.NONE.code, response.data.errorCode())
    assertEquals(null, response.data.errorMessage())
    assertEquals(
      java.util.List.of(
        new StreamsGroupHeartbeatResponseData.Status()
          .setStatusCode(StreamsGroupHeartbeatResponse.Status.MISSING_INTERNAL_TOPICS.code())
          .setStatusDetail("Unauthorized to CREATE on topics test.")
      ),
      response.data.status()
    )
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testConsumerGroupDescribe(includeAuthorizedOperations: Boolean): Unit = {
    val fooTopicName = "foo"
    val barTopicName = "bar"
    metadataCache = mock(classOf[KRaftMetadataCache])

    val groupIds = List("group-id-0", "group-id-1", "group-id-2").asJava
    val consumerGroupDescribeRequestData = new ConsumerGroupDescribeRequestData()
      .setIncludeAuthorizedOperations(includeAuthorizedOperations)
    consumerGroupDescribeRequestData.groupIds.addAll(groupIds)
    val requestChannelRequest = buildRequest(new ConsumerGroupDescribeRequest.Builder(consumerGroupDescribeRequestData, true).build())

    val future = new CompletableFuture[util.List[ConsumerGroupDescribeResponseData.DescribedGroup]]()
    when(groupCoordinator.consumerGroupDescribe(
      any[RequestContext],
      any[util.List[String]]
    )).thenReturn(future)
    kafkaApis = createKafkaApis(
      featureVersions = Seq(GroupVersion.GV_1)
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val member0 = new ConsumerGroupDescribeResponseData.Member()
      .setMemberId("member0")
      .setAssignment(new ConsumerGroupDescribeResponseData.Assignment()
        .setTopicPartitions(List(
          new TopicPartitions().setTopicName(fooTopicName)).asJava))
      .setTargetAssignment(new ConsumerGroupDescribeResponseData.Assignment()
        .setTopicPartitions(List(
          new TopicPartitions().setTopicName(fooTopicName)).asJava))

    val member1 = new ConsumerGroupDescribeResponseData.Member()
      .setMemberId("member1")
      .setAssignment(new ConsumerGroupDescribeResponseData.Assignment()
        .setTopicPartitions(List(
          new TopicPartitions().setTopicName(fooTopicName)).asJava))
      .setTargetAssignment(new ConsumerGroupDescribeResponseData.Assignment()
        .setTopicPartitions(List(
          new TopicPartitions().setTopicName(fooTopicName),
          new TopicPartitions().setTopicName(barTopicName)).asJava))

    val member2 = new ConsumerGroupDescribeResponseData.Member()
      .setMemberId("member2")
      .setAssignment(new ConsumerGroupDescribeResponseData.Assignment()
        .setTopicPartitions(List(
          new TopicPartitions().setTopicName(barTopicName)).asJava))
      .setTargetAssignment(new ConsumerGroupDescribeResponseData.Assignment()
        .setTopicPartitions(List(
          new TopicPartitions().setTopicName(fooTopicName)).asJava))

    future.complete(List(
      new DescribedGroup()
        .setGroupId(groupIds.get(0))
        .setMembers(List(member0).asJava),
      new DescribedGroup()
        .setGroupId(groupIds.get(1))
        .setMembers(List(member0, member1).asJava),
      new DescribedGroup()
        .setGroupId(groupIds.get(2))
        .setMembers(List(member2).asJava)
    ).asJava)

    var authorizedOperationsInt = Int.MinValue
    if (includeAuthorizedOperations) {
      authorizedOperationsInt = Utils.to32BitField(
        AclEntry.supportedOperations(ResourceType.GROUP).asScala
          .map(_.code.asInstanceOf[JByte]).asJava)
    }

    // Can't reuse the above list here because we would not test the implementation in KafkaApis then
    val describedGroups = List(
      new DescribedGroup()
        .setGroupId(groupIds.get(0))
        .setMembers(List(member0).asJava),
      new DescribedGroup()
        .setGroupId(groupIds.get(1))
        .setMembers(List(member0, member1).asJava),
      new DescribedGroup()
        .setGroupId(groupIds.get(2))
        .setMembers(List(member2).asJava)
    ).map(group => group.setAuthorizedOperations(authorizedOperationsInt))
    val expectedConsumerGroupDescribeResponseData = new ConsumerGroupDescribeResponseData()
      .setGroups(describedGroups.asJava)

    val response = verifyNoThrottling[ConsumerGroupDescribeResponse](requestChannelRequest)

    assertEquals(expectedConsumerGroupDescribeResponseData, response.data)
  }

  @Test
  def testConsumerGroupDescribeReturnsUnsupportedVersion(): Unit = {
    val groupId = "group0"
    val consumerGroupDescribeRequestData = new ConsumerGroupDescribeRequestData()
    consumerGroupDescribeRequestData.groupIds.add(groupId)
    val requestChannelRequest = buildRequest(new ConsumerGroupDescribeRequest.Builder(consumerGroupDescribeRequestData, true).build())

    val errorCode = Errors.UNSUPPORTED_VERSION.code
    val expectedDescribedGroup = new DescribedGroup().setGroupId(groupId).setErrorCode(errorCode)
    val expectedResponse = new ConsumerGroupDescribeResponseData()
    expectedResponse.groups.add(expectedDescribedGroup)
    metadataCache = {
      val cache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_1)
      val delta = new MetadataDelta(MetadataImage.EMPTY);
      delta.replay(new FeatureLevelRecord()
        .setName(MetadataVersion.FEATURE_NAME)
        .setFeatureLevel(MetadataVersion.MINIMUM_VERSION.featureLevel())
      )
      cache.setImage(delta.apply(MetadataProvenance.EMPTY))
      cache
    }
    kafkaApis = createKafkaApis()
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)
    val response = verifyNoThrottling[ConsumerGroupDescribeResponse](requestChannelRequest)

    assertEquals(expectedResponse, response.data)
  }

  @Test
  def testConsumerGroupDescribeAuthorizationFailed(): Unit = {
    metadataCache = mock(classOf[KRaftMetadataCache])

    val consumerGroupDescribeRequestData = new ConsumerGroupDescribeRequestData()
    consumerGroupDescribeRequestData.groupIds.add("group-id")
    val requestChannelRequest = buildRequest(new ConsumerGroupDescribeRequest.Builder(consumerGroupDescribeRequestData, true).build())

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenReturn(Seq(AuthorizationResult.DENIED).asJava)

    val future = new CompletableFuture[util.List[ConsumerGroupDescribeResponseData.DescribedGroup]]()
    when(groupCoordinator.consumerGroupDescribe(
      any[RequestContext],
      any[util.List[String]]
    )).thenReturn(future)
    future.complete(List().asJava)
    kafkaApis = createKafkaApis(
      authorizer = Some(authorizer),
      featureVersions = Seq(GroupVersion.GV_1)
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val response = verifyNoThrottling[ConsumerGroupDescribeResponse](requestChannelRequest)
    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code, response.data.groups.get(0).errorCode)
  }

  @Test
  def testConsumerGroupDescribeFutureFailed(): Unit = {
    metadataCache = mock(classOf[KRaftMetadataCache])

    val consumerGroupDescribeRequestData = new ConsumerGroupDescribeRequestData()
    consumerGroupDescribeRequestData.groupIds.add("group-id")
    val requestChannelRequest = buildRequest(new ConsumerGroupDescribeRequest.Builder(consumerGroupDescribeRequestData, true).build())

    val future = new CompletableFuture[util.List[ConsumerGroupDescribeResponseData.DescribedGroup]]()
    when(groupCoordinator.consumerGroupDescribe(
      any[RequestContext],
      any[util.List[String]]
    )).thenReturn(future)
    kafkaApis = createKafkaApis(
      featureVersions = Seq(GroupVersion.GV_1)
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    future.completeExceptionally(Errors.FENCED_MEMBER_EPOCH.exception)
    val response = verifyNoThrottling[ConsumerGroupDescribeResponse](requestChannelRequest)
    assertEquals(Errors.FENCED_MEMBER_EPOCH.code, response.data.groups.get(0).errorCode)
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testStreamsGroupDescribe(includeAuthorizedOperations: Boolean): Unit = {
    metadataCache = mock(classOf[KRaftMetadataCache])

    val groupIds = List("group-id-0", "group-id-1", "group-id-2").asJava
    val streamsGroupDescribeRequestData = new StreamsGroupDescribeRequestData()
      .setIncludeAuthorizedOperations(includeAuthorizedOperations)
    streamsGroupDescribeRequestData.groupIds.addAll(groupIds)
    val requestChannelRequest = buildRequest(new StreamsGroupDescribeRequest.Builder(streamsGroupDescribeRequestData, true).build())

    val future = new CompletableFuture[util.List[StreamsGroupDescribeResponseData.DescribedGroup]]()
    when(groupCoordinator.streamsGroupDescribe(
      any[RequestContext],
      any[util.List[String]]
    )).thenReturn(future)
    kafkaApis = createKafkaApis(
      overrideProperties = Map(GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG -> "classic,streams")
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    future.complete(List(
      new StreamsGroupDescribeResponseData.DescribedGroup().setGroupId(groupIds.get(0)),
      new StreamsGroupDescribeResponseData.DescribedGroup().setGroupId(groupIds.get(1)),
      new StreamsGroupDescribeResponseData.DescribedGroup().setGroupId(groupIds.get(2))
    ).asJava)

    var authorizedOperationsInt = Int.MinValue;
    if (includeAuthorizedOperations) {
      authorizedOperationsInt = Utils.to32BitField(
        AclEntry.supportedOperations(ResourceType.GROUP).asScala
          .map(_.code.asInstanceOf[JByte]).asJava)
    }

    // Can't reuse the above list here because we would not test the implementation in KafkaApis then
    val describedGroups = List(
      new StreamsGroupDescribeResponseData.DescribedGroup().setGroupId(groupIds.get(0)),
      new StreamsGroupDescribeResponseData.DescribedGroup().setGroupId(groupIds.get(1)),
      new StreamsGroupDescribeResponseData.DescribedGroup().setGroupId(groupIds.get(2))
    ).map(group => group.setAuthorizedOperations(authorizedOperationsInt))
    val expectedStreamsGroupDescribeResponseData = new StreamsGroupDescribeResponseData()
      .setGroups(describedGroups.asJava)

    val response = verifyNoThrottling[StreamsGroupDescribeResponse](requestChannelRequest)

    assertEquals(expectedStreamsGroupDescribeResponseData, response.data)
  }

  @Test
  def testStreamsGroupDescribeReturnsUnsupportedVersion(): Unit = {
    val groupId = "group0"
    val streamsGroupDescribeRequestData = new StreamsGroupDescribeRequestData()
    streamsGroupDescribeRequestData.groupIds.add(groupId)
    val requestChannelRequest = buildRequest(new StreamsGroupDescribeRequest.Builder(streamsGroupDescribeRequestData, true).build())

    val errorCode = Errors.UNSUPPORTED_VERSION.code
    val expectedDescribedGroup = new StreamsGroupDescribeResponseData.DescribedGroup().setGroupId(groupId).setErrorCode(errorCode)
    val expectedResponse = new StreamsGroupDescribeResponseData()
    expectedResponse.groups.add(expectedDescribedGroup)
    metadataCache = {
      val cache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_1)
      val delta = new MetadataDelta(MetadataImage.EMPTY);
      delta.replay(new FeatureLevelRecord()
        .setName(MetadataVersion.FEATURE_NAME)
        .setFeatureLevel(MetadataVersion.MINIMUM_VERSION.featureLevel())
      )
      cache.setImage(delta.apply(MetadataProvenance.EMPTY))
      cache
    }
    kafkaApis = createKafkaApis()
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)
    val response = verifyNoThrottling[StreamsGroupDescribeResponse](requestChannelRequest)

    assertEquals(expectedResponse, response.data)
  }

  @Test
  def testStreamsGroupDescribeAuthorizationFailed(): Unit = {
    metadataCache = mock(classOf[KRaftMetadataCache])

    val streamsGroupDescribeRequestData = new StreamsGroupDescribeRequestData()
    streamsGroupDescribeRequestData.groupIds.add("group-id")
    val requestChannelRequest = buildRequest(new StreamsGroupDescribeRequest.Builder(streamsGroupDescribeRequestData, true).build())

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenReturn(Seq(AuthorizationResult.DENIED).asJava)

    val future = new CompletableFuture[util.List[StreamsGroupDescribeResponseData.DescribedGroup]]()
    when(groupCoordinator.streamsGroupDescribe(
      any[RequestContext],
      any[util.List[String]]
    )).thenReturn(future)
    future.complete(List().asJava)
    kafkaApis = createKafkaApis(
      authorizer = Some(authorizer),
      overrideProperties = Map(GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG -> "classic,streams")
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val response = verifyNoThrottling[StreamsGroupDescribeResponse](requestChannelRequest)
    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code, response.data.groups.get(0).errorCode)
  }

  @Test
  def testStreamsGroupDescribeFutureFailed(): Unit = {
    metadataCache = mock(classOf[KRaftMetadataCache])

    val streamsGroupDescribeRequestData = new StreamsGroupDescribeRequestData()
    streamsGroupDescribeRequestData.groupIds.add("group-id")
    val requestChannelRequest = buildRequest(new StreamsGroupDescribeRequest.Builder(streamsGroupDescribeRequestData, true).build())

    val future = new CompletableFuture[util.List[StreamsGroupDescribeResponseData.DescribedGroup]]()
    when(groupCoordinator.streamsGroupDescribe(
      any[RequestContext],
      any[util.List[String]]
    )).thenReturn(future)
    kafkaApis = createKafkaApis(
      overrideProperties = Map(GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG -> "classic,streams")
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    future.completeExceptionally(Errors.FENCED_MEMBER_EPOCH.exception)
    val response = verifyNoThrottling[StreamsGroupDescribeResponse](requestChannelRequest)
    assertEquals(Errors.FENCED_MEMBER_EPOCH.code, response.data.groups.get(0).errorCode)
  }

  @Test
  def testConsumerGroupDescribeFilterUnauthorizedTopics(): Unit = {
    val fooTopicName = "foo"
    val barTopicName = "bar"
    val errorMessage = "The group has described topic(s) that the client is not authorized to describe."

    metadataCache = mock(classOf[KRaftMetadataCache])

    val groupIds = List("group-id-0", "group-id-1", "group-id-2").asJava
    val consumerGroupDescribeRequestData = new ConsumerGroupDescribeRequestData()
      .setGroupIds(groupIds)
    val requestChannelRequest = buildRequest(new ConsumerGroupDescribeRequest.Builder(consumerGroupDescribeRequestData, true).build())

    val authorizer: Authorizer = mock(classOf[Authorizer])
    val acls = Map(
      groupIds.get(0) -> AuthorizationResult.ALLOWED,
      groupIds.get(1) -> AuthorizationResult.ALLOWED,
      groupIds.get(2) -> AuthorizationResult.ALLOWED,
      fooTopicName    -> AuthorizationResult.ALLOWED,
      barTopicName    -> AuthorizationResult.DENIED,
    )
    when(authorizer.authorize(
      any[RequestContext],
      any[util.List[Action]]
    )).thenAnswer { invocation =>
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      actions.asScala.map { action =>
        acls.getOrElse(action.resourcePattern.name, AuthorizationResult.DENIED)
      }.asJava
    }

    val future = new CompletableFuture[util.List[ConsumerGroupDescribeResponseData.DescribedGroup]]()
    when(groupCoordinator.consumerGroupDescribe(
      any[RequestContext],
      any[util.List[String]]
    )).thenReturn(future)
    kafkaApis = createKafkaApis(
      authorizer = Some(authorizer),
      featureVersions = Seq(GroupVersion.GV_1)
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val member0 = new ConsumerGroupDescribeResponseData.Member()
      .setMemberId("member0")
      .setAssignment(new ConsumerGroupDescribeResponseData.Assignment()
        .setTopicPartitions(List(
          new TopicPartitions().setTopicName(fooTopicName)).asJava))
      .setTargetAssignment(new ConsumerGroupDescribeResponseData.Assignment()
        .setTopicPartitions(List(
          new TopicPartitions().setTopicName(fooTopicName)).asJava))

    val member1 = new ConsumerGroupDescribeResponseData.Member()
      .setMemberId("member1")
      .setAssignment(new ConsumerGroupDescribeResponseData.Assignment()
        .setTopicPartitions(List(
          new TopicPartitions().setTopicName(fooTopicName)).asJava))
      .setTargetAssignment(new ConsumerGroupDescribeResponseData.Assignment()
        .setTopicPartitions(List(
          new TopicPartitions().setTopicName(fooTopicName),
          new TopicPartitions().setTopicName(barTopicName)).asJava))

    val member2 = new ConsumerGroupDescribeResponseData.Member()
      .setMemberId("member2")
      .setAssignment(new ConsumerGroupDescribeResponseData.Assignment()
        .setTopicPartitions(List(
          new TopicPartitions().setTopicName(barTopicName)).asJava))
      .setTargetAssignment(new ConsumerGroupDescribeResponseData.Assignment()
        .setTopicPartitions(List(
          new TopicPartitions().setTopicName(fooTopicName)).asJava))

    future.complete(List(
      new DescribedGroup()
        .setGroupId(groupIds.get(0))
        .setMembers(List(member0).asJava),
      new DescribedGroup()
        .setGroupId(groupIds.get(1))
        .setMembers(List(member0, member1).asJava),
      new DescribedGroup()
        .setGroupId(groupIds.get(2))
        .setMembers(List(member2).asJava)
    ).asJava)

    val expectedConsumerGroupDescribeResponseData = new ConsumerGroupDescribeResponseData()
      .setGroups(List(
        new DescribedGroup()
          .setGroupId(groupIds.get(0))
          .setMembers(List(member0).asJava),
        new DescribedGroup()
          .setGroupId(groupIds.get(1))
          .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code)
          .setErrorMessage(errorMessage),
        new DescribedGroup()
          .setGroupId(groupIds.get(2))
          .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code)
          .setErrorMessage(errorMessage)
      ).asJava)

    val response = verifyNoThrottling[ConsumerGroupDescribeResponse](requestChannelRequest)

    assertEquals(expectedConsumerGroupDescribeResponseData, response.data)
  }

  @Test
  def testGetTelemetrySubscriptions(): Unit = {
    val request = buildRequest(new GetTelemetrySubscriptionsRequest.Builder(
      new GetTelemetrySubscriptionsRequestData(), true).build())

    when(clientMetricsManager.isTelemetryReceiverConfigured).thenReturn(true)
    when(clientMetricsManager.processGetTelemetrySubscriptionRequest(any[GetTelemetrySubscriptionsRequest](),
      any[RequestContext]())).thenReturn(new GetTelemetrySubscriptionsResponse(
      new GetTelemetrySubscriptionsResponseData()))

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    kafkaApis = createKafkaApis()
    kafkaApis.handle(request, RequestLocal.noCaching)

    val response = verifyNoThrottling[GetTelemetrySubscriptionsResponse](request)

    val expectedResponse = new GetTelemetrySubscriptionsResponseData()
    assertEquals(expectedResponse, response.data)
  }

  @Test
  def testGetTelemetrySubscriptionsWithException(): Unit = {
    val request = buildRequest(new GetTelemetrySubscriptionsRequest.Builder(
      new GetTelemetrySubscriptionsRequestData(), true).build())

    when(clientMetricsManager.isTelemetryReceiverConfigured).thenReturn(true)
    when(clientMetricsManager.processGetTelemetrySubscriptionRequest(any[GetTelemetrySubscriptionsRequest](),
      any[RequestContext]())).thenThrow(new RuntimeException("test"))

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    kafkaApis = createKafkaApis()
    kafkaApis.handle(request, RequestLocal.noCaching)

    val response = verifyNoThrottling[GetTelemetrySubscriptionsResponse](request)

    val expectedResponse = new GetTelemetrySubscriptionsResponseData().setErrorCode(Errors.INVALID_REQUEST.code)
    assertEquals(expectedResponse, response.data)
  }

  @Test
  def testPushTelemetry(): Unit = {
    val request = buildRequest(new PushTelemetryRequest.Builder(new PushTelemetryRequestData(), true).build())

    when(clientMetricsManager.isTelemetryReceiverConfigured).thenReturn(true)
    when(clientMetricsManager.processPushTelemetryRequest(any[PushTelemetryRequest](), any[RequestContext]()))
      .thenReturn(new PushTelemetryResponse(new PushTelemetryResponseData()))

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    kafkaApis = createKafkaApis()
    kafkaApis.handle(request, RequestLocal.noCaching)
    val response = verifyNoThrottling[PushTelemetryResponse](request)

    val expectedResponse = new PushTelemetryResponseData().setErrorCode(Errors.NONE.code)
    assertEquals(expectedResponse, response.data)
  }

  @Test
  def testPushTelemetryWithException(): Unit = {
    val request = buildRequest(new PushTelemetryRequest.Builder(new PushTelemetryRequestData(), true).build())

    when(clientMetricsManager.isTelemetryReceiverConfigured).thenReturn(true)
    when(clientMetricsManager.processPushTelemetryRequest(any[PushTelemetryRequest](), any[RequestContext]()))
      .thenThrow(new RuntimeException("test"))

    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    kafkaApis = createKafkaApis()
    kafkaApis.handle(request, RequestLocal.noCaching)
    val response = verifyNoThrottling[PushTelemetryResponse](request)

    val expectedResponse = new PushTelemetryResponseData().setErrorCode(Errors.INVALID_REQUEST.code)
    assertEquals(expectedResponse, response.data)
  }

  @Test
  def testListClientMetricsResources(): Unit = {
    val request = buildRequest(new ListClientMetricsResourcesRequest.Builder(new ListClientMetricsResourcesRequestData()).build())
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)

    val resources = new mutable.HashSet[String]
    resources.add("test1")
    resources.add("test2")
    when(clientMetricsManager.listClientMetricsResources).thenReturn(resources.asJava)
    kafkaApis = createKafkaApis()
    kafkaApis.handle(request, RequestLocal.noCaching)
    val response = verifyNoThrottling[ListClientMetricsResourcesResponse](request)
    val expectedResponse = new ListClientMetricsResourcesResponseData().setClientMetricsResources(
      resources.map(resource => new ClientMetricsResource().setName(resource)).toBuffer.asJava)
    assertEquals(expectedResponse, response.data)
  }

  @Test
  def testListClientMetricsResourcesEmptyResponse(): Unit = {
    val request = buildRequest(new ListClientMetricsResourcesRequest.Builder(new ListClientMetricsResourcesRequestData()).build())
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)

    val resources = new mutable.HashSet[String]
    when(clientMetricsManager.listClientMetricsResources).thenReturn(resources.asJava)
    kafkaApis = createKafkaApis()
    kafkaApis.handle(request, RequestLocal.noCaching)
    val response = verifyNoThrottling[ListClientMetricsResourcesResponse](request)
    val expectedResponse = new ListClientMetricsResourcesResponseData()
    assertEquals(expectedResponse, response.data)
  }

  @Test
  def testListClientMetricsResourcesWithException(): Unit = {
    val request = buildRequest(new ListClientMetricsResourcesRequest.Builder(new ListClientMetricsResourcesRequestData()).build())
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)

    when(clientMetricsManager.listClientMetricsResources).thenThrow(new RuntimeException("test"))
    kafkaApis = createKafkaApis()
    kafkaApis.handle(request, RequestLocal.noCaching)
    val response = verifyNoThrottling[ListClientMetricsResourcesResponse](request)

    val expectedResponse = new ListClientMetricsResourcesResponseData().setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code)
    assertEquals(expectedResponse, response.data)
  }

  @Test
  def testShareGroupHeartbeatReturnsUnsupportedVersion(): Unit = {
    val shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequestData().setGroupId("group")

    val requestChannelRequest = buildRequest(new ShareGroupHeartbeatRequest.Builder(shareGroupHeartbeatRequest, true).build())
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    kafkaApis = createKafkaApis()
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val expectedHeartbeatResponse = new ShareGroupHeartbeatResponseData()
      .setErrorCode(Errors.UNSUPPORTED_VERSION.code)
    val response = verifyNoThrottling[ShareGroupHeartbeatResponse](requestChannelRequest)
    assertEquals(expectedHeartbeatResponse, response.data)
  }

  @Test
  def testShareGroupHeartbeatRequest(): Unit = {
    val shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequestData().setGroupId("group")

    val requestChannelRequest = buildRequest(new ShareGroupHeartbeatRequest.Builder(shareGroupHeartbeatRequest, true).build())

    val future = new CompletableFuture[ShareGroupHeartbeatResponseData]()
    when(groupCoordinator.shareGroupHeartbeat(
      requestChannelRequest.context,
      shareGroupHeartbeatRequest
    )).thenReturn(future)
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    kafkaApis = createKafkaApis(
      overrideProperties = Map(ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val shareGroupHeartbeatResponse = new ShareGroupHeartbeatResponseData()
      .setMemberId("member")

    future.complete(shareGroupHeartbeatResponse)
    val response = verifyNoThrottling[ShareGroupHeartbeatResponse](requestChannelRequest)
    assertEquals(shareGroupHeartbeatResponse, response.data)
  }

  @Test
  def testShareGroupHeartbeatRequestGroupAuthorizationFailed(): Unit = {
    val shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequestData().setGroupId("group")

    val requestChannelRequest = buildRequest(new ShareGroupHeartbeatRequest.Builder(shareGroupHeartbeatRequest, true).build())

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenReturn(Seq(AuthorizationResult.DENIED).asJava)
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    kafkaApis = createKafkaApis(
      overrideProperties = Map(ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      authorizer = Some(authorizer),
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val response = verifyNoThrottling[ShareGroupHeartbeatResponse](requestChannelRequest)
    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code, response.data.errorCode)
  }

  @Test
  def testShareGroupHeartbeatRequestTopicAuthorizationFailed(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    val groupId = "group"
    val fooTopicName = "foo"
    val barTopicName = "bar"
    val zarTopicName = "zar"

    val shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequestData()
      .setGroupId(groupId)
      .setSubscribedTopicNames(List(fooTopicName, barTopicName, zarTopicName).asJava)

    val requestChannelRequest = buildRequest(new ShareGroupHeartbeatRequest.Builder(shareGroupHeartbeatRequest).build())

    val authorizer: Authorizer = mock(classOf[Authorizer])
    val acls = Map(
      groupId -> AuthorizationResult.ALLOWED,
      fooTopicName -> AuthorizationResult.ALLOWED,
      barTopicName -> AuthorizationResult.DENIED,
    )
    when(authorizer.authorize(
      any[RequestContext],
      any[util.List[Action]]
    )).thenAnswer { invocation =>
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      actions.asScala.map { action =>
        acls.getOrElse(action.resourcePattern.name, AuthorizationResult.DENIED)
      }.asJava
    }

    kafkaApis = createKafkaApis(
      overrideProperties = Map(ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      authorizer = Some(authorizer),
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val response = verifyNoThrottling[ShareGroupHeartbeatResponse](requestChannelRequest)
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code, response.data.errorCode)
  }

  @Test
  def testShareGroupHeartbeatRequestFutureFailed(): Unit = {
    val shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequestData().setGroupId("group")

    val requestChannelRequest = buildRequest(new ShareGroupHeartbeatRequest.Builder(shareGroupHeartbeatRequest, true).build())

    val future = new CompletableFuture[ShareGroupHeartbeatResponseData]()
    when(groupCoordinator.shareGroupHeartbeat(
      requestChannelRequest.context,
      shareGroupHeartbeatRequest
    )).thenReturn(future)
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    kafkaApis = createKafkaApis(
      overrideProperties = Map(ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    future.completeExceptionally(Errors.FENCED_MEMBER_EPOCH.exception)
    val response = verifyNoThrottling[ShareGroupHeartbeatResponse](requestChannelRequest)
    assertEquals(Errors.FENCED_MEMBER_EPOCH.code, response.data.errorCode)
  }

  @Test
  def testShareGroupDescribeSuccess(): Unit = {
    val fooTopicName = "foo"
    val barTopicName = "bar"

    val groupIds = List("share-group-id-0", "share-group-id-1", "share-group_id-2").asJava

    val member0 = new ShareGroupDescribeResponseData.Member()
      .setMemberId("member0")
      .setAssignment(new ShareGroupDescribeResponseData.Assignment()
        .setTopicPartitions(List(
          new ShareGroupDescribeResponseData.TopicPartitions().setTopicName(fooTopicName)).asJava))

    val member1 = new ShareGroupDescribeResponseData.Member()
      .setMemberId("member1")
      .setAssignment(new ShareGroupDescribeResponseData.Assignment()
        .setTopicPartitions(List(
          new ShareGroupDescribeResponseData.TopicPartitions().setTopicName(fooTopicName),
          new ShareGroupDescribeResponseData.TopicPartitions().setTopicName(barTopicName)).asJava))

    val member2 = new ShareGroupDescribeResponseData.Member()
      .setMemberId("member2")
      .setAssignment(new ShareGroupDescribeResponseData.Assignment()
        .setTopicPartitions(List(
          new ShareGroupDescribeResponseData.TopicPartitions().setTopicName(barTopicName)).asJava))

    val describedGroups: util.List[ShareGroupDescribeResponseData.DescribedGroup] = List(
      new ShareGroupDescribeResponseData.DescribedGroup().setGroupId(groupIds.get(0)).setMembers(List(member0).asJava),
      new ShareGroupDescribeResponseData.DescribedGroup().setGroupId(groupIds.get(1)).setMembers(List(member1).asJava),
      new ShareGroupDescribeResponseData.DescribedGroup().setGroupId(groupIds.get(2)).setMembers(List(member2).asJava)
    ).asJava
    getShareGroupDescribeResponse(groupIds, Map(ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true")
      , true, null, describedGroups)
  }

  @Test
  def testShareGroupDescribeReturnsUnsupportedVersion(): Unit = {
    val groupIds = List("share-group-id-0", "share-group-id-1").asJava
    val describedGroups: util.List[ShareGroupDescribeResponseData.DescribedGroup] = List(
      new ShareGroupDescribeResponseData.DescribedGroup().setGroupId(groupIds.get(0)),
      new ShareGroupDescribeResponseData.DescribedGroup().setGroupId(groupIds.get(1))
    ).asJava
    val response = getShareGroupDescribeResponse(groupIds, Map.empty, false, null, describedGroups)
    assertNotNull(response.data)
    assertEquals(2, response.data.groups.size)
    response.data.groups.forEach(group => assertEquals(Errors.UNSUPPORTED_VERSION.code(), group.errorCode()))
  }

  @Test
  def testShareGroupDescribeRequestAuthorizationFailed(): Unit = {
    val groupIds = List("share-group-id-0", "share-group-id-1").asJava
    val describedGroups: util.List[ShareGroupDescribeResponseData.DescribedGroup] = List().asJava
    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenReturn(Seq(AuthorizationResult.DENIED).asJava)
    val response = getShareGroupDescribeResponse(groupIds, Map(ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true")
      , false, authorizer, describedGroups)
    assertNotNull(response.data)
    assertEquals(2, response.data.groups.size)
    response.data.groups.forEach(group => assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code(), group.errorCode()))
  }

  @Test
  def testShareGroupDescribeRequestAuthorizationFailedForOneGroup(): Unit = {
    val groupIds = List("share-group-id-fail-0", "share-group-id-1").asJava
    val describedGroups: util.List[ShareGroupDescribeResponseData.DescribedGroup] = List(
      new ShareGroupDescribeResponseData.DescribedGroup().setGroupId(groupIds.get(1))
    ).asJava

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenReturn(Seq(AuthorizationResult.DENIED).asJava, Seq(AuthorizationResult.ALLOWED).asJava)

    val response = getShareGroupDescribeResponse(groupIds, Map(ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true")
      , false, authorizer, describedGroups)

    assertNotNull(response.data)
    assertEquals(2, response.data.groups.size)
    assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code(), response.data.groups.get(0).errorCode())
    assertEquals(Errors.NONE.code(), response.data.groups.get(1).errorCode())
  }

  @Test
  def testShareGroupDescribeFilterUnauthorizedTopics(): Unit = {
    val fooTopicName = "foo"
    val barTopicName = "bar"
    val errorMessage = "The group has described topic(s) that the client is not authorized to describe."

    val groupIds = List("share-group-id-0", "share-group-id-1", "share-group_id-2").asJava

    val authorizer: Authorizer = mock(classOf[Authorizer])
    val acls = Map(
      groupIds.get(0) -> AuthorizationResult.ALLOWED,
      groupIds.get(1) -> AuthorizationResult.ALLOWED,
      groupIds.get(2) -> AuthorizationResult.ALLOWED,
      fooTopicName    -> AuthorizationResult.ALLOWED,
      barTopicName    -> AuthorizationResult.DENIED,
    )
    when(authorizer.authorize(
      any[RequestContext],
      any[util.List[Action]]
    )).thenAnswer { invocation =>
      val actions = invocation.getArgument(1, classOf[util.List[Action]])
      actions.asScala.map { action =>
        acls.getOrElse(action.resourcePattern.name, AuthorizationResult.DENIED)
      }.asJava
    }
     val member0 = new ShareGroupDescribeResponseData.Member()
      .setMemberId("member0")
      .setAssignment(new ShareGroupDescribeResponseData.Assignment()
        .setTopicPartitions(List(
          new ShareGroupDescribeResponseData.TopicPartitions().setTopicName(fooTopicName)).asJava))

    val member1 = new ShareGroupDescribeResponseData.Member()
      .setMemberId("member1")
      .setAssignment(new ShareGroupDescribeResponseData.Assignment()
        .setTopicPartitions(List(
          new ShareGroupDescribeResponseData.TopicPartitions().setTopicName(fooTopicName),
          new ShareGroupDescribeResponseData.TopicPartitions().setTopicName(barTopicName)).asJava))

    val member2 = new ShareGroupDescribeResponseData.Member()
      .setMemberId("member2")
      .setAssignment(new ShareGroupDescribeResponseData.Assignment()
        .setTopicPartitions(List(
          new ShareGroupDescribeResponseData.TopicPartitions().setTopicName(barTopicName)).asJava))

    val describedGroups: util.List[ShareGroupDescribeResponseData.DescribedGroup] = List(
      new ShareGroupDescribeResponseData.DescribedGroup()
        .setGroupId(groupIds.get(0))
        .setMembers(List(member0).asJava),
      new ShareGroupDescribeResponseData.DescribedGroup()
        .setGroupId(groupIds.get(1))
        .setMembers(List(member1).asJava),
      new ShareGroupDescribeResponseData.DescribedGroup()
        .setGroupId(groupIds.get(2))
        .setMembers(List(member2).asJava)).asJava

    val response = getShareGroupDescribeResponse(groupIds, Map(ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true")
      , false, authorizer, describedGroups)

    assertNotNull(response.data)
    assertEquals(3, response.data.groups.size)
    assertEquals(Errors.NONE.code(), response.data.groups.get(0).errorCode())
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code(), response.data.groups.get(1).errorCode())
    assertEquals(Errors.TOPIC_AUTHORIZATION_FAILED.code(), response.data.groups.get(2).errorCode())
    assertEquals(errorMessage, response.data.groups.get(1).errorMessage())
    assertEquals(errorMessage, response.data.groups.get(2).errorMessage())
  }

  @Test
  def testReadShareGroupStateSuccess(): Unit = {
    val topicId = Uuid.randomUuid();
    val readRequestData = new ReadShareGroupStateRequestData()
      .setGroupId("group1")
      .setTopics(List(
        new ReadShareGroupStateRequestData.ReadStateData()
          .setTopicId(topicId)
          .setPartitions(List(
            new ReadShareGroupStateRequestData.PartitionData()
              .setPartition(1)
              .setLeaderEpoch(1)
          ).asJava)
      ).asJava)

    val readStateResultData: util.List[ReadShareGroupStateResponseData.ReadStateResult] = List(
      new ReadShareGroupStateResponseData.ReadStateResult()
        .setTopicId(topicId)
        .setPartitions(List(
          new ReadShareGroupStateResponseData.PartitionResult()
            .setPartition(1)
            .setErrorCode(Errors.NONE.code())
            .setErrorMessage(null)
            .setStateEpoch(1)
            .setStartOffset(10)
            .setStateBatches(List(
              new ReadShareGroupStateResponseData.StateBatch()
                .setFirstOffset(11)
                .setLastOffset(15)
                .setDeliveryState(0)
                .setDeliveryCount(1)
            ).asJava)
        ).asJava)
    ).asJava

    val config = Map(
      ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true",
    )

    val response = getReadShareGroupResponse(
      readRequestData,
      config ++ ShareCoordinatorTestConfig.testConfigMap().asScala,
      verifyNoErr = true,
      null,
      readStateResultData
    )

    assertNotNull(response.data)
    assertEquals(1, response.data.results.size)
  }

  @Test
  def testReadShareGroupStateAuthorizationFailed(): Unit = {
    val topicId = Uuid.randomUuid();
    val readRequestData = new ReadShareGroupStateRequestData()
      .setGroupId("group1")
      .setTopics(List(
        new ReadShareGroupStateRequestData.ReadStateData()
          .setTopicId(topicId)
          .setPartitions(List(
            new ReadShareGroupStateRequestData.PartitionData()
              .setPartition(1)
              .setLeaderEpoch(1)
          ).asJava)
      ).asJava)

    val readStateResultData: util.List[ReadShareGroupStateResponseData.ReadStateResult] = List(
      new ReadShareGroupStateResponseData.ReadStateResult()
        .setTopicId(topicId)
        .setPartitions(List(
          new ReadShareGroupStateResponseData.PartitionResult()
            .setPartition(1)
            .setErrorCode(Errors.NONE.code())
            .setErrorMessage(null)
            .setStateEpoch(1)
            .setStartOffset(10)
            .setStateBatches(List(
              new ReadShareGroupStateResponseData.StateBatch()
                .setFirstOffset(11)
                .setLastOffset(15)
                .setDeliveryState(0)
                .setDeliveryCount(1)
            ).asJava)
        ).asJava)
    ).asJava

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenReturn(Seq(AuthorizationResult.DENIED).asJava, Seq(AuthorizationResult.ALLOWED).asJava)

    val config = Map(
      ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true",
    )

    val response = getReadShareGroupResponse(
      readRequestData,
      config ++ ShareCoordinatorTestConfig.testConfigMap().asScala,
      verifyNoErr = false,
      authorizer,
      readStateResultData
    )

    assertNotNull(response.data)
    assertEquals(1, response.data.results.size)
    response.data.results.forEach(readResult => {
      assertEquals(1, readResult.partitions.size)
      assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED.code(), readResult.partitions.get(0).errorCode())
    })
  }

  @Test
  def testReadShareGroupStateSummarySuccess(): Unit = {
    val topicId = Uuid.randomUuid();
    val readSummaryRequestData = new ReadShareGroupStateSummaryRequestData()
      .setGroupId("group1")
      .setTopics(List(
        new ReadShareGroupStateSummaryRequestData.ReadStateSummaryData()
          .setTopicId(topicId)
          .setPartitions(List(
            new ReadShareGroupStateSummaryRequestData.PartitionData()
              .setPartition(1)
              .setLeaderEpoch(1)
          ).asJava)
      ).asJava)

    val readStateSummaryResultData: util.List[ReadShareGroupStateSummaryResponseData.ReadStateSummaryResult] = List(
      new ReadShareGroupStateSummaryResponseData.ReadStateSummaryResult()
        .setTopicId(topicId)
        .setPartitions(List(
          new ReadShareGroupStateSummaryResponseData.PartitionResult()
            .setPartition(1)
            .setErrorCode(Errors.NONE.code())
            .setErrorMessage(null)
            .setStateEpoch(1)
            .setStartOffset(10)
        ).asJava)
    ).asJava

    val config = Map(
      ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true",
    )

    val response = getReadShareGroupSummaryResponse(
      readSummaryRequestData,
      config ++ ShareCoordinatorTestConfig.testConfigMap().asScala,
      verifyNoErr = true,
      null,
      readStateSummaryResultData
    )

    assertNotNull(response.data)
    assertEquals(1, response.data.results.size)
  }

  @Test
  def testReadShareGroupStateSummaryAuthorizationFailed(): Unit = {
    val topicId = Uuid.randomUuid();
    val readSummaryRequestData = new ReadShareGroupStateSummaryRequestData()
      .setGroupId("group1")
      .setTopics(List(
        new ReadShareGroupStateSummaryRequestData.ReadStateSummaryData()
          .setTopicId(topicId)
          .setPartitions(List(
            new ReadShareGroupStateSummaryRequestData.PartitionData()
              .setPartition(1)
              .setLeaderEpoch(1)
          ).asJava)
      ).asJava)

    val readStateSummaryResultData: util.List[ReadShareGroupStateSummaryResponseData.ReadStateSummaryResult] = List(
      new ReadShareGroupStateSummaryResponseData.ReadStateSummaryResult()
        .setTopicId(topicId)
        .setPartitions(List(
          new ReadShareGroupStateSummaryResponseData.PartitionResult()
            .setPartition(1)
            .setErrorCode(Errors.NONE.code())
            .setErrorMessage(null)
            .setStateEpoch(1)
            .setStartOffset(10)
        ).asJava)
    ).asJava

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenReturn(Seq(AuthorizationResult.DENIED).asJava, Seq(AuthorizationResult.ALLOWED).asJava)

    val config = Map(
      ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true",
    )

    val response = getReadShareGroupSummaryResponse(
      readSummaryRequestData,
      config ++ ShareCoordinatorTestConfig.testConfigMap().asScala,
      verifyNoErr = false,
      authorizer,
      readStateSummaryResultData
    )

    assertNotNull(response.data)
    assertEquals(1, response.data.results.size)
    response.data.results.forEach(readResult => {
      assertEquals(1, readResult.partitions.size)
      assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED.code(), readResult.partitions.get(0).errorCode())
    })
  }

  @Test
  def testDescribeShareGroupOffsetsReturnsUnsupportedVersion(): Unit = {
    val describeShareGroupOffsetsRequest = new DescribeShareGroupOffsetsRequestData().setGroups(
      util.List.of(new DescribeShareGroupOffsetsRequestGroup().setGroupId("group").setTopics(
        util.List.of(new DescribeShareGroupOffsetsRequestTopic().setTopicName("topic-1").setPartitions(util.List.of(1)))
      ))
    )

    val requestChannelRequest = buildRequest(new DescribeShareGroupOffsetsRequest.Builder(describeShareGroupOffsetsRequest, true).build())
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    kafkaApis = createKafkaApis()
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val response = verifyNoThrottling[DescribeShareGroupOffsetsResponse](requestChannelRequest)
    response.data.groups.forEach(group => group.topics.forEach(topic => topic.partitions.forEach(partition => assertEquals(Errors.UNSUPPORTED_VERSION.code, partition.errorCode))))
  }

  @Test
  def testDescribeShareGroupOffsetsRequestsAuthorizationFailed(): Unit = {
    val describeShareGroupOffsetsRequest = new DescribeShareGroupOffsetsRequestData().setGroups(
      util.List.of(new DescribeShareGroupOffsetsRequestGroup().setGroupId("group").setTopics(
        util.List.of(new DescribeShareGroupOffsetsRequestTopic().setTopicName("topic-1").setPartitions(util.List.of(1)))
      ))
    )

    val requestChannelRequest = buildRequest(new DescribeShareGroupOffsetsRequest.Builder(describeShareGroupOffsetsRequest, true).build)

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenReturn(util.List.of(AuthorizationResult.DENIED))
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    kafkaApis = createKafkaApis(
      overrideProperties = Map(ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
      authorizer = Some(authorizer),
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val response = verifyNoThrottling[DescribeShareGroupOffsetsResponse](requestChannelRequest)
    response.data.groups.forEach(
      group => group.topics.forEach(
        topic => topic.partitions.forEach(
          partition => assertEquals(Errors.GROUP_AUTHORIZATION_FAILED.code, partition.errorCode)
        )
      )
    )
  }

  @Test
  def testDescribeShareGroupOffsetsRequestSuccess(): Unit = {
    val topicName1 = "topic-1"
    val topicId1 = Uuid.randomUuid
    val topicName2 = "topic-2"
    val topicId2 = Uuid.randomUuid
    val topicName3 = "topic-3"
    val topicId3 = Uuid.randomUuid
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    addTopicToMetadataCache(topicName1, 1, topicId = topicId1)
    addTopicToMetadataCache(topicName2, 1, topicId = topicId2)
    addTopicToMetadataCache(topicName3, 1, topicId = topicId3)

    val describeShareGroupOffsetsRequestGroup1 = new DescribeShareGroupOffsetsRequestGroup().setGroupId("group1").setTopics(
      util.List.of(
        new DescribeShareGroupOffsetsRequestTopic().setTopicName(topicName1).setPartitions(util.List.of(1, 2, 3)),
        new DescribeShareGroupOffsetsRequestTopic().setTopicName(topicName2).setPartitions(util.List.of(10, 20)),
      )
    )

    val describeShareGroupOffsetsRequestGroup2 = new DescribeShareGroupOffsetsRequestGroup().setGroupId("group2").setTopics(
      util.List.of(
        new DescribeShareGroupOffsetsRequestTopic().setTopicName(topicName3).setPartitions(util.List.of(0)),
      )
    )

    val describeShareGroupOffsetsRequest = new DescribeShareGroupOffsetsRequestData()
      .setGroups(util.List.of(describeShareGroupOffsetsRequestGroup1, describeShareGroupOffsetsRequestGroup2))

    val requestChannelRequest = buildRequest(new DescribeShareGroupOffsetsRequest.Builder(describeShareGroupOffsetsRequest, true).build)

    val futureGroup1 = new CompletableFuture[DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup]
    when(groupCoordinator.describeShareGroupOffsets(
      requestChannelRequest.context,
      describeShareGroupOffsetsRequestGroup1
    )).thenReturn(futureGroup1)
    val futureGroup2 = new CompletableFuture[DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup]
    when(groupCoordinator.describeShareGroupOffsets(
      requestChannelRequest.context,
      describeShareGroupOffsetsRequestGroup2
    )).thenReturn(futureGroup2)
    kafkaApis = createKafkaApis(
      overrideProperties = Map(ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val describeShareGroupOffsetsResponseGroup1 = new DescribeShareGroupOffsetsResponseGroup()
      .setGroupId("group1")
      .setTopics(util.List.of(
        new DescribeShareGroupOffsetsResponseTopic()
          .setTopicName(topicName1)
          .setTopicId(topicId1)
          .setPartitions(util.List.of(
            new DescribeShareGroupOffsetsResponsePartition()
              .setPartitionIndex(1)
              .setStartOffset(0)
              .setLeaderEpoch(1)
              .setErrorMessage(null)
              .setErrorCode(0),
            new DescribeShareGroupOffsetsResponsePartition()
              .setPartitionIndex(2)
              .setStartOffset(0)
              .setLeaderEpoch(1)
              .setErrorMessage(null)
              .setErrorCode(0),
            new DescribeShareGroupOffsetsResponsePartition()
              .setPartitionIndex(3)
              .setStartOffset(0)
              .setLeaderEpoch(1)
              .setErrorMessage(null)
              .setErrorCode(0)
          )),
        new DescribeShareGroupOffsetsResponseTopic()
          .setTopicName(topicName2)
          .setTopicId(topicId2)
          .setPartitions(util.List.of(
            new DescribeShareGroupOffsetsResponsePartition()
              .setPartitionIndex(10)
              .setStartOffset(0)
              .setLeaderEpoch(1)
              .setErrorMessage(null)
              .setErrorCode(0),
            new DescribeShareGroupOffsetsResponsePartition()
              .setPartitionIndex(20)
              .setStartOffset(0)
              .setLeaderEpoch(1)
              .setErrorMessage(null)
              .setErrorCode(0)
          ))
      ))

    val describeShareGroupOffsetsResponseGroup2 = new DescribeShareGroupOffsetsResponseGroup()
      .setGroupId("group2")
      .setTopics(util.List.of(
        new DescribeShareGroupOffsetsResponseTopic()
          .setTopicName(topicName3)
          .setTopicId(topicId3)
          .setPartitions(util.List.of(
            new DescribeShareGroupOffsetsResponsePartition()
              .setPartitionIndex(0)
              .setStartOffset(0)
              .setLeaderEpoch(1)
              .setErrorMessage(null)
              .setErrorCode(0)
          ))
      ))

    val describeShareGroupOffsetsResponse = new DescribeShareGroupOffsetsResponseData()
      .setGroups(util.List.of(describeShareGroupOffsetsResponseGroup1, describeShareGroupOffsetsResponseGroup2))

    futureGroup1.complete(describeShareGroupOffsetsResponseGroup1)
    futureGroup2.complete(describeShareGroupOffsetsResponseGroup2)
    val response = verifyNoThrottling[DescribeShareGroupOffsetsResponse](requestChannelRequest)
    assertEquals(describeShareGroupOffsetsResponse, response.data)
  }

  @Test
  def testDescribeShareGroupOffsetsRequestEmptyGroupsSuccess(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)

    val describeShareGroupOffsetsRequest = new DescribeShareGroupOffsetsRequestData

    val requestChannelRequest = buildRequest(new DescribeShareGroupOffsetsRequest.Builder(describeShareGroupOffsetsRequest, true).build)

    val future = new CompletableFuture[DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup]
    kafkaApis = createKafkaApis(
      overrideProperties = Map(ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val describeShareGroupOffsetsResponseGroup = new DescribeShareGroupOffsetsResponseGroup()

    val describeShareGroupOffsetsResponse = new DescribeShareGroupOffsetsResponseData()

    future.complete(describeShareGroupOffsetsResponseGroup)
    val response = verifyNoThrottling[DescribeShareGroupOffsetsResponse](requestChannelRequest)
    assertEquals(describeShareGroupOffsetsResponse, response.data)
  }

  @Test
  def testDescribeShareGroupOffsetsRequestEmptyTopicsSuccess(): Unit = {
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)

    val describeShareGroupOffsetsRequestGroup = new DescribeShareGroupOffsetsRequestGroup().setGroupId("group")

    val describeShareGroupOffsetsRequest = new DescribeShareGroupOffsetsRequestData().setGroups(util.List.of(describeShareGroupOffsetsRequestGroup))

    val requestChannelRequest = buildRequest(new DescribeShareGroupOffsetsRequest.Builder(describeShareGroupOffsetsRequest, true).build)

    val future = new CompletableFuture[DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup]
    when(groupCoordinator.describeShareGroupOffsets(
      requestChannelRequest.context,
      describeShareGroupOffsetsRequestGroup
    )).thenReturn(future)
    kafkaApis = createKafkaApis(
      overrideProperties = Map(ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true"),
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    val describeShareGroupOffsetsResponseGroup = new DescribeShareGroupOffsetsResponseGroup()
      .setGroupId("group")
      .setTopics(util.List.of())

    val describeShareGroupOffsetsResponse = new DescribeShareGroupOffsetsResponseData().setGroups(util.List.of(describeShareGroupOffsetsResponseGroup))

    future.complete(describeShareGroupOffsetsResponseGroup)
    val response = verifyNoThrottling[DescribeShareGroupOffsetsResponse](requestChannelRequest)
    assertEquals(describeShareGroupOffsetsResponse, response.data)
  }

  @Test
  def testWriteShareGroupStateSuccess(): Unit = {
    val topicId = Uuid.randomUuid();
    val writeRequestData = new WriteShareGroupStateRequestData()
      .setGroupId("group1")
      .setTopics(List(
        new WriteShareGroupStateRequestData.WriteStateData()
          .setTopicId(topicId)
          .setPartitions(List(
            new WriteShareGroupStateRequestData.PartitionData()
              .setPartition(1)
              .setLeaderEpoch(1)
              .setStateEpoch(2)
              .setStartOffset(10)
              .setStateBatches(List(
                new WriteShareGroupStateRequestData.StateBatch()
                  .setFirstOffset(11)
                  .setLastOffset(15)
                  .setDeliveryCount(1)
                  .setDeliveryState(0)
              ).asJava)
          ).asJava)
      ).asJava)

    val writeStateResultData: util.List[WriteShareGroupStateResponseData.WriteStateResult] = List(
      new WriteShareGroupStateResponseData.WriteStateResult()
        .setTopicId(topicId)
        .setPartitions(List(
          new WriteShareGroupStateResponseData.PartitionResult()
            .setPartition(1)
            .setErrorCode(Errors.NONE.code())
            .setErrorMessage(null)
        ).asJava)
    ).asJava

    val config = Map(
      ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true",
    )

    val response = getWriteShareGroupResponse(
      writeRequestData,
      config ++ ShareCoordinatorTestConfig.testConfigMap().asScala,
      verifyNoErr = true,
      null,
      writeStateResultData
    )

    assertNotNull(response.data)
    assertEquals(1, response.data.results.size)
  }

  @Test
  def testWriteShareGroupStateAuthorizationFailed(): Unit = {
    val topicId = Uuid.randomUuid();
    val writeRequestData = new WriteShareGroupStateRequestData()
      .setGroupId("group1")
      .setTopics(List(
        new WriteShareGroupStateRequestData.WriteStateData()
          .setTopicId(topicId)
          .setPartitions(List(
            new WriteShareGroupStateRequestData.PartitionData()
              .setPartition(1)
              .setLeaderEpoch(1)
              .setStateEpoch(2)
              .setStartOffset(10)
              .setStateBatches(List(
                new WriteShareGroupStateRequestData.StateBatch()
                  .setFirstOffset(11)
                  .setLastOffset(15)
                  .setDeliveryCount(1)
                  .setDeliveryState(0)
              ).asJava)
          ).asJava)
      ).asJava)

    val writeStateResultData: util.List[WriteShareGroupStateResponseData.WriteStateResult] = List(
      new WriteShareGroupStateResponseData.WriteStateResult()
        .setTopicId(topicId)
        .setPartitions(List(
          new WriteShareGroupStateResponseData.PartitionResult()
            .setPartition(1)
            .setErrorCode(Errors.NONE.code())
            .setErrorMessage(null)
        ).asJava)
    ).asJava

    val config = Map(
      ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true",
    )

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenReturn(Seq(AuthorizationResult.DENIED).asJava, Seq(AuthorizationResult.ALLOWED).asJava)

    val response = getWriteShareGroupResponse(
      writeRequestData,
      config ++ ShareCoordinatorTestConfig.testConfigMap().asScala,
      verifyNoErr = false,
      authorizer,
      writeStateResultData
    )

    assertNotNull(response.data)
    assertEquals(1, response.data.results.size)
    response.data.results.forEach(writeResult => {
      assertEquals(1, writeResult.partitions.size)
      assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED.code(), writeResult.partitions.get(0).errorCode())
    })
  }

  @Test
  def testDeleteShareGroupStateSuccess(): Unit = {
    val topicId = Uuid.randomUuid();
    val deleteRequestData = new DeleteShareGroupStateRequestData()
      .setGroupId("group1")
      .setTopics(List(
        new DeleteShareGroupStateRequestData.DeleteStateData()
          .setTopicId(topicId)
          .setPartitions(List(
            new DeleteShareGroupStateRequestData.PartitionData()
              .setPartition(1)
          ).asJava)
      ).asJava)

    val deleteStateResultData: util.List[DeleteShareGroupStateResponseData.DeleteStateResult] = List(
      new DeleteShareGroupStateResponseData.DeleteStateResult()
        .setTopicId(topicId)
        .setPartitions(List(
          new DeleteShareGroupStateResponseData.PartitionResult()
            .setPartition(1)
            .setErrorCode(Errors.NONE.code())
            .setErrorMessage(null)
        ).asJava)
    ).asJava

    val config = Map(
      ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true",
    )

    val response = getDeleteShareGroupResponse(
      deleteRequestData,
      config ++ ShareCoordinatorTestConfig.testConfigMap().asScala,
      verifyNoErr = true,
      null,
      deleteStateResultData
    )

    assertNotNull(response.data)
    assertEquals(1, response.data.results.size)
  }

  @Test
  def testDeleteShareGroupStateAuthorizationFailed(): Unit = {
    val topicId = Uuid.randomUuid();
    val deleteRequestData = new DeleteShareGroupStateRequestData()
      .setGroupId("group1")
      .setTopics(List(
        new DeleteShareGroupStateRequestData.DeleteStateData()
          .setTopicId(topicId)
          .setPartitions(List(
            new DeleteShareGroupStateRequestData.PartitionData()
              .setPartition(1)
          ).asJava)
      ).asJava)

    val deleteStateResultData: util.List[DeleteShareGroupStateResponseData.DeleteStateResult] = List(
      new DeleteShareGroupStateResponseData.DeleteStateResult()
        .setTopicId(topicId)
        .setPartitions(List(
          new DeleteShareGroupStateResponseData.PartitionResult()
            .setPartition(1)
            .setErrorCode(Errors.NONE.code())
            .setErrorMessage(null)
        ).asJava)
    ).asJava

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenReturn(Seq(AuthorizationResult.DENIED).asJava, Seq(AuthorizationResult.ALLOWED).asJava)

    val config = Map(
      ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true",
    )

    val response = getDeleteShareGroupResponse(
      deleteRequestData,
      config ++ ShareCoordinatorTestConfig.testConfigMap().asScala,
      verifyNoErr = false,
      authorizer,
      deleteStateResultData
    )

    assertNotNull(response.data)
    assertEquals(1, response.data.results.size)
    response.data.results.forEach(deleteResult => {
      assertEquals(1, deleteResult.partitions.size)
      assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED.code(), deleteResult.partitions.get(0).errorCode())
    })
  }

  @Test
  def testInitializeShareGroupStateSuccess(): Unit = {
    val topicId = Uuid.randomUuid();
    val initRequestData = new InitializeShareGroupStateRequestData()
      .setGroupId("group1")
      .setTopics(List(
        new InitializeShareGroupStateRequestData.InitializeStateData()
          .setTopicId(topicId)
          .setPartitions(List(
            new InitializeShareGroupStateRequestData.PartitionData()
              .setPartition(1)
              .setStateEpoch(0)
          ).asJava)
      ).asJava)

    val initStateResultData: util.List[InitializeShareGroupStateResponseData.InitializeStateResult] = List(
      new InitializeShareGroupStateResponseData.InitializeStateResult()
        .setTopicId(topicId)
        .setPartitions(List(
          new InitializeShareGroupStateResponseData.PartitionResult()
            .setPartition(1)
            .setErrorCode(Errors.NONE.code())
            .setErrorMessage(null)
        ).asJava)
    ).asJava

    val config = Map(
      ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true",
    )

    val response = getInitializeShareGroupResponse(
      initRequestData,
      config ++ ShareCoordinatorTestConfig.testConfigMap().asScala,
      verifyNoErr = true,
      null,
      initStateResultData
    )

    assertNotNull(response.data)
    assertEquals(1, response.data.results.size)
  }

  @Test
  def testInitializeShareGroupStateAuthorizationFailed(): Unit = {
    val topicId = Uuid.randomUuid();
    val initRequestData = new InitializeShareGroupStateRequestData()
      .setGroupId("group1")
      .setTopics(List(
        new InitializeShareGroupStateRequestData.InitializeStateData()
          .setTopicId(topicId)
          .setPartitions(List(
            new InitializeShareGroupStateRequestData.PartitionData()
              .setPartition(1)
              .setStateEpoch(0)
          ).asJava)
      ).asJava)

    val initStateResultData: util.List[InitializeShareGroupStateResponseData.InitializeStateResult] = List(
      new InitializeShareGroupStateResponseData.InitializeStateResult()
        .setTopicId(topicId)
        .setPartitions(List(
          new InitializeShareGroupStateResponseData.PartitionResult()
            .setPartition(1)
            .setErrorCode(Errors.NONE.code())
            .setErrorMessage(null)
        ).asJava)
    ).asJava

    val authorizer: Authorizer = mock(classOf[Authorizer])
    when(authorizer.authorize(any[RequestContext], any[util.List[Action]]))
      .thenReturn(Seq(AuthorizationResult.DENIED).asJava, Seq(AuthorizationResult.ALLOWED).asJava)

    val config = Map(
      ShareGroupConfig.SHARE_GROUP_ENABLE_CONFIG -> "true",
    )

    val response = getInitializeShareGroupResponse(
      initRequestData,
      config ++ ShareCoordinatorTestConfig.testConfigMap().asScala,
      verifyNoErr = false,
      authorizer,
      initStateResultData
    )

    assertNotNull(response.data)
    assertEquals(1, response.data.results.size)
    response.data.results.forEach(deleteResult => {
      assertEquals(1, deleteResult.partitions.size)
      assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED.code(), deleteResult.partitions.get(0).errorCode())
    })
  }

  def getShareGroupDescribeResponse(groupIds: util.List[String], configOverrides: Map[String, String] = Map.empty,
                                    verifyNoErr: Boolean = true, authorizer: Authorizer = null,
                                    describedGroups: util.List[ShareGroupDescribeResponseData.DescribedGroup]): ShareGroupDescribeResponse = {
    val shareGroupDescribeRequestData = new ShareGroupDescribeRequestData()
    shareGroupDescribeRequestData.groupIds.addAll(groupIds)
    val requestChannelRequest = buildRequest(new ShareGroupDescribeRequest.Builder(shareGroupDescribeRequestData, true).build())

    val future = new CompletableFuture[util.List[ShareGroupDescribeResponseData.DescribedGroup]]()
    when(groupCoordinator.shareGroupDescribe(
      any[RequestContext],
      any[util.List[String]]
    )).thenReturn(future)
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    kafkaApis = createKafkaApis(
      overrideProperties = configOverrides,
      authorizer = Option(authorizer),
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching)

    future.complete(describedGroups)

    val response = verifyNoThrottling[ShareGroupDescribeResponse](requestChannelRequest)
    if (verifyNoErr) {
      val expectedShareGroupDescribeResponseData = new ShareGroupDescribeResponseData()
        .setGroups(describedGroups)
      assertEquals(expectedShareGroupDescribeResponseData, response.data)
    }
    response
  }

  def getReadShareGroupResponse(requestData: ReadShareGroupStateRequestData, configOverrides: Map[String, String] = Map.empty,
                                verifyNoErr: Boolean = true, authorizer: Authorizer = null,
                                readStateResult: util.List[ReadShareGroupStateResponseData.ReadStateResult]): ReadShareGroupStateResponse = {
    val requestChannelRequest = buildRequest(new ReadShareGroupStateRequest.Builder(requestData, true).build())

    val future = new CompletableFuture[ReadShareGroupStateResponseData]()
    when(shareCoordinator.readState(
      any[RequestContext],
      any[ReadShareGroupStateRequestData]
    )).thenReturn(future)
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    kafkaApis = createKafkaApis(
      overrideProperties = configOverrides,
      authorizer = Option(authorizer),
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching())

    future.complete(new ReadShareGroupStateResponseData()
      .setResults(readStateResult))

    val response = verifyNoThrottling[ReadShareGroupStateResponse](requestChannelRequest)
    if (verifyNoErr) {
      val expectedReadShareGroupStateResponseData = new ReadShareGroupStateResponseData()
        .setResults(readStateResult)
      assertEquals(expectedReadShareGroupStateResponseData, response.data)
    }
    response
  }

  def getReadShareGroupSummaryResponse(requestData: ReadShareGroupStateSummaryRequestData, configOverrides: Map[String, String] = Map.empty,
                                verifyNoErr: Boolean = true, authorizer: Authorizer = null,
                                readStateSummaryResult: util.List[ReadShareGroupStateSummaryResponseData.ReadStateSummaryResult]): ReadShareGroupStateSummaryResponse = {
    val requestChannelRequest = buildRequest(new ReadShareGroupStateSummaryRequest.Builder(requestData, true).build())

    val future = new CompletableFuture[ReadShareGroupStateSummaryResponseData]()
    when(shareCoordinator.readStateSummary(
      any[RequestContext],
      any[ReadShareGroupStateSummaryRequestData]
    )).thenReturn(future)
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    kafkaApis = createKafkaApis(
      overrideProperties = configOverrides,
      authorizer = Option(authorizer),
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching())

    future.complete(new ReadShareGroupStateSummaryResponseData()
      .setResults(readStateSummaryResult))

    val response = verifyNoThrottling[ReadShareGroupStateSummaryResponse](requestChannelRequest)
    if (verifyNoErr) {
      val expectedReadShareGroupStateSummaryResponseData = new ReadShareGroupStateSummaryResponseData()
        .setResults(readStateSummaryResult)
      assertEquals(expectedReadShareGroupStateSummaryResponseData, response.data)
    }
    response
  }

  def getWriteShareGroupResponse(requestData: WriteShareGroupStateRequestData, configOverrides: Map[String, String] = Map.empty,
                                verifyNoErr: Boolean = true, authorizer: Authorizer = null,
                                 writeStateResult: util.List[WriteShareGroupStateResponseData.WriteStateResult]): WriteShareGroupStateResponse = {
    val requestChannelRequest = buildRequest(new WriteShareGroupStateRequest.Builder(requestData, true).build())

    val future = new CompletableFuture[WriteShareGroupStateResponseData]()
    when(shareCoordinator.writeState(
      any[RequestContext],
      any[WriteShareGroupStateRequestData]
    )).thenReturn(future)
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    kafkaApis = createKafkaApis(
      overrideProperties = configOverrides,
      authorizer = Option(authorizer),
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching())

    future.complete(new WriteShareGroupStateResponseData()
      .setResults(writeStateResult))

    val response = verifyNoThrottling[WriteShareGroupStateResponse](requestChannelRequest)
    if (verifyNoErr) {
      val expectedWriteShareGroupStateResponseData = new WriteShareGroupStateResponseData()
        .setResults(writeStateResult)
      assertEquals(expectedWriteShareGroupStateResponseData, response.data)
    }
    response
  }

  def getDeleteShareGroupResponse(requestData: DeleteShareGroupStateRequestData, configOverrides: Map[String, String] = Map.empty,
                                  verifyNoErr: Boolean = true, authorizer: Authorizer = null,
                                  deleteStateResult: util.List[DeleteShareGroupStateResponseData.DeleteStateResult]): DeleteShareGroupStateResponse = {
    val requestChannelRequest = buildRequest(new DeleteShareGroupStateRequest.Builder(requestData, true).build())

    val future = new CompletableFuture[DeleteShareGroupStateResponseData]()
    when(shareCoordinator.deleteState(
      any[RequestContext],
      any[DeleteShareGroupStateRequestData]
    )).thenReturn(future)
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    kafkaApis = createKafkaApis(
      overrideProperties = configOverrides,
      authorizer = Option(authorizer),
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching())

    future.complete(new DeleteShareGroupStateResponseData()
      .setResults(deleteStateResult))

    val response = verifyNoThrottling[DeleteShareGroupStateResponse](requestChannelRequest)
    if (verifyNoErr) {
      val expectedDeleteShareGroupStateResponseData = new DeleteShareGroupStateResponseData()
        .setResults(deleteStateResult)
      assertEquals(expectedDeleteShareGroupStateResponseData, response.data)
    }
    response
  }

  def getInitializeShareGroupResponse(requestData: InitializeShareGroupStateRequestData, configOverrides: Map[String, String] = Map.empty,
                                      verifyNoErr: Boolean = true, authorizer: Authorizer = null,
                                      initStateResult: util.List[InitializeShareGroupStateResponseData.InitializeStateResult]): InitializeShareGroupStateResponse = {
    val requestChannelRequest = buildRequest(new InitializeShareGroupStateRequest.Builder(requestData, true).build())

    val future = new CompletableFuture[InitializeShareGroupStateResponseData]()
    when(shareCoordinator.initializeState(
      any[RequestContext],
      any[InitializeShareGroupStateRequestData]
    )).thenReturn(future)
    metadataCache = MetadataCache.kRaftMetadataCache(brokerId, () => KRaftVersion.KRAFT_VERSION_0)
    kafkaApis = createKafkaApis(
      overrideProperties = configOverrides,
      authorizer = Option(authorizer),
    )
    kafkaApis.handle(requestChannelRequest, RequestLocal.noCaching())

    future.complete(new InitializeShareGroupStateResponseData()
      .setResults(initStateResult))

    val response = verifyNoThrottling[InitializeShareGroupStateResponse](requestChannelRequest)
    if (verifyNoErr) {
      val expectedInitShareGroupStateResponseData = new InitializeShareGroupStateResponseData()
        .setResults(initStateResult)
      assertEquals(expectedInitShareGroupStateResponseData, response.data)
    }
    response
  }
}
