/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package kafka.api

import java.{time, util}
import java.util.concurrent._
import java.util.Properties
import kafka.server.KafkaConfig
import kafka.utils.{Logging, TestInfoUtils, TestUtils}
import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.GroupMaxSizeReachedException
import org.apache.kafka.common.message.FindCoordinatorRequestData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{FindCoordinatorRequest, FindCoordinatorResponse}
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.apache.kafka.server.config.{KRaftConfigs, ReplicationConfigs, ServerLogConfigs}
import org.apache.kafka.server.util.ShutdownableThread
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, Disabled, TestInfo}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

import java.time.Duration
import scala.jdk.CollectionConverters._
import scala.collection.{Seq, mutable}

/**
 * Integration tests for the consumer that cover basic usage as well as server failures
 */
class ConsumerBounceTest extends AbstractConsumerTest with Logging {
  val maxGroupSize = 5

  // Time to process commit and leave group requests in tests when brokers are available
  val gracefulCloseTimeMs = Some(1000L)
  val executor: ScheduledExecutorService = Executors.newScheduledThreadPool(2)
  val consumerPollers: mutable.Buffer[ConsumerAssignmentPoller] = mutable.Buffer[ConsumerAssignmentPoller]()

  this.consumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")

  override def generateConfigs: Seq[KafkaConfig] = {
    generateKafkaConfigs()
  }

  val testConfigs = Map[String, String](
    GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG -> "3", // don't want to lose offset
    GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG -> "1",
    GroupCoordinatorConfig.GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG -> "10", // set small enough session timeout
    GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG -> "0",

    // Tests will run for CONSUMER and CLASSIC group protocol, so set the group max size property
    // required for each.
    GroupCoordinatorConfig.CONSUMER_GROUP_MAX_SIZE_CONFIG -> maxGroupSize.toString,
    GroupCoordinatorConfig.GROUP_MAX_SIZE_CONFIG -> maxGroupSize.toString,

    ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG -> "false",
    ReplicationConfigs.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG -> "true",
    ReplicationConfigs.UNCLEAN_LEADER_ELECTION_INTERVAL_MS_CONFIG -> "50",
    KRaftConfigs.BROKER_HEARTBEAT_INTERVAL_MS_CONFIG -> "50",
    KRaftConfigs.BROKER_SESSION_TIMEOUT_MS_CONFIG -> "300",
  )

  override def kraftControllerConfigs(testInfo: TestInfo): Seq[Properties] = {
    super.kraftControllerConfigs(testInfo).map(props => {
      testConfigs.foreachEntry((k, v) => props.setProperty(k, v))
      props
    })
  }

  private def generateKafkaConfigs(maxGroupSize: String = maxGroupSize.toString): Seq[KafkaConfig] = {
    val properties = new Properties
    testConfigs.foreachEntry((k, v) => properties.setProperty(k, v))
    FixedPortTestUtils.createBrokerConfigs(brokerCount, enableControlledShutdown = false, inklessMode = inklessMode)
      .map(KafkaConfig.fromProps(_, properties))
  }

  @AfterEach
  override def tearDown(): Unit = {
    try {
      consumerPollers.foreach(_.shutdown())
      executor.shutdownNow()
      // Wait for any active tasks to terminate to ensure consumer is not closed while being used from another thread
      assertTrue(executor.awaitTermination(5000, TimeUnit.MILLISECONDS), "Executor did not terminate")
    } finally {
      super.tearDown()
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testConsumptionWithBrokerFailures(groupProtocol: String): Unit = consumeWithBrokerFailures(10)

  /*
   * 1. Produce a bunch of messages
   * 2. Then consume the messages while killing and restarting brokers at random
   */
  def consumeWithBrokerFailures(numIters: Int): Unit = {
    val numRecords = 1000
    val producer = createProducer()
    producerSend(producer, numRecords)

    var consumed = 0L
    val consumer = createConsumer()

    consumer.subscribe(util.List.of(topic))

    val scheduler = new BounceBrokerScheduler(numIters)
    try {
      scheduler.start()

      while (scheduler.isRunning) {
        val records = consumer.poll(Duration.ofMillis(100)).asScala

        for (record <- records) {
          assertEquals(consumed, record.offset())
          consumed += 1
        }

        if (records.nonEmpty) {
          consumer.commitSync()
          assertEquals(consumer.position(tp), consumer.committed(util.Set.of(tp)).get(tp).offset)

          if (consumer.position(tp) == numRecords) {
            consumer.seekToBeginning(util.List.of())
            consumed = 0
          }
        }
      }
    } finally {
      scheduler.shutdown()
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testSeekAndCommitWithBrokerFailures(groupProtocol: String): Unit = seekAndCommitWithBrokerFailures(5)

  def seekAndCommitWithBrokerFailures(numIters: Int): Unit = {
    val numRecords = 1000
    val producer = createProducer()
    producerSend(producer, numRecords)

    val consumer = createConsumer()
    consumer.assign(util.List.of(tp))
    consumer.seek(tp, 0)

    // wait until all the followers have synced the last HW with leader
    TestUtils.waitUntilTrue(() => brokerServers.forall(server =>
      server.replicaManager.localLog(tp).get.highWatermark == numRecords
    ), "Failed to update high watermark for followers after timeout")

    val scheduler = new BounceBrokerScheduler(numIters)
    try {
      scheduler.start()

      while (scheduler.isRunning) {
        val coin = TestUtils.random.nextInt(3)
        if (coin == 0) {
          info("Seeking to end of log")
          consumer.seekToEnd(util.List.of())
          assertEquals(numRecords.toLong, consumer.position(tp))
        } else if (coin == 1) {
          val pos = TestUtils.random.nextInt(numRecords).toLong
          info("Seeking to " + pos)
          consumer.seek(tp, pos)
          assertEquals(pos, consumer.position(tp))
        } else if (coin == 2) {
          info("Committing offset.")
          consumer.commitSync()
          assertEquals(consumer.position(tp), consumer.committed(java.util.Set.of(tp)).get(tp).offset)
        }
      }
    } finally {
      scheduler.shutdown()
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testSubscribeWhenTopicUnavailable(groupProtocol: String): Unit = {
    val numRecords = 1000
    val newtopic = "newtopic"

    val consumer = createConsumer()
    consumer.subscribe(util.Set.of(newtopic))
    executor.schedule(new Runnable {
        def run(): Unit = createTopic(newtopic, numPartitions = brokerCount, replicationFactor = brokerCount)
      }, 2, TimeUnit.SECONDS)
    consumer.poll(time.Duration.ZERO)

    val producer = createProducer()

    def sendRecords(numRecords: Int, topic: String): Unit = {
      var remainingRecords = numRecords
      val endTimeMs = System.currentTimeMillis + 20000
      while (remainingRecords > 0 && System.currentTimeMillis < endTimeMs) {
        val futures = (0 until remainingRecords).map { i =>
          producer.send(new ProducerRecord(topic, part, i.toString.getBytes, i.toString.getBytes))
        }
        futures.map { future =>
          try {
            future.get
            remainingRecords -= 1
          } catch {
            case _: Exception =>
          }
        }
      }
      assertEquals(0, remainingRecords)
    }

    val poller = new ConsumerAssignmentPoller(consumer, List(newtopic))
    consumerPollers += poller
    poller.start()
    sendRecords(numRecords, newtopic)
    receiveExactRecords(poller, numRecords, 10000)
    poller.shutdown()

    brokerServers.foreach(server => killBroker(server.config.brokerId))
    Thread.sleep(500)
    restartDeadBrokers()

    val poller2 = new ConsumerAssignmentPoller(consumer, List(newtopic))
    consumerPollers += poller2
    poller2.start()
    sendRecords(numRecords, newtopic)
    receiveExactRecords(poller, numRecords, 10000L)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testClose(groupProtocol: String): Unit = {
    val numRecords = 10
    val producer = createProducer()
    producerSend(producer, numRecords)

    checkCloseGoodPath(numRecords, "group1")
    checkCloseWithCoordinatorFailure(numRecords, "group2", "group3")
    checkCloseWithClusterFailure(numRecords, "group4", "group5", groupProtocol)
  }

  /**
   * Consumer is closed while cluster is healthy. Consumer should complete pending offset commits
   * and leave group. New consumer instance should be able join group and start consuming from
   * last committed offset.
   */
  private def checkCloseGoodPath(numRecords: Int, groupId: String): Unit = {
    val consumer = createConsumerAndReceive(groupId, manualAssign = false, numRecords)
    val future = submitCloseAndValidate(consumer, Long.MaxValue, None, gracefulCloseTimeMs)
    future.get
    checkClosedState(groupId, numRecords)
  }

  /**
   * Consumer closed while coordinator is unavailable. Close of consumers using group
   * management should complete after commit attempt even though commits fail due to rebalance.
   * Close of consumers using manual assignment should complete with successful commits since a
   * broker is available.
   */
  private def checkCloseWithCoordinatorFailure(numRecords: Int, dynamicGroup: String, manualGroup: String): Unit = {
    val consumer1 = createConsumerAndReceive(dynamicGroup, manualAssign = false, numRecords)
    val consumer2 = createConsumerAndReceive(manualGroup, manualAssign = true, numRecords)

    killBroker(findCoordinator(dynamicGroup))
    killBroker(findCoordinator(manualGroup))

    submitCloseAndValidate(consumer1, Long.MaxValue, None, gracefulCloseTimeMs).get
    submitCloseAndValidate(consumer2, Long.MaxValue, None, gracefulCloseTimeMs).get

    restartDeadBrokers()
    checkClosedState(dynamicGroup, 0)
    checkClosedState(manualGroup, numRecords)
  }

  private def findCoordinator(group: String): Int = {
    val request = new FindCoordinatorRequest.Builder(new FindCoordinatorRequestData()
      .setKeyType(FindCoordinatorRequest.CoordinatorType.GROUP.id)
      .setCoordinatorKeys(util.List.of(group))).build()
    var nodeId = -1
    TestUtils.waitUntilTrue(() => {
      val response = connectAndReceive[FindCoordinatorResponse](request)
      nodeId = response.node.id
      response.error == Errors.NONE
    }, s"Failed to find coordinator for group $group")
    nodeId
  }

  /**
   * Consumer is closed while all brokers are unavailable. Cannot rebalance or commit offsets since
   * there is no coordinator, but close should timeout and return. If close is invoked with a very
   * large timeout, close should timeout after request timeout.
   */
  private def checkCloseWithClusterFailure(numRecords: Int, group1: String, group2: String,
                                           groupProtocol: String): Unit = {
    val consumer1 = createConsumerAndReceive(group1, manualAssign = false, numRecords)

    val requestTimeout = 6000
    if (groupProtocol.equalsIgnoreCase(GroupProtocol.CLASSIC.name)) {
      this.consumerConfig.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "5000")
      this.consumerConfig.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1000")
    }
    this.consumerConfig.setProperty(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeout.toString)
    val consumer2 = createConsumerAndReceive(group2, manualAssign = true, numRecords)

    brokerServers.foreach(server => killBroker(server.config.brokerId))
    val closeTimeout = 2000
    val future1 = submitCloseAndValidate(consumer1, closeTimeout, None, Some(closeTimeout))
    val future2 = submitCloseAndValidate(consumer2, Long.MaxValue, None, Some(requestTimeout))
    future1.get
    future2.get
  }

  /**
    * If we have a running consumer group of size N, configure consumer.group.max.size = N-1 and restart all brokers,
    * the group should be forced to rebalance when it becomes hosted on a Coordinator with the new config.
    * Then, 1 consumer should be left out of the group.
    */
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  @Disabled // TODO: To be re-enabled once we can make it less flaky (KAFKA-13421)
  def testRollingBrokerRestartsWithSmallerMaxGroupSizeConfigDisruptsBigGroup(groupProtocol: String): Unit = {
    val group = "group-max-size-test"
    val topic = "group-max-size-test"
    val maxGroupSize = 2
    val consumerCount = maxGroupSize + 1
    val partitionCount = consumerCount * 2

    this.consumerConfig.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "60000")
    if (groupProtocol.equalsIgnoreCase(GroupProtocol.CLASSIC.name)) {
      this.consumerConfig.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1000")
    }
    this.consumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    val partitions = createTopicPartitions(topic, numPartitions = partitionCount, replicationFactor = brokerCount)

    addConsumersToGroupAndWaitForGroupAssignment(consumerCount, mutable.Buffer[Consumer[Array[Byte], Array[Byte]]](),
      consumerPollers, List[String](topic), partitions, group)

    // roll all brokers with a lesser max group size to make sure coordinator has the new config
    val newConfigs = generateKafkaConfigs(maxGroupSize.toString)
    for (serverIdx <- brokerServers.indices) {
      killBroker(serverIdx)
      val config = newConfigs(serverIdx)
      servers(serverIdx) = createBroker(config, time = brokerTime(config.brokerId))
      restartDeadBrokers()
    }

    def raisedExceptions: Seq[Throwable] = {
      consumerPollers.flatten(_.thrownException)
    }

    // we are waiting for the group to rebalance and one member to get kicked
    TestUtils.waitUntilTrue(() => raisedExceptions.nonEmpty,
      msg = "The remaining consumers in the group could not fetch the expected records", 10000L)

    assertEquals(1, raisedExceptions.size)
    assertTrue(raisedExceptions.head.isInstanceOf[GroupMaxSizeReachedException])
  }

  /**
    * When we have the consumer group max size configured to X, the X+1th consumer trying to join should receive a fatal exception
    */
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testConsumerReceivesFatalExceptionWhenGroupPassesMaxSize(groupProtocol: String): Unit = {
    val group = "fatal-exception-test"
    val topic = "fatal-exception-test"
    this.consumerConfig.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "60000")
    if (groupProtocol.equalsIgnoreCase(GroupProtocol.CLASSIC.name)) {
      this.consumerConfig.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1000")
    }
    this.consumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")

    val partitions = createTopicPartitions(topic, numPartitions = maxGroupSize, replicationFactor = brokerCount)

    // Create N+1 consumers in the same consumer group and assert that the N+1th consumer receives a fatal error when it tries to join the group
    val consumerPollers = mutable.Buffer[ConsumerAssignmentPoller]()
    try {
      addConsumersToGroupAndWaitForGroupAssignment(maxGroupSize, mutable.Buffer[Consumer[Array[Byte], Array[Byte]]](),
        consumerPollers, List[String](topic), partitions, group)
      val (_, rejectedConsumerPollers) = addConsumersToGroup(1,
        mutable.Buffer[Consumer[Array[Byte], Array[Byte]]](), mutable.Buffer[ConsumerAssignmentPoller](), List[String](topic), partitions, group)
      val rejectedConsumer = rejectedConsumerPollers.head
      TestUtils.waitUntilTrue(() => {
        rejectedConsumer.thrownException.isDefined
      }, "Extra consumer did not throw an exception")
      assertTrue(rejectedConsumer.thrownException.get.isInstanceOf[GroupMaxSizeReachedException])

      // assert group continues to live
      producerSend(createProducer(), maxGroupSize * 100, topic, numPartitions = Some(partitions.size))
      TestUtils.waitUntilTrue(() => {
        consumerPollers.forall(p => p.receivedMessages >= 100)
      }, "The consumers in the group could not fetch the expected records", 10000L)
    } finally {
      consumerPollers.foreach(_.shutdown())
    }
  }

  /**
   * Consumer is closed during rebalance. Close should leave group and close
   * immediately if rebalance is in progress. If brokers are not available,
   * close should terminate immediately without sending leave group.
   */
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  def testCloseDuringRebalance(groupProtocol: String): Unit = {
    val topic = "closetest"
    createTopic(topic, 10, brokerCount)
    this.consumerConfig.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "60000")
    if (groupProtocol.equalsIgnoreCase(GroupProtocol.CLASSIC.name)) {
      this.consumerConfig.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1000")
    }
    this.consumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    checkCloseDuringRebalance("group1", topic, executor, brokersAvailableDuringClose = true)
  }

  private def checkCloseDuringRebalance(groupId: String, topic: String, executor: ExecutorService, brokersAvailableDuringClose: Boolean): Unit = {

    def subscribeAndPoll(consumer: Consumer[Array[Byte], Array[Byte]], revokeSemaphore: Option[Semaphore] = None): Future[Any] = {
      executor.submit(() => {
        consumer.subscribe(util.List.of(topic))
        revokeSemaphore.foreach(s => s.release())
          consumer.poll(Duration.ofMillis(500))
        }, 0)
    }

    def waitForRebalance(timeoutMs: Long, future: Future[Any], otherConsumers: Consumer[Array[Byte], Array[Byte]]*): Unit = {
      val startMs = System.currentTimeMillis
      while (System.currentTimeMillis < startMs + timeoutMs && !future.isDone)
          otherConsumers.foreach(consumer => consumer.poll(time.Duration.ofMillis(100L)))
      assertTrue(future.isDone, "Rebalance did not complete in time")
    }

    def createConsumerToRebalance(): Future[Any] = {
      val consumer = createConsumerWithGroupId(groupId)
      val rebalanceSemaphore = new Semaphore(0)
      val future = subscribeAndPoll(consumer, Some(rebalanceSemaphore))
      // Wait for consumer to poll and trigger rebalance
      assertTrue(rebalanceSemaphore.tryAcquire(2000, TimeUnit.MILLISECONDS), "Rebalance not triggered")
      // Rebalance is blocked by other consumers not polling
      assertFalse(future.isDone, "Rebalance completed too early")
      future
    }
    val consumer1 = createConsumerWithGroupId(groupId)
    waitForRebalance(2000, subscribeAndPoll(consumer1))
    val consumer2 = createConsumerWithGroupId(groupId)
    waitForRebalance(2000, subscribeAndPoll(consumer2), consumer1)
    val rebalanceFuture = createConsumerToRebalance()

    // consumer1 should leave group and close immediately even though rebalance is in progress
    val closeFuture1 = submitCloseAndValidate(consumer1, Long.MaxValue, None, gracefulCloseTimeMs)

    // Rebalance should complete without waiting for consumer1 to timeout since consumer1 has left the group
    waitForRebalance(2000, rebalanceFuture, consumer2)

    // Trigger another rebalance and shutdown all brokers
    // This consumer poll() doesn't complete and `tearDown` shuts down the executor and closes the consumer
    createConsumerToRebalance()
    brokerServers.foreach(server => killBroker(server.config.brokerId))

    // consumer2 should close immediately without LeaveGroup request since there are no brokers available
    val closeFuture2 = submitCloseAndValidate(consumer2, Long.MaxValue, None, Some(0))

    // Ensure futures complete to avoid concurrent shutdown attempt during test cleanup
    closeFuture1.get(2000, TimeUnit.MILLISECONDS)
    closeFuture2.get(2000, TimeUnit.MILLISECONDS)
  }

  private def createConsumerAndReceive(groupId: String, manualAssign: Boolean, numRecords: Int): Consumer[Array[Byte], Array[Byte]] = {
    val consumer = createConsumerWithGroupId(groupId)
    val consumerPoller = if (manualAssign)
        subscribeConsumerAndStartPolling(consumer, List(), Set(tp))
      else
        subscribeConsumerAndStartPolling(consumer, List(topic))

    consumerPollers += consumerPoller
    receiveExactRecords(consumerPoller, numRecords)
    consumerPoller.shutdown()
    consumer
  }

  private def receiveExactRecords(consumer: ConsumerAssignmentPoller, numRecords: Int, timeoutMs: Long = 60000): Unit = {
    TestUtils.waitUntilTrue(() => {
      consumer.receivedMessages == numRecords
    }, s"Consumer did not receive expected $numRecords. It received ${consumer.receivedMessages}", timeoutMs)
  }

  private def submitCloseAndValidate(consumer: Consumer[Array[Byte], Array[Byte]],
      closeTimeoutMs: Long, minCloseTimeMs: Option[Long], maxCloseTimeMs: Option[Long]): Future[Any] = {
    executor.submit(() => {
      val closeGraceTimeMs = 2000
      val startMs = System.currentTimeMillis()
      info("Closing consumer with timeout " + closeTimeoutMs + " ms.")
      consumer.close(time.Duration.ofMillis(closeTimeoutMs))
      val timeTakenMs = System.currentTimeMillis() - startMs
      maxCloseTimeMs.foreach { ms =>
        assertTrue(timeTakenMs < ms + closeGraceTimeMs, "Close took too long " + timeTakenMs)
      }
      minCloseTimeMs.foreach { ms =>
        assertTrue(timeTakenMs >= ms, "Close finished too quickly " + timeTakenMs)
      }
      info("consumer.close() completed in " + timeTakenMs + " ms.")
    }, 0)
  }

  private def checkClosedState(groupId: String, committedRecords: Int): Unit = {
    // Check that close was graceful with offsets committed and leave group sent.
    // New instance of consumer should be assigned partitions immediately and should see committed offsets.
    val assignSemaphore = new Semaphore(0)
    val consumer = createConsumerWithGroupId(groupId)
    consumer.subscribe(util.List.of(topic), new ConsumerRebalanceListener {
      def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {
        assignSemaphore.release()
      }
      def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = {
      }})

    TestUtils.waitUntilTrue(() => {
      consumer.poll(time.Duration.ofMillis(100L))
      assignSemaphore.tryAcquire()
    }, "Assignment did not complete on time")

    if (committedRecords > 0)
      assertEquals(committedRecords, consumer.committed(java.util.Set.of(tp)).get(tp).offset)
    consumer.close()
  }

  private class BounceBrokerScheduler(val numIters: Int) extends ShutdownableThread("daemon-bounce-broker", false) {
    private var iter: Int = 0

    override def doWork(): Unit = {
      killRandomBroker()
      Thread.sleep(500)
      restartDeadBrokers()

      iter += 1
      if (iter == numIters)
        initiateShutdown()
      else
        Thread.sleep(500)
    }
  }

  private def createTopicPartitions(topic: String, numPartitions: Int, replicationFactor: Int,
                                    topicConfig: Properties = new Properties): Set[TopicPartition] = {
    createTopic(topic, numPartitions = numPartitions, replicationFactor = replicationFactor, topicConfig = topicConfig)
    Range(0, numPartitions).map(part => new TopicPartition(topic, part)).toSet
  }

  private def producerSend(producer: KafkaProducer[Array[Byte], Array[Byte]],
                           numRecords: Int,
                           topic: String = this.topic,
                           numPartitions: Option[Int] = None): Unit = {
    var partitionIndex = 0
    def getPartition: Int = {
      numPartitions match {
        case Some(partitions) =>
          val nextPart = partitionIndex % partitions
          partitionIndex += 1
          nextPart
        case None => part
      }
    }

    val futures = (0 until numRecords).map { i =>
      producer.send(new ProducerRecord(topic, getPartition, i.toString.getBytes, i.toString.getBytes))
    }
    futures.map(_.get)
  }

}
