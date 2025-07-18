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

package kafka.log

import java.io.File
import java.util.Properties
import kafka.utils.TestUtils
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.record.{ControlRecordType, EndTransactionMarker, FileRecords, MemoryRecords, RecordBatch, SimpleRecord}
import org.apache.kafka.common.utils.{Time, Utils}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse}

import java.nio.file.Files
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.coordinator.transaction.TransactionLogConfig
import org.apache.kafka.server.common.RequestLocal
import org.apache.kafka.server.config.ServerLogConfigs
import org.apache.kafka.server.log.remote.storage.RemoteLogManager
import org.apache.kafka.server.storage.log.FetchIsolation
import org.apache.kafka.server.util.Scheduler
import org.apache.kafka.storage.internals.log.LogConfig.{DEFAULT_REMOTE_LOG_COPY_DISABLE_CONFIG, DEFAULT_REMOTE_LOG_DELETE_ON_DISABLE_CONFIG}
import org.apache.kafka.storage.internals.log.{AbortedTxn, AppendOrigin, FetchDataInfo, LazyIndex, LogAppendInfo, LogConfig, LogDirFailureChannel, LogFileUtils, LogOffsetsListener, LogSegment, ProducerStateManager, ProducerStateManagerConfig, TransactionIndex, VerificationGuard, UnifiedLog}
import org.apache.kafka.storage.log.metrics.BrokerTopicStats

import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters.RichOption

object LogTestUtils {
  /**
    *  Create a segment with the given base offset
    */
  def createSegment(offset: Long,
                    logDir: File,
                    indexIntervalBytes: Int = 10,
                    time: Time = Time.SYSTEM): LogSegment = {
    val ms = FileRecords.open(LogFileUtils.logFile(logDir, offset))
    val idx = LazyIndex.forOffset(LogFileUtils.offsetIndexFile(logDir, offset), offset, 1000)
    val timeIdx = LazyIndex.forTime(LogFileUtils.timeIndexFile(logDir, offset), offset, 1500)
    val txnIndex = new TransactionIndex(offset, LogFileUtils.transactionIndexFile(logDir, offset, ""))

    new LogSegment(ms, idx, timeIdx, txnIndex, offset, indexIntervalBytes, 0, time)
  }

  def createLogConfig(segmentMs: Long = LogConfig.DEFAULT_SEGMENT_MS,
                      segmentBytes: Int = LogConfig.DEFAULT_SEGMENT_BYTES,
                      retentionMs: Long = LogConfig.DEFAULT_RETENTION_MS,
                      localRetentionMs: Long = LogConfig.DEFAULT_LOCAL_RETENTION_MS,
                      retentionBytes: Long = ServerLogConfigs.LOG_RETENTION_BYTES_DEFAULT,
                      localRetentionBytes: Long = LogConfig.DEFAULT_LOCAL_RETENTION_BYTES,
                      segmentJitterMs: Long = LogConfig.DEFAULT_SEGMENT_JITTER_MS,
                      cleanupPolicy: String = ServerLogConfigs.LOG_CLEANUP_POLICY_DEFAULT,
                      maxMessageBytes: Int = ServerLogConfigs.MAX_MESSAGE_BYTES_DEFAULT,
                      indexIntervalBytes: Int = ServerLogConfigs.LOG_INDEX_INTERVAL_BYTES_DEFAULT,
                      segmentIndexBytes: Int = ServerLogConfigs.LOG_INDEX_SIZE_MAX_BYTES_DEFAULT,
                      fileDeleteDelayMs: Long = ServerLogConfigs.LOG_DELETE_DELAY_MS_DEFAULT,
                      remoteLogStorageEnable: Boolean = LogConfig.DEFAULT_REMOTE_STORAGE_ENABLE,
                      remoteLogCopyDisable: Boolean = DEFAULT_REMOTE_LOG_COPY_DISABLE_CONFIG,
                      remoteLogDeleteOnDisable: Boolean = DEFAULT_REMOTE_LOG_DELETE_ON_DISABLE_CONFIG): LogConfig = {
    val logProps = new Properties()
    logProps.put(TopicConfig.SEGMENT_MS_CONFIG, segmentMs: java.lang.Long)
    logProps.put(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, segmentBytes: Integer)
    logProps.put(TopicConfig.RETENTION_MS_CONFIG, retentionMs: java.lang.Long)
    logProps.put(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG, localRetentionMs: java.lang.Long)
    logProps.put(TopicConfig.RETENTION_BYTES_CONFIG, retentionBytes: java.lang.Long)
    logProps.put(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG, localRetentionBytes: java.lang.Long)
    logProps.put(TopicConfig.SEGMENT_JITTER_MS_CONFIG, segmentJitterMs: java.lang.Long)
    logProps.put(TopicConfig.CLEANUP_POLICY_CONFIG, cleanupPolicy)
    logProps.put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, maxMessageBytes: Integer)
    logProps.put(TopicConfig.INDEX_INTERVAL_BYTES_CONFIG, indexIntervalBytes: Integer)
    logProps.put(TopicConfig.SEGMENT_INDEX_BYTES_CONFIG, segmentIndexBytes: Integer)
    logProps.put(TopicConfig.FILE_DELETE_DELAY_MS_CONFIG, fileDeleteDelayMs: java.lang.Long)
    logProps.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, remoteLogStorageEnable: java.lang.Boolean)
    logProps.put(TopicConfig.REMOTE_LOG_COPY_DISABLE_CONFIG, remoteLogCopyDisable: java.lang.Boolean)
    logProps.put(TopicConfig.REMOTE_LOG_DELETE_ON_DISABLE_CONFIG, remoteLogDeleteOnDisable: java.lang.Boolean)
    new LogConfig(logProps)
  }

  def createLog(dir: File,
                config: LogConfig,
                brokerTopicStats: BrokerTopicStats,
                scheduler: Scheduler,
                time: Time,
                logStartOffset: Long = 0L,
                recoveryPoint: Long = 0L,
                maxTransactionTimeoutMs: Int = 5 * 60 * 1000,
                producerStateManagerConfig: ProducerStateManagerConfig = new ProducerStateManagerConfig(TransactionLogConfig.PRODUCER_ID_EXPIRATION_MS_DEFAULT, false),
                producerIdExpirationCheckIntervalMs: Int = TransactionLogConfig.PRODUCER_ID_EXPIRATION_MS_DEFAULT,
                lastShutdownClean: Boolean = true,
                topicId: Option[Uuid] = None,
                numRemainingSegments: ConcurrentMap[String, Integer] = new ConcurrentHashMap[String, Integer],
                remoteStorageSystemEnable: Boolean = false,
                remoteLogManager: Option[RemoteLogManager] = None,
                logOffsetsListener: LogOffsetsListener = LogOffsetsListener.NO_OP_OFFSETS_LISTENER): UnifiedLog = {
    UnifiedLog.create(
      dir,
      config,
      logStartOffset,
      recoveryPoint,
      scheduler,
      brokerTopicStats,
      time,
      maxTransactionTimeoutMs,
      producerStateManagerConfig,
      producerIdExpirationCheckIntervalMs,
      new LogDirFailureChannel(10),
      lastShutdownClean,
      topicId.toJava,
      numRemainingSegments,
      remoteStorageSystemEnable,
      logOffsetsListener
    )
  }

  /**
   * Check if the given log contains any segment with records that cause offset overflow.
   * @param log Log to check
   * @return true if log contains at least one segment with offset overflow; false otherwise
   */
  def hasOffsetOverflow(log: UnifiedLog): Boolean = firstOverflowSegment(log).isDefined

  def firstOverflowSegment(log: UnifiedLog): Option[LogSegment] = {
    def hasOverflow(baseOffset: Long, batch: RecordBatch): Boolean =
      batch.lastOffset > baseOffset + Int.MaxValue || batch.baseOffset < baseOffset

    for (segment <- log.logSegments.asScala) {
      val overflowBatch = segment.log.batches.asScala.find(batch => hasOverflow(segment.baseOffset, batch))
      if (overflowBatch.isDefined)
        return Some(segment)
    }
    None
  }

  def rawSegment(logDir: File, baseOffset: Long): FileRecords =
    FileRecords.open(LogFileUtils.logFile(logDir, baseOffset))

  /**
   * Initialize the given log directory with a set of segments, one of which will have an
   * offset which overflows the segment
   */
  def initializeLogDirWithOverflowedSegment(logDir: File): Unit = {
    def writeSampleBatches(baseOffset: Long, segment: FileRecords): Long = {
      def record(offset: Long) = {
        val data = offset.toString.getBytes
        new SimpleRecord(data, data)
      }

      segment.append(MemoryRecords.withRecords(baseOffset, Compression.NONE, 0,
        record(baseOffset)))
      segment.append(MemoryRecords.withRecords(baseOffset + 1, Compression.NONE, 0,
        record(baseOffset + 1),
        record(baseOffset + 2)))
      segment.append(MemoryRecords.withRecords(baseOffset + Int.MaxValue - 1, Compression.NONE, 0,
        record(baseOffset + Int.MaxValue - 1)))
      // Need to create the offset files explicitly to avoid triggering segment recovery to truncate segment.
      Files.createFile(LogFileUtils.offsetIndexFile(logDir, baseOffset).toPath)
      Files.createFile(LogFileUtils.timeIndexFile(logDir, baseOffset).toPath)
      baseOffset + Int.MaxValue
    }

    def writeNormalSegment(baseOffset: Long): Long = {
      val segment = rawSegment(logDir, baseOffset)
      try writeSampleBatches(baseOffset, segment)
      finally segment.close()
    }

    def writeOverflowSegment(baseOffset: Long): Long = {
      val segment = rawSegment(logDir, baseOffset)
      try {
        val nextOffset = writeSampleBatches(baseOffset, segment)
        writeSampleBatches(nextOffset, segment)
      } finally segment.close()
    }

    // We create three segments, the second of which contains offsets which overflow
    var nextOffset = 0L
    nextOffset = writeNormalSegment(nextOffset)
    nextOffset = writeOverflowSegment(nextOffset)
    writeNormalSegment(nextOffset)
  }

  /* extract all the keys from a log */
  def keysInLog(log: UnifiedLog): Iterable[Long] = {
    for (logSegment <- log.logSegments.asScala;
         batch <- logSegment.log.batches.asScala if !batch.isControlBatch;
         record <- batch.asScala if record.hasValue && record.hasKey)
      yield TestUtils.readString(record.key).toLong
  }

  def recoverAndCheck(logDir: File, config: LogConfig, expectedKeys: Iterable[Long], brokerTopicStats: BrokerTopicStats, time: Time, scheduler: Scheduler): UnifiedLog = {
    // Recover log file and check that after recovery, keys are as expected
    // and all temporary files have been deleted
    val recoveredLog = createLog(logDir, config, brokerTopicStats, scheduler, time, lastShutdownClean = false)
    time.sleep(config.fileDeleteDelayMs + 1)
    for (file <- logDir.listFiles) {
      assertFalse(file.getName.endsWith(LogFileUtils.DELETED_FILE_SUFFIX), "Unexpected .deleted file after recovery")
      assertFalse(file.getName.endsWith(UnifiedLog.CLEANED_FILE_SUFFIX), "Unexpected .cleaned file after recovery")
      assertFalse(file.getName.endsWith(UnifiedLog.SWAP_FILE_SUFFIX), "Unexpected .swap file after recovery")
    }
    assertEquals(expectedKeys, keysInLog(recoveredLog))
    assertFalse(hasOffsetOverflow(recoveredLog))
    recoveredLog
  }

  def appendEndTxnMarkerAsLeader(log: UnifiedLog,
                                 producerId: Long,
                                 producerEpoch: Short,
                                 controlType: ControlRecordType,
                                 timestamp: Long,
                                 coordinatorEpoch: Int = 0,
                                 leaderEpoch: Int = 0): LogAppendInfo = {
    val records = endTxnRecords(controlType, producerId, producerEpoch,
      coordinatorEpoch = coordinatorEpoch, timestamp = timestamp)
    log.appendAsLeader(records, leaderEpoch, AppendOrigin.COORDINATOR, RequestLocal.noCaching(), VerificationGuard.SENTINEL)
  }

  private def endTxnRecords(controlRecordType: ControlRecordType,
                            producerId: Long,
                            epoch: Short,
                            offset: Long = 0L,
                            coordinatorEpoch: Int,
                            partitionLeaderEpoch: Int = 0,
                            timestamp: Long): MemoryRecords = {
    val marker = new EndTransactionMarker(controlRecordType, coordinatorEpoch)
    MemoryRecords.withEndTransactionMarker(offset, timestamp, partitionLeaderEpoch, producerId, epoch, marker)
  }

  def readLog(log: UnifiedLog,
              startOffset: Long,
              maxLength: Int,
              isolation: FetchIsolation = FetchIsolation.LOG_END,
              minOneMessage: Boolean = true): FetchDataInfo = {
    log.read(startOffset, maxLength, isolation, minOneMessage)
  }

  def allAbortedTransactions(log: UnifiedLog): Iterable[AbortedTxn] =
    log.logSegments.asScala.flatMap(_.txnIndex.allAbortedTxns.asScala)

  def deleteProducerSnapshotFiles(logDir: File): Unit = {
    val files = logDir.listFiles.filter(f => f.isFile && f.getName.endsWith(LogFileUtils.PRODUCER_SNAPSHOT_FILE_SUFFIX))
    files.foreach(Utils.delete)
  }

  def listProducerSnapshotOffsets(logDir: File): Seq[Long] =
    ProducerStateManager.listSnapshotFiles(logDir).asScala.map(_.offset).sorted.toSeq

  def appendNonTransactionalAsLeader(log: UnifiedLog, numRecords: Int): Unit = {
    val simpleRecords = (0 until numRecords).map { seq =>
      new SimpleRecord(s"$seq".getBytes)
    }
    val records = MemoryRecords.withRecords(Compression.NONE, simpleRecords: _*)
    log.appendAsLeader(records, 0)
  }

  def appendTransactionalAsLeader(log: UnifiedLog,
                                  producerId: Long,
                                  producerEpoch: Short,
                                  time: Time): Int => Unit = {
    appendIdempotentAsLeader(log, producerId, producerEpoch, time, isTransactional = true)
  }

  def appendIdempotentAsLeader(log: UnifiedLog,
                               producerId: Long,
                               producerEpoch: Short,
                               time: Time,
                               isTransactional: Boolean = false): Int => Unit = {
    var sequence = 0
    numRecords: Int => {
      val simpleRecords = (sequence until sequence + numRecords).map { seq =>
        new SimpleRecord(time.milliseconds(), s"$seq".getBytes)
      }

      val records = if (isTransactional) {
        MemoryRecords.withTransactionalRecords(Compression.NONE, producerId,
          producerEpoch, sequence, simpleRecords: _*)
      } else {
        MemoryRecords.withIdempotentRecords(Compression.NONE, producerId,
          producerEpoch, sequence, simpleRecords: _*)
      }

      log.appendAsLeader(records, 0)
      sequence += numRecords
    }
  }
}
