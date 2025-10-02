package kafka.server;

import kafka.utils.Logging;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.DisklessCommitRequestData;
import org.apache.kafka.common.message.DisklessCommitResponseData;
import org.apache.kafka.common.metadata.DisklessBatchRecord;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.common.RequestLocal;
import org.apache.kafka.storage.internals.log.AppendOrigin;
import org.apache.kafka.storage.internals.log.UnifiedLog;
import org.apache.kafka.storage.internals.log.VerificationGuard;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.jdk.CollectionConverters;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class DisklessPartition {
    private static final Logger LOG = LoggerFactory.getLogger(DisklessPartition.class);

    private final Time time;
    private final ReplicaManager replicaManager;
    private final TopicIdPartition topicIdPartition;

    private final java.sql.Connection dbConn;

    public DisklessPartition(final Time time,
                             final ReplicaManager replicaManager,
                             final TopicIdPartition topicIdPartition) {
        this.time = time;
        this.replicaManager = replicaManager;
        this.topicIdPartition = topicIdPartition;

        final Option<UnifiedLog> logOpt = replicaManager.getLog(topicIdPartition.topicPartition());
        if (logOpt.isEmpty()) {
            throw new RuntimeException("Log for " + topicIdPartition + " not found");
        }
        final UnifiedLog log = logOpt.get();

        // TODO necessary precautions regarding creating SQLite DB.

        try {
            final String dbUrl = "jdbc:sqlite:" + log.dir().toPath().resolve("state.db").toString();
            dbConn = java.sql.DriverManager.getConnection(dbUrl);
            try (final java.sql.Statement stmt = dbConn.createStatement()) {
                stmt.executeUpdate(
                    "CREATE TABLE IF NOT EXISTS batches (" +
                        "batch_id INTEGER PRIMARY KEY AUTOINCREMENT," +
                        "magic INTEGER NOT NULL," +
                        "base_offset BIGINT NOT NULL," +
                        "last_offset BIGINT NOT NULL," +
                        "object_name TEXT," +
                        "byte_offset BIGINT NOT NULL," +
                        "byte_size BIGINT NOT NULL," +
                        "timestamp_type INTEGER NOT NULL," +
                        "log_append_timestamp BIGINT," +
                        "batch_max_timestamp BIGINT" +
                        ")"
                );
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize SQLite DB for " + topicIdPartition, e);
        }
    }

    CompletableFuture<DisklessCommitResponseData.PartitionDisklessCommitResponse> commit(
        final String objectName,
        final DisklessCommitRequestData.PartitionDisklessCommitData partitionData
    ) {
        final long now = time.milliseconds();

        // TODO make singleton for speedup
        final ObjectSerializationCache cache = new ObjectSerializationCache();

        final SimpleRecord[] recordsArray = new SimpleRecord[partitionData.batches().size()];
        for (int i = 0; i < partitionData.batches().size(); i++) {
            final var batch = partitionData.batches().get(i);
            final DisklessBatchRecord batchRecord = new DisklessBatchRecord()
                .setMagic(batch.magic())
                .setBaseOffset(batch.baseOffset())
                .setLastOffset(batch.lastOffset())
                .setObjectName(objectName)
                .setByteOffset(batch.byteOffset())
                .setByteSize(batch.byteSize())
                .setTimestampType(batch.timestampType())
                .setLogAppendTimestamp(now)
                .setBatchMaxTimestamp(batch.batchMaxTimestamp());
            final int size = batchRecord.size(cache, (short) 0);
            final ByteBuffer buffer = ByteBuffer.allocate(size);
            final ByteBufferAccessor accessor = new ByteBufferAccessor(buffer);
            batchRecord.write(accessor, cache, (short) 0);
            recordsArray[i] = new SimpleRecord(now, buffer.array());
        }
        final var records = MemoryRecords.withRecords(Compression.NONE, recordsArray);

        // TODO add locking

        final CompletableFuture<DisklessCommitResponseData.PartitionDisklessCommitResponse> result =
            new CompletableFuture<>();
        replicaManager.appendRecords(
            30000, (short) -1, true, AppendOrigin.COORDINATOR,
            CollectionConverters.MapHasAsScala(Map.of(topicIdPartition, records)).asScala(),
            r -> {
                final DisklessCommitResponseData.PartitionDisklessCommitResponse response;
                final ProduceResponse.PartitionResponse partitionResponse = r.apply(topicIdPartition);
                if (partitionResponse.error == Errors.NONE) {
                    try {
                        for (final DisklessCommitRequestData.BatchDisklessCommitData batch : partitionData.batches()) {
                            final String insertSql = "INSERT INTO batches (object_name, magic, base_offset, last_offset, byte_offset, byte_size, timestamp_type, log_append_timestamp, batch_max_timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
                            try (var pstmt = dbConn.prepareStatement(insertSql)) {
                                pstmt.setString(1, objectName);
                                pstmt.setInt(2, batch.magic());
                                pstmt.setLong(3, batch.baseOffset());
                                pstmt.setLong(4, batch.lastOffset());
                                pstmt.setLong(5, batch.byteOffset());
                                pstmt.setInt(6, batch.byteSize());
                                pstmt.setShort(7, batch.timestampType());
                                pstmt.setLong(8, now);
                                pstmt.setLong(9, batch.batchMaxTimestamp());
                                pstmt.executeUpdate();
                            }
                        }
                        response = new DisklessCommitResponseData.PartitionDisklessCommitResponse()
                            .setPartition(topicIdPartition.partition())
                            .setErrorCode((short) 0)
                            .setErrorMessage(null);
                    } catch (Exception dbEx) {
                        throw new RuntimeException("Failed to insert batches into DB for " + topicIdPartition, dbEx);
                    }
                } else {
                    LOG.error("Error appending to log: " + partitionResponse.error + " " + partitionResponse.errorMessage);
                    response = new DisklessCommitResponseData.PartitionDisklessCommitResponse()
                        .setPartition(topicIdPartition.partition())
                        .setErrorCode(partitionResponse.error.code())
                        .setErrorMessage(partitionResponse.errorMessage);
                }
                result.complete(response);

                System.out.println(r);
                return null;
            },
            r -> null,
            RequestLocal.noCaching(),
            scala.collection.mutable.HashMap.<TopicPartition, VerificationGuard>newBuilder().result()
        );

        return result;
    }
}
