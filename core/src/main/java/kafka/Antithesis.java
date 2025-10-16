package kafka;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.common.RequestLocal;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.util.KafkaScheduler;
import org.apache.kafka.storage.internals.log.FetchDataInfo;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.LogDirFailureChannel;
import org.apache.kafka.storage.internals.log.ProducerStateManagerConfig;
import org.apache.kafka.storage.internals.log.UnifiedLog;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import io.aiven.inkless.cache.FixedBlockAlignment;
import io.aiven.inkless.cache.ObjectCache;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.common.SharedState;
import io.aiven.inkless.config.InklessConfig;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.CreateTopicAndPartitionsRequest;
import io.aiven.inkless.control_plane.MetadataView;
import io.aiven.inkless.generated.CacheKey;
import io.aiven.inkless.generated.FileExtent;
import io.aiven.inkless.log.MaterializedLogManager;
import io.aiven.inkless.produce.AppendHandler;

import com.antithesis.sdk.Assert;
import com.antithesis.sdk.Lifecycle;
import com.antithesis.sdk.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Antithesis {
    private static final Logger LOG = LoggerFactory.getLogger(Antithesis.class);

    private static final Time TIME = Time.SYSTEM;

    private static Uuid TOPIC_ID_0 = new Uuid(101, 201);
    private static Uuid TOPIC_ID_1 = new Uuid(102, 202);
    private static String TOPIC_NAME_0 = "topic0";
    private static String TOPIC_NAME_1 = "topic1";
    private static TopicIdPartition T0P0 = new TopicIdPartition(TOPIC_ID_0, 0, TOPIC_NAME_0);
    private static TopicIdPartition T0P1 = new TopicIdPartition(TOPIC_ID_0, 1, TOPIC_NAME_0);
    private static TopicIdPartition T1P0 = new TopicIdPartition(TOPIC_ID_1, 0, TOPIC_NAME_1);

    private static final LogConfig LOG_CONFIG = LogConfig.fromProps(Map.of(), new Properties());
    private static final LogDirFailureChannel LOG_DIR_FAILURE_CHANNEL =
        new LogDirFailureChannel(1);
    private static final ProducerStateManagerConfig PRODUCER_STATE_MANAGER_CONFIG =
        new ProducerStateManagerConfig(60000, false);

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        final String pgConnectionString = args[0];
        final String s3Endpoint = args[1];
        final String materializationDir = args[2];

        final BrokerTopicStats brokerTopicStats = new BrokerTopicStats();

        final InklessConfig config = inklessConfig(pgConnectionString, s3Endpoint, materializationDir);
        final MetadataView metadataView = new TestMetadataView();

        final ControlPlane controlPlane = ControlPlane.create(config, TIME);
        controlPlane.createTopicAndPartitions(Set.of(
            new CreateTopicAndPartitionsRequest(TOPIC_ID_0, TOPIC_NAME_0, 2),
            new CreateTopicAndPartitionsRequest(TOPIC_ID_1, TOPIC_NAME_1, 1)
        ));

        final ObjectKeyCreator objectKeyCreator = ObjectKey.creator(config.objectKeyPrefix(), config.objectKeyLogPrefixMasked());

        final FixedBlockAlignment keyAlignmentStrategy = new FixedBlockAlignment(0);

        final ObjectCache objectCache = new TestObjectCache();

        final SharedState sharedState = new SharedState(
            TIME, 0, config, metadataView, controlPlane, objectKeyCreator, keyAlignmentStrategy,
            objectCache, brokerTopicStats, () -> LogConfig.fromProps(Map.of(), new Properties())
        );
        final AppendHandler handler = new AppendHandler(sharedState);

        Lifecycle.setupComplete(null);

        LOG.info("Starting materialization");
        final MaterializedLogManager materializedLogManager = new MaterializedLogManager(sharedState);
        materializedLogManager.startReplica(T0P0);
        materializedLogManager.startReplica(T0P1);
        materializedLogManager.startReplica(T1P0);

        LOG.info("Letting everything settle down...");
        Thread.sleep(3000);

        LOG.info("Producing test values");
        final Map<TopicIdPartition, List<RecordWithOffset>> expectedRecords =
            produceRecords(handler, randomPositiveInt(50) + 5, true);

        LOG.info("Letting everything settle down...");
        Thread.sleep(10000);

        brokerTopicStats.close();
        materializedLogManager.shutdown();
        handler.close();
        controlPlane.close();

        LOG.info("Checking materialized records");
        final KafkaScheduler scheduler = new KafkaScheduler(1, true);
        final Path materializationPath = Path.of(materializationDir);
        checkRecords(T0P0, expectedRecords.get(T0P0), materializationPath, scheduler);
        checkRecords(T0P1, expectedRecords.get(T0P1), materializationPath, scheduler);
        checkRecords(T1P0, expectedRecords.get(T1P0), materializationPath, scheduler);
        scheduler.shutdown();

        LOG.info("Test finished");
    }

    private static InklessConfig inklessConfig(final String pgConnectionString,
                                               final String s3Endpoint,
                                               final String materializationDir) {
        final Map<String, Object> inklessConfigProps = new HashMap<>();
        inklessConfigProps.put("control.plane.class", "io.aiven.inkless.control_plane.postgres.PostgresControlPlane");
        inklessConfigProps.put("control.plane.connection.string", pgConnectionString);
        inklessConfigProps.put("control.plane.username", "admin");
        inklessConfigProps.put("control.plane.password", "admin");
        inklessConfigProps.put("storage.backend.class", "io.aiven.inkless.storage_backend.s3.S3Storage");
        inklessConfigProps.put("storage.s3.bucket.name", "inkless");
        inklessConfigProps.put("storage.s3.region", "us-east-1");
        inklessConfigProps.put("storage.s3.endpoint.url", s3Endpoint);
        inklessConfigProps.put("storage.aws.access.key.id", "minioadmin");
        inklessConfigProps.put("storage.aws.secret.access.key", "minioadmin");
        inklessConfigProps.put("storage.s3.path.style.access.enabled", "true");
        inklessConfigProps.put("produce.commit.interval.ms", "100");  // speed up things
        inklessConfigProps.put("materialization.directory", materializationDir);
        inklessConfigProps.put("materialization.segment.bytes", 2 * 1024 * 1024);
        return new InklessConfig(inklessConfigProps);
    }

    private static Map<TopicIdPartition, List<RecordWithOffset>> produceRecords(
        final AppendHandler handler, final long totalRounds, final boolean includeFirstBatchInResult
    ) throws ExecutionException, InterruptedException {
        final Map<TopicIdPartition, List<RecordWithOffset>> result = new HashMap<>(Map.of(
            T0P0, new ArrayList<>(),
            T0P1, new ArrayList<>(),
            T1P0, new ArrayList<>()
        ));
        for (int round = 0; round < totalRounds; round++) {
            final int countT0p0 = round % 3 + 1;
            final int countT0p1 = (round + 1) % 3 + 1;
            final int countT0p2 = (round + 2) % 3 + 1;
            final SimpleRecord[] recordArrT0p0 = createRecords(10 * round + 1, countT0p0);
            final SimpleRecord[] recordArrT0p1 = createRecords(10 * round + 2, countT0p1);
            final SimpleRecord[] recordArrT1p0 = createRecords(10 * round + 3, countT0p2);
            final MemoryRecords recordsT0p0 = MemoryRecords.withRecords(Compression.NONE, recordArrT0p0);
            final MemoryRecords recordsT0p1 = MemoryRecords.withRecords(Compression.NONE, recordArrT0p1);
            final MemoryRecords recordsT0p2 = MemoryRecords.withRecords(Compression.NONE, recordArrT1p0);
            final var future = handler.handle(Map.of(
                T0P0, recordsT0p0,
                T0P1, recordsT0p1,
                T1P0, recordsT0p2
            ), RequestLocal.noCaching());
            final var response = future.get();

            final var responseT0p0 = response.get(T0P0);
            final var responseT0p1 = response.get(T0P1);
            final var responseT1p0 = response.get(T1P0);
            checkPartitionResponse(responseT0p0);
            checkPartitionResponse(responseT0p1);
            checkPartitionResponse(responseT1p0);

            LOG.debug("In {} batch starts with {} and has {} records", T0P0, responseT0p0.baseOffset, recordArrT0p0.length);
            LOG.debug("In {} batch starts with {} and has {} records", T0P1, responseT0p1.baseOffset, recordArrT0p1.length);
            LOG.debug("In {} batch starts with {} and has {} records", T1P0, responseT1p0.baseOffset, recordArrT1p0.length);
            if (round > 0 || includeFirstBatchInResult) {
                for (int i = 0; i < recordArrT0p0.length; i++) {
                    result.get(T0P0).add(new RecordWithOffset(recordArrT0p0[i], responseT0p0.baseOffset + i));
                }
                for (int i = 0; i < recordArrT0p1.length; i++) {
                    result.get(T0P1).add(new RecordWithOffset(recordArrT0p1[i], responseT0p1.baseOffset + i));
                }
                for (int i = 0; i < recordArrT1p0.length; i++) {
                    result.get(T1P0).add(new RecordWithOffset(recordArrT1p0[i], responseT1p0.baseOffset + i));
                }
            }
        }

        if (result.get(T0P0).size() == 0
            || result.get(T0P1).size() == 0
            || result.get(T1P0).size() == 0) {
            throw new RuntimeException();
        }
        return result;
    }

    private static void checkPartitionResponse(final ProduceResponse.PartitionResponse response) {
        final boolean condition = response.error == Errors.NONE;
        Assert.always(condition, "No produce errors", null);
        if (!condition) {
            throw new AssertionError();
        }
    }

    private static SimpleRecord[] createRecords(final int round, final int count) {
        final SimpleRecord[] records = new SimpleRecord[count];
        for (int i = 0; i < count; i++) {
            final long idLong = (long) round * 100000000 + count;
            final String id = Long.toBinaryString(idLong).repeat(1000);
            final long timestamp = idLong;
            final byte[] key = id.substring(1).getBytes(StandardCharsets.UTF_8);
            final byte[] value = id.getBytes(StandardCharsets.UTF_8);
            records[i] = new SimpleRecord(timestamp, key, value);
        }
        return records;
    }

    private static void checkRecords(final TopicIdPartition tidp,
                                     final List<RecordWithOffset> expectedRecords,
                                     final Path materializationPath,
                                     final KafkaScheduler scheduler) throws IOException {
        LOG.info("Checking records for {}", tidp);

        final Path partitionDir = materializationPath.resolve(String.format("%s-%d", tidp.topic(), tidp.partition()));
        final UnifiedLog log = UnifiedLog.create(
            partitionDir.toFile(),
            LOG_CONFIG,
            0L,
            0L,
            scheduler,
            new BrokerTopicStats(),
            TIME,
            1,
            PRODUCER_STATE_MANAGER_CONFIG,
            1,
            LOG_DIR_FAILURE_CHANNEL,
            true,
            Optional.of(tidp.topicId())
        );

        long readOffset = log.logStartOffset();
        int recordCounter = 0;
        do {
            final FetchDataInfo fetchDataInfo = log.read(readOffset, 10 * 1024 * 1024, FetchIsolation.LOG_END, true);
            final var batchIterator = fetchDataInfo.records.batchIterator();
            if (!batchIterator.hasNext()) {
                break;
            }
            while (batchIterator.hasNext()) {
                final var batch = batchIterator.next();
                for (final Record record : batch) {
                    final RecordWithOffset expectedRecord = expectedRecords.get(recordCounter);

                    assertRecordMatch(record.offset() == expectedRecord.offset);
                    assertRecordMatch(record.key().equals(expectedRecord.record().key()));
                    assertRecordMatch(record.value().equals(expectedRecord.record().value()));
                    recordCounter++;
                    readOffset = record.offset() + 1;
                }
            }
        } while (true);

        assertRecordMatch(expectedRecords.size() == recordCounter);
        log.close();
    }

    private record RecordWithOffset(SimpleRecord record,
                                    long offset) {
    }

    private static class TestMetadataView implements MetadataView {
        @Override
        public Map<String, Object> getDefaultConfig() {
            return Map.of();
        }

        @Override
        public Iterable<Node> getAliveBrokerNodes(final ListenerName listenerName) {
            return null;
        }

        @Override
        public Integer getBrokerCount() {
            return 1;
        }

        @Override
        public Uuid getTopicId(final String topicName) {
            if (topicName.equals(TOPIC_NAME_0)) {
                return TOPIC_ID_0;
            } else if (topicName.equals(TOPIC_NAME_1)) {
                return TOPIC_ID_1;
            }
            return null;
        }

        @Override
        public boolean isDisklessTopic(final String topicName) {
            return true;
        }

        @Override
        public Properties getTopicConfig(final String topicName) {
            return new Properties();
        }

        @Override
        public Set<TopicIdPartition> getDisklessTopicPartitions() {
            return Set.of(T0P0, T0P1, T1P0);
        }
    }

    private static class TestObjectCache implements ObjectCache {
        @Override
        public void close() throws IOException {

        }

        @Override
        public FileExtent get(final CacheKey key) {
            return null;
        }

        @Override
        public void put(final CacheKey key, final FileExtent value) {

        }

        @Override
        public boolean remove(final CacheKey key) {
            return false;
        }

        @Override
        public long size() {
            return 0;
        }
    }

    private static int randomPositiveInt(final int max) {
        return Math.toIntExact(Math.abs(Random.getRandom()) % (max + 1));
    }

    private static void assertRecordMatch(final boolean condition) {
        Assert.always(condition, "Records match", null);
        if (!condition) {
            throw new AssertionError();
        }
    }
}
