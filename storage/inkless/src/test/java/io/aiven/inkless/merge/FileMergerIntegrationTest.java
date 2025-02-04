// Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.merge;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.aiven.inkless.common.SharedState;
import io.aiven.inkless.config.InklessConfig;
import io.aiven.inkless.consume.FetchInterceptor;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.CreateTopicAndPartitionsRequest;
import io.aiven.inkless.control_plane.DeleteFilesRequest;
import io.aiven.inkless.control_plane.FileToDelete;
import io.aiven.inkless.control_plane.FindBatchRequest;
import io.aiven.inkless.control_plane.FindBatchResponse;
import io.aiven.inkless.control_plane.InMemoryControlPlane;
import io.aiven.inkless.control_plane.MetadataView;
import io.aiven.inkless.produce.AppendInterceptor;
import io.aiven.inkless.produce.WriterTestUtils;
import io.aiven.inkless.storage_backend.s3.S3Storage;
import io.aiven.inkless.test_utils.S3TestContainer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.S3Object;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
@Testcontainers
@Tag("integration")
class FileMergerIntegrationTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileMergerIntegrationTest.class);

    @Container
    static final LocalStackContainer LOCALSTACK = S3TestContainer.localstack();

    static final int BROKER_ID = 1;

    static final String TOPIC_0 = "topic0";
    static final String TOPIC_1 = "topic1";
    static final Uuid TOPIC_ID_0 = new Uuid(0, 1);
    static final Uuid TOPIC_ID_1 = new Uuid(0, 2);
    static final Map<String, Uuid> TOPICS = Map.of(
        TOPIC_0, TOPIC_ID_0,
        TOPIC_1, TOPIC_ID_1
    );
    static final int PARTITIONS_PER_TOPIC = 10;
    // increase when ci is beefier
    static final int WRITE_ITERATIONS = 500;
    static final String BUCKET_NAME = "test-bucket";
    static final long MAX_UPLOAD_FILE_SIZE = 10 * 1024;
    static final long FILE_MERGE_THRESHOLD = 20 * MAX_UPLOAD_FILE_SIZE;
    static final short FETCH_VERSION = ApiMessageType.FETCH.highestSupportedVersion(true);

    static final List<TopicIdPartition> ALL_TOPIC_ID_PARTITIONS = TOPICS.entrySet().stream().flatMap(kv ->
        IntStream.range(0, PARTITIONS_PER_TOPIC)
            .mapToObj(p -> new TopicIdPartition(kv.getValue(), p, kv.getKey()))
    ).toList();

    static S3Client s3Client;

    @Mock
    Time time;
    @Mock
    MetadataView metadataView;
    @Mock
    Supplier<LogConfig> defaultTopicConfigs;

    @BeforeAll
    static void setupS3() {
        final var clientBuilder = S3Client.builder();
        clientBuilder.region(Region.of(LOCALSTACK.getRegion()))
            .endpointOverride(LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.S3))
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(
                        LOCALSTACK.getAccessKey(),
                        LOCALSTACK.getSecretKey()
                    )
                )
            );
        s3Client = clientBuilder.build();
        s3Client.createBucket(CreateBucketRequest.builder().bucket(BUCKET_NAME).build());
    }

    @AfterAll
    static void tearDownS3() {
        s3Client.close();
    }

    ControlPlane controlPlane;
    SharedState sharedState;

    @BeforeEach
    void setup() {
        for (final var entry : TOPICS.entrySet()) {
            when(metadataView.isInklessTopic(entry.getKey())).thenReturn(true);
            when(metadataView.getTopicId(entry.getKey())).thenReturn(entry.getValue());
        }
        when(metadataView.getTopicConfig(anyString())).thenReturn(new Properties());
        when(defaultTopicConfigs.get()).thenReturn(new LogConfig(Map.of()));

        controlPlane = new InMemoryControlPlane(time);
        controlPlane.configure(Map.of(
            "file.merge.size.threshold.bytes", Long.toString(FILE_MERGE_THRESHOLD)
        ));

        final Map<String, String> config = new HashMap<>();
        config.put("control.plane.class", InMemoryControlPlane.class.getCanonicalName());
        config.put("object.key.prefix", "my-prefix");
        config.put("produce.commit.interval.ms", Integer.toString(Integer.MAX_VALUE));  // deterministically commit by bytes
        config.put("produce.buffer.max.bytes", Long.toString(MAX_UPLOAD_FILE_SIZE));
        config.put("storage.backend.class", S3Storage.class.getCanonicalName());
        config.put("storage.s3.bucket.name", BUCKET_NAME);
        config.put("storage.s3.region", LOCALSTACK.getRegion());
        config.put("storage.s3.endpoint.url", LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.S3).toString());
        config.put("storage.aws.access.key.id", LOCALSTACK.getAccessKey());
        config.put("storage.aws.secret.access.key", LOCALSTACK.getSecretKey());
        config.put("storage.s3.path.style.access.enabled", "true");
        final InklessConfig inklessConfig = new InklessConfig(config);

        sharedState = SharedState.initialize(time, "cluster-id", "az1", BROKER_ID, inklessConfig,
            metadataView, controlPlane, new BrokerTopicStats(), defaultTopicConfigs);
    }

    @AfterEach
    void tearDown() {
        try {
            sharedState.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void test() throws Exception {

        createTopics(controlPlane);

        final AppendInterceptor appendInterceptor = new AppendInterceptor(sharedState);
        final FetchInterceptor fetchInterceptor = new FetchInterceptor(sharedState);
        final FileMerger fileMerger = new FileMerger(sharedState);

        // Write a bunch of records.
        writeRecords(appendInterceptor);

        // Consume the high watermarks and the records themselves for future comparison.
        final Map<TopicIdPartition, Long> highWatermarks1 = getHighWatermarks(controlPlane);
        final Map<TopicIdPartition, List<RecordBatch>> batches1 = read(fetchInterceptor, highWatermarks1);
        // Ensure _something_ was written.
        for (final long hwm : highWatermarks1.values()) {
            assertThat(hwm).isPositive();
        }
        for (final List<RecordBatch> bs : batches1.values()) {
            assertThat(bs).isNotEmpty();
            for (final RecordBatch b : bs) {
                assertThat(b.countOrNull()).isPositive();
            }
        }
        final List<S3Object> files1 = getFiles();

        // Merge and delete old files while possible.
        int deletedPreviousIteration = Integer.MAX_VALUE;
        while (deletedPreviousIteration > 0) {
            fileMerger.run();
            deletedPreviousIteration = deleteFilesToBeDeleted(controlPlane);
        }

        final List<S3Object> files2 = getFiles();

        // After merging, there should be fewer files.
        assertThat(files2.size()).isLessThan(files1.size());

        // However, the watermarks and records should be exactly as before.
        final Map<TopicIdPartition, Long> highWatermarks2 = getHighWatermarks(controlPlane);
        assertThat(highWatermarks2).isEqualTo(highWatermarks1);
        final Map<TopicIdPartition, List<RecordBatch>> batches2 = read(fetchInterceptor, highWatermarks2);
        assertThat(batches2).isEqualTo(batches1);
    }

    private void createTopics(final ControlPlane controlPlane) {
        final Set<CreateTopicAndPartitionsRequest> createTopicsRequests = TOPICS.entrySet().stream()
            .map(kv -> new CreateTopicAndPartitionsRequest(kv.getValue(), kv.getKey(), PARTITIONS_PER_TOPIC))
            .collect(Collectors.toSet());
        controlPlane.createTopicAndPartitions(createTopicsRequests);
    }

    private void writeRecords(final AppendInterceptor appendInterceptor) {
        final WriterTestUtils.RecordCreator recordCreator = new WriterTestUtils.RecordCreator();
        final AtomicInteger produceResponseCallbackCalls = new AtomicInteger(WRITE_ITERATIONS);
        final Consumer<Map<TopicPartition, ProduceResponse.PartitionResponse>> responseCallback =
            (r) -> produceResponseCallbackCalls.decrementAndGet();

        for (int i = 0; i < WRITE_ITERATIONS; i++) {
            final HashMap<TopicPartition, MemoryRecords> records = new HashMap<>();
            for (int tpi = 0; tpi < ALL_TOPIC_ID_PARTITIONS.size(); tpi++) {
                if (i % (tpi + 1) == 0) {
                    final TopicIdPartition tidp = ALL_TOPIC_ID_PARTITIONS.get(tpi);
                    records.put(tidp.topicPartition(), recordCreator.create(tidp.topicPartition(), i));
                }
            }
            assertThat(appendInterceptor.intercept(records, responseCallback)).isTrue();
        }

        await().atMost(Duration.ofSeconds(60))
            .pollDelay(Duration.ofMillis(100))
            .until(() -> produceResponseCallbackCalls.get() == 0);
    }

    private Map<TopicIdPartition, Long> getHighWatermarks(final ControlPlane controlPlane) {
        final List<FindBatchRequest> findBatchRequests = ALL_TOPIC_ID_PARTITIONS.stream()
            .map(tidp -> new FindBatchRequest(tidp, 0, Integer.MAX_VALUE))
            .toList();
        final List<FindBatchResponse> findBatchResponses = controlPlane.findBatches(findBatchRequests, true, Integer.MAX_VALUE);

        final Map<TopicIdPartition, Long> result = new HashMap<>();
        for (int i = 0; i < findBatchResponses.size(); i++) {
            final FindBatchRequest findBatchRequest = findBatchRequests.get(i);
            final FindBatchResponse findBatchResponse = findBatchResponses.get(i);
            assertThat(findBatchResponse.errors()).isEqualTo(Errors.NONE);
            result.put(findBatchRequest.topicIdPartition(), findBatchResponse.highWatermark());
        }
        return result;
    }

    private Map<TopicIdPartition, List<RecordBatch>> read(final FetchInterceptor fetchInterceptor,
                                                          final Map<TopicIdPartition, Long> highWatermarks) throws InterruptedException {
        final ConcurrentHashMap<TopicIdPartition, Long> fetchPositions = new ConcurrentHashMap<>(
            highWatermarks.keySet().stream().collect(Collectors.toMap(k -> k, ignored -> 0L))
        );
        final ConcurrentMap<TopicIdPartition, List<RecordBatch>> records = new ConcurrentHashMap<>(
            highWatermarks.keySet().stream().collect(Collectors.toMap(k -> k, ignored -> new ArrayList<>()))
        );

        final Supplier<Boolean> hasMoreToRead = () -> fetchPositions.entrySet().stream().anyMatch(kv ->
            kv.getValue() < highWatermarks.get(kv.getKey())
        );
        while (hasMoreToRead.get()) {
            readIteration(fetchInterceptor, fetchPositions, records);
        }
        assertThat(fetchPositions).isEqualTo(highWatermarks);

        return records;
    }

    private void readIteration(final FetchInterceptor fetchInterceptor,
                               final ConcurrentHashMap<TopicIdPartition, Long> fetchPositions,
                               final ConcurrentMap<TopicIdPartition, List<RecordBatch>> records) throws InterruptedException {
        final FetchParams params = new FetchParams(FETCH_VERSION,
            -1, -1, -1, -1, -1,
            FetchIsolation.LOG_END, Optional.empty());

        final AtomicBoolean inconsistentOffset = new AtomicBoolean(false);
        final CountDownLatch callbackCalled = new CountDownLatch(1);
        final Consumer<Map<TopicIdPartition, FetchPartitionData>> responseCallback = (Map<TopicIdPartition, FetchPartitionData> result) -> {
            for (final var entry : result.entrySet()) {
                final var tidp = entry.getKey();
                boolean isEmpty = true;
                for (final var record : entry.getValue().records.records()) {
                    isEmpty = false;
                    final long pos = fetchPositions.get(tidp);
                    if (record.offset() != pos) {
                        LOGGER.error("Inconsistent offset in {}: expected {}, got {}", tidp, pos, record.offset());
                        inconsistentOffset.set(true);
                    }
                    fetchPositions.put(tidp, pos + 1);
                }
                if (!isEmpty) {
                    records.computeIfPresent(tidp, (ignore, rs) -> {
                        for (final var batch : entry.getValue().records.batches()) {
                            rs.add(batch);
                        }
                        return rs;
                    });
                }
            }

            callbackCalled.countDown();
        };

        final Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos = ALL_TOPIC_ID_PARTITIONS.stream().collect(Collectors.toMap(
            tidp -> tidp,
            tidp -> new FetchRequest.PartitionData(TOPIC_ID_0, fetchPositions.get(tidp), 0, 1024 * 1024, Optional.empty())
        ));
        assertThat(fetchInterceptor.intercept(params, fetchInfos, responseCallback)).isTrue();
        callbackCalled.await();

        if (inconsistentOffset.get()) {
            throw new RuntimeException("Inconsistent offset");
        }
    }

    private List<S3Object> getFiles() {
        final List<S3Object> result = new ArrayList<>();
        ListObjectsV2Response response = null;
        while (response == null || response.isTruncated()) {
            final ListObjectsV2Request request = ListObjectsV2Request.builder().bucket(BUCKET_NAME)
                .continuationToken(response != null ? response.nextContinuationToken() : null)
                .build();
            response = s3Client.listObjectsV2(request);
            result.addAll(response.contents());
        }
        return result;
    }

    private int deleteFilesToBeDeleted(final ControlPlane controlPlane) {
        final List<FileToDelete> filesToDelete = controlPlane.getFilesToDelete();
        if (filesToDelete.isEmpty()) {
            return 0;
        }

        final List<ObjectIdentifier> objectIdentifiers = filesToDelete.stream()
            .map(f -> ObjectIdentifier.builder().key(f.objectKey()).build())
            .toList();
        final DeleteObjectsRequest deleteObjectsRequest = DeleteObjectsRequest.builder()
            .bucket(BUCKET_NAME)
            .delete(Delete.builder().objects(objectIdentifiers).build())
            .build();
        final DeleteObjectsResponse deleteObjectsResponse = s3Client.deleteObjects(deleteObjectsRequest);
        assertThat(deleteObjectsResponse.errors()).isEmpty();

        controlPlane.deleteFiles(new DeleteFilesRequest(filesToDelete.stream().map(FileToDelete::objectKey).collect(Collectors.toSet())));

        return filesToDelete.size();
    }
}
