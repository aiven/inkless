package kafka;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.common.RequestLocal;
import org.apache.kafka.storage.internals.log.LogConfig;
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

public class Antithesis {
    private static Uuid TOPIC_ID_0 = new Uuid(101, 201);
    private static Uuid TOPIC_ID_1 = new Uuid(102, 202);
    private static String TOPIC_NAME_0 = "topic0";
    private static String TOPIC_NAME_1 = "topic1";
    private static TopicIdPartition T0P0 = new TopicIdPartition(TOPIC_ID_0, 0, TOPIC_NAME_0);
    private static TopicIdPartition T0P1 = new TopicIdPartition(TOPIC_ID_0, 1, TOPIC_NAME_0);
    private static TopicIdPartition T1P0 = new TopicIdPartition(TOPIC_ID_1, 0, TOPIC_NAME_1);

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        final String pgConnectionString = args[0];
        final String s3Endpoint = args[1];
        final String materializationDir = args[2];
        System.out.println(pgConnectionString);
        System.out.println(s3Endpoint);

        final BrokerTopicStats brokerTopicStats = new BrokerTopicStats();

        final Time time = Time.SYSTEM;
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
        inklessConfigProps.put("materialization.directory", materializationDir);
        final InklessConfig config = new InklessConfig(inklessConfigProps);
        final MetadataView metadataView = new TestMetadataView();

        final ControlPlane controlPlane = ControlPlane.create(config, time);
        controlPlane.createTopicAndPartitions(Set.of(
            new CreateTopicAndPartitionsRequest(TOPIC_ID_0, TOPIC_NAME_0, 2),
            new CreateTopicAndPartitionsRequest(TOPIC_ID_1, TOPIC_NAME_1, 1)
        ));

        final ObjectKeyCreator objectKeyCreator = ObjectKey.creator(config.objectKeyPrefix(), config.objectKeyLogPrefixMasked());

        final FixedBlockAlignment keyAlignmentStrategy = new FixedBlockAlignment(0);

        final ObjectCache objectCache = new TestObjectCache();

        final SharedState sharedState = new SharedState(
            time, 0, config, metadataView, controlPlane, objectKeyCreator, keyAlignmentStrategy,
            objectCache, brokerTopicStats, () -> LogConfig.fromProps(Map.of(), new Properties())
        );

        final MaterializedLogManager materializedLogManager = new MaterializedLogManager(sharedState);
        materializedLogManager.startReplica(T0P0);
        materializedLogManager.startReplica(T0P1);
        materializedLogManager.startReplica(T1P0);

        final AppendHandler handler = new AppendHandler(sharedState);
        for (int i = 0; i < 10; i++) {
            final MemoryRecords records = MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(
                123, new byte[10], new byte[100]
            ));
            final var future = handler.handle(
                Map.of(T0P0, records),
                RequestLocal.noCaching()
            );
            future.get();
        }

        Thread.sleep(5000);

        materializedLogManager.shutdown();
        handler.close();


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
}
