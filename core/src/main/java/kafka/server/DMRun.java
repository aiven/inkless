//package kafka.server;
//
//import kafka.Kafka;
//import kafka.log.LogManager;
//import kafka.server.metadata.KRaftMetadataCache;
//import org.apache.kafka.common.Node;
//import org.apache.kafka.common.Uuid;
//import org.apache.kafka.common.metadata.PartitionRecord;
//import org.apache.kafka.common.metadata.TopicRecord;
//import org.apache.kafka.common.metrics.Metrics;
//import org.apache.kafka.common.network.ListenerName;
//import org.apache.kafka.common.requests.LeaderAndIsrRequest;
//import org.apache.kafka.common.security.auth.SecurityProtocol;
//import org.apache.kafka.common.utils.Time;
//import org.apache.kafka.image.AclsImage;
//import org.apache.kafka.image.ClientQuotasImage;
//import org.apache.kafka.image.ClusterImage;
//import org.apache.kafka.image.ConfigurationsImage;
//import org.apache.kafka.image.DelegationTokenImage;
//import org.apache.kafka.image.FeaturesImage;
//import org.apache.kafka.image.MetadataImage;
//import org.apache.kafka.image.MetadataProvenance;
//import org.apache.kafka.image.ProducerIdsImage;
//import org.apache.kafka.image.ScramImage;
//import org.apache.kafka.image.TopicDelta;
//import org.apache.kafka.image.TopicImage;
//import org.apache.kafka.image.TopicsDelta;
//import org.apache.kafka.image.TopicsImage;
//import org.apache.kafka.metadata.PartitionRegistration;
//import org.apache.kafka.server.DelayedActionQueue;
//import org.apache.kafka.server.ProcessRole;
//import org.apache.kafka.server.common.DirectoryEventHandler;
//import org.apache.kafka.server.common.KRaftVersion;
//import org.apache.kafka.server.common.MetadataVersion;
//import org.apache.kafka.server.immutable.pcollections.PCollectionsImmutableMap;
//import org.apache.kafka.server.util.KafkaScheduler;
//import org.apache.kafka.storage.internals.log.LogDirFailureChannel;
//import org.apache.kafka.storage.log.metrics.BrokerTopicStats;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import scala.None$;
//import scala.jdk.CollectionConverters;
//
//import java.util.Collections;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.atomic.AtomicBoolean;
//
//public class DMRun {
//    final static Logger LOGGER = LoggerFactory.getLogger(DMRun.class);
//
//    public static void main(String[] args) throws InterruptedException {
//        final KafkaConfig config = new KafkaConfig(Kafka.getPropsFromArgs(args));
//        final Time time = Time.SYSTEM;
//
//        final var initLogDirsResult = KafkaRaftServer.initializeLogDirs(config, LOGGER, "");
//
//        final KafkaScheduler kafkaScheduler = new KafkaScheduler(1);
//        kafkaScheduler.startup();
//
//        final Metrics metrics = Server.initializeMetrics(
//            config,
//            time,
//            initLogDirsResult._1.clusterId().get()
//        );
//
//        final BrokerTopicStats brokerTopicStats = new BrokerTopicStats(false);
//        final KRaftMetadataCache metadataCache = new KRaftMetadataCache(1, () -> KRaftVersion.LATEST_PRODUCTION);
//
////        final Uuid topicId = new Uuid(123, 456);
//        final Uuid topicId = Uuid.fromString("QVCt427RS-ensL3Yp7q0CQ");
//        final String topicName = "tx";
//        final TopicImage topicImage = new TopicImage(topicName, topicId, Map.of(
//            0, new PartitionRegistration(
//                new PartitionRecord()
//                    .setPartitionId(0)
//                    .setTopicId(topicId)
//                    .setReplicas(List.of(1))
//                    .setIsr(List.of(1))
//                    .setLeader(1)
//                    .setLeaderEpoch(0)
//            ),
//            1, new PartitionRegistration(
//                new PartitionRecord()
//                    .setPartitionId(1)
//                    .setTopicId(topicId)
//                    .setReplicas(List.of(1))
//                    .setIsr(List.of(1))
//                    .setLeader(1)
//                    .setLeaderEpoch(0)
//            )
//        ));
//        final TopicsImage topicsImage = new TopicsImage(PCollectionsImmutableMap.singleton(topicId, topicImage), PCollectionsImmutableMap.singleton(topicName, topicImage));
//        final var featuresImageLatest = new FeaturesImage(
//            Collections.emptyMap(),
//            MetadataVersion.latestProduction());
//        final MetadataImage metadataImage = new MetadataImage(
//            MetadataProvenance.EMPTY,
//            featuresImageLatest,
//            ClusterImage.EMPTY,
//            topicsImage,
//            ConfigurationsImage.EMPTY,
//            ClientQuotasImage.EMPTY,
//            ProducerIdsImage.EMPTY,
//            AclsImage.EMPTY,
//            ScramImage.EMPTY,
//            DelegationTokenImage.EMPTY
//        );
//        metadataCache.setImage(metadataImage);
//
//        System.out.println(metadataCache.topicIdsToNames());
//
//        final LogDirFailureChannel logDirFailureChannel = new LogDirFailureChannel(config.logDirs().size());
//
//        final LogManager logManager = LogManager.apply(config,
//            CollectionConverters.SetHasAsScala(initLogDirsResult._1.errorLogDirs()).asScala().toSeq(),
//            metadataCache,
//            kafkaScheduler,
//            time,
//            brokerTopicStats,
//            logDirFailureChannel);
//
//        logManager.startup(CollectionConverters.SetHasAsScala(metadataCache.getAllTopics()).asScala(), log -> false);
//
//        final QuotaFactory.QuotaManagers quotaManagers = QuotaFactory.instantiate(config, metrics, time, "broker-0-", ProcessRole.BrokerRole.toString());
//
//        final var alterPartitionManager = AlterPartitionManager.apply(
//            config,
//            kafkaScheduler,
//            new ControllerNodeProvider() {
//                @Override
//                public ControllerInformation getControllerInfo() {
//                    return new ControllerInformation(None$.empty(), ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT), SecurityProtocol.PLAINTEXT, "");
//                }
//            },
//            time,
//            metrics,
//            "broker-0-",
//            () -> 0
//        );
////        alterPartitionManager.start();
//
//        final DirectoryEventHandler directoryEventHandler = new DirectoryEventHandler() {
//            @Override
//            public void handleAssignment(final org.apache.kafka.server.common.TopicIdPartition partition, final Uuid directoryId, final String reason, final Runnable callback) {
//
//            }
//
//            @Override
//            public void handleFailure(final Uuid directoryId) {
//
//            }
//        };
//        final DelayedActionQueue defaultActionQueue = new DelayedActionQueue();
//
//        final ReplicaManager replicaManager = new ReplicaManager(
//            config,
//            metrics,
//            time,
//            kafkaScheduler,
//            logManager,
//            None$.empty(),
//            quotaManagers,
//            metadataCache,
//            logDirFailureChannel,
//            alterPartitionManager,
//            brokerTopicStats,
//            new AtomicBoolean(false),
//            None$.empty(),
//            None$.empty(),
//            None$.empty(),
//            None$.empty(),
//            None$.empty(),
//            None$.empty(),
//            None$.empty(),
//            () -> -1L,
//            None$.empty(),
//            directoryEventHandler,
//            defaultActionQueue,
//            None$.empty(),
//            None$.empty()
//        );
//        final LeaderAndIsrRequest leaderAndIsrRequest = new LeaderAndIsrRequest.Builder(
//            1, 0, 0,
//            List.of(
//                new LeaderAndIsrRequest.PartitionState()
//                    .setLeader(1)
//                    .setLeaderEpoch(0)
//                    .setReplicas(List.of(1))
//                    .setIsr(List.of(1))
//                    .setTopicName(topicName)
//            ),
//            Map.of(topicName, topicId),
//            List.of(new Node(1, "127.0.0.1", 9092))
//        ).build();
//
//        final TopicsDelta delta = new TopicsDelta(topicsImage);
//        delta.replay(new TopicRecord().setName(topicName).setTopicId(topicId));
//        delta.replay(new PartitionRecord()
//            .setPartitionId(0)
//            .setTopicId(topicId)
//            .setReplicas(List.of(1))
//            .setIsr(List.of(1))
//            .setRemovingReplicas(Collections.emptyList())
//            .setAddingReplicas(Collections.emptyList())
//            .setLeader(1)
//            .setLeaderEpoch(0)
//            .setPartitionEpoch(0)
//        );
//
//        replicaManager.applyDelta(delta, metadataImage);
//
////        final DisklessManager disklessManager = new DisklessManager(Time.SYSTEM, replicaManager);
////
////        final TopicIdPartition tidp = new TopicIdPartition(topicId, 0, topicName);
////        final DisklessCommitRequestData.PartitionDisklessCommitData commitData = new DisklessCommitRequestData.PartitionDisklessCommitData()
////            .setPartition(0)
////            .setBatches(List.of(
////                new DisklessCommitRequestData.BatchDisklessCommitData().setBaseOffset(0),
////                new DisklessCommitRequestData.BatchDisklessCommitData().setBaseOffset(10)
////            ));
////        disklessManager.commitFile("file1", Map.of(tidp, commitData));
//
//        kafkaScheduler.shutdown();
//        metrics.close();
//        brokerTopicStats.close();
//        logManager.shutdown(-1);
//        quotaManagers.shutdown();
//        replicaManager.shutdown(true);
//    }
//}
