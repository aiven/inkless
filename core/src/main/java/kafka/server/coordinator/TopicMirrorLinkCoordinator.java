package kafka.server.coordinator;

import kafka.server.KafkaConfig;
import kafka.server.ReplicaManager;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.metadata.MetadataCache;
import org.apache.kafka.server.util.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.OptionalInt;

public record TopicMirrorLinkCoordinator(KafkaConfig config,
                                         ReplicaManager replicaManager,
                                         Scheduler scheduler,
                                         Metrics metrics,
                                         MetadataCache metadataCache,
                                         Time time) {

    private static final Logger log = LoggerFactory.getLogger(TopicMirrorLinkCoordinator.class);

    public void startup() {
        log.info("Starting up.");
        scheduler.startup();
        scheduler.schedule("topic-mirror-link-query",
                this::querySourceCluster,
                5000,
                5000
        );
    }

    // periodically query source cluster to get the metadata
    void querySourceCluster() {

    }

    // called when onMetadataUpdate, needs to handle new leader elected
    public void onElection(
            int partitionIndex,
            int partitionLeaderEpoch
    ) {

    }

    // called when onMetadataUpdate, needs to handle old leader resigned
    public void onResignation(
            int partitionIndex,
            OptionalInt partitionLeaderEpoch
    ) {

    }

    public void shutdown() {

    }
}
