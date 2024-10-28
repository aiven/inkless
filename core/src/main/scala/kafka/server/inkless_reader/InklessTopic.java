package kafka.server.inkless_reader;

import static org.apache.kafka.common.internals.Topic.CLUSTER_METADATA_TOPIC_NAME;
import static org.apache.kafka.common.internals.Topic.GROUP_METADATA_TOPIC_NAME;
import static org.apache.kafka.common.internals.Topic.SHARE_GROUP_STATE_TOPIC_NAME;
import static org.apache.kafka.common.internals.Topic.TRANSACTION_STATE_TOPIC_NAME;

public class InklessTopic {
    public static boolean isInklessTopic(final String topic) {
        return !topic.equals(GROUP_METADATA_TOPIC_NAME)
            && !topic.equals(TRANSACTION_STATE_TOPIC_NAME)
            && !topic.equals(SHARE_GROUP_STATE_TOPIC_NAME)
            && !topic.equals(CLUSTER_METADATA_TOPIC_NAME);
    }
}
