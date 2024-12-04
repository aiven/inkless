package io.aiven.inkless.produce;

import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class BrokerTopicMetricMarks {
    public final Consumer<String> requestRateMark;
    public final BiConsumer<String, Integer> bytesInRateMark;
    public final BiConsumer<String, Long> messagesInRateMark;

    public BrokerTopicMetricMarks() {
        this.requestRateMark = (String topicName) -> {};
        this.bytesInRateMark = (String topicName, Integer bytes) -> {};
        this.messagesInRateMark = (String topicName, Long messages) -> {};
    }

    public BrokerTopicMetricMarks(BrokerTopicStats brokerTopicStats) {
        this.requestRateMark = (String topicName) -> {
            brokerTopicStats.topicStats(topicName).totalProduceRequestRate().mark();
            brokerTopicStats.allTopicsStats().totalProduceRequestRate().mark();
        };
        this.bytesInRateMark = (String topicName, Integer bytes) -> {
            brokerTopicStats.topicStats(topicName).bytesInRate().mark(bytes);
            brokerTopicStats.allTopicsStats().bytesInRate().mark(bytes);
        };
        this.messagesInRateMark = (String topicName, Long messages) -> {
            brokerTopicStats.topicStats(topicName).messagesInRate().mark(messages);
            brokerTopicStats.allTopicsStats().messagesInRate().mark(messages);
        };
    }
}
