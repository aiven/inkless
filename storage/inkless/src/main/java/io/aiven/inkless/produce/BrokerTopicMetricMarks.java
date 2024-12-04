package io.aiven.inkless.produce;

import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import com.groupcdg.pitest.annotations.CoverageIgnore;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

@CoverageIgnore
public class BrokerTopicMetricMarks {
    public final Consumer<String> requestRateMark;
    public final BiConsumer<String, Integer> bytesInRateMark;
    public final BiConsumer<String, Long> messagesInRateMark;

    public BrokerTopicMetricMarks(Consumer<String> requestRateMark,
                                  BiConsumer<String, Integer> bytesInRateMark,
                                  BiConsumer<String, Long> messagesInRateMark) {
        this.requestRateMark = requestRateMark;
        this.bytesInRateMark = bytesInRateMark;
        this.messagesInRateMark = messagesInRateMark;
    }

    public BrokerTopicMetricMarks() {
        this(
            (String topicName) -> {},
            (String topicName, Integer bytes) -> {},
            (String topicName, Long messages) -> {}
        );
    }

    public BrokerTopicMetricMarks(BrokerTopicStats brokerTopicStats) {
        this(
            (String topicName) -> {
                brokerTopicStats.topicStats(topicName).totalProduceRequestRate().mark();
                brokerTopicStats.allTopicsStats().totalProduceRequestRate().mark();
            },
            (String topicName, Integer bytes) -> {
                brokerTopicStats.topicStats(topicName).bytesInRate().mark(bytes);
                brokerTopicStats.allTopicsStats().bytesInRate().mark(bytes);
            },
            (String topicName, Long messages) -> {
                brokerTopicStats.topicStats(topicName).messagesInRate().mark(messages);
                brokerTopicStats.allTopicsStats().messagesInRate().mark(messages);
            }
        );
    }
}
