// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.storage.internals.log.BatchMetadata;
import org.apache.kafka.storage.internals.log.ProducerStateEntry;
import org.apache.kafka.storage.internals.log.ProducerStateManagerConfig;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProducerStateCacheTest {
    final ProducerStateManagerConfig producerStateManagerConfig = new ProducerStateManagerConfig(60000, false);

    @Test
    void failGetOnNonExistentPartition() {
        final ProducerStateCache state = new ProducerStateCache(new MockTime(), producerStateManagerConfig);
        final TopicPartition t1p0 = new TopicPartition("t1", 0);
        assertThatThrownBy(() -> state.producerStateManager(t1p0)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void initializePartition() {
        final ProducerStateCache state = new ProducerStateCache(new MockTime(), producerStateManagerConfig);
        final TopicPartition t1p0 = new TopicPartition("t1", 0);
        state.initializeProducerStateManagerIfNeeded(t1p0);
        assertThat(state.producerStateManager(t1p0)).isNotNull();
    }

    @Test
    void updateNotRequired() {
        final ProducerStateCache state = new ProducerStateCache(new MockTime(), producerStateManagerConfig);
        final TopicIdPartition t1p0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("t1", 0));
        state.initializeProducerStateManagerIfNeeded(t1p0.topicPartition());
        final long producerId = 1L;
        assertThat(state.isUnknownProducerId(t1p0.topicPartition(), producerId)).isTrue();

        final CommitBatchResponse commitBatchResponse = CommitBatchResponse.success(
            20, 100, 1,
            CommitBatchRequest.of(t1p0, 10, 100, 10, 19, 120000L, TimestampType.CREATE_TIME)
        );
        state.update(commitBatchResponse);
        assertThat(state.isUnknownProducerId(t1p0.topicPartition(), producerId)).isTrue();
    }

    @Test
    void failUpdateWithoutRequest() {
        final ProducerStateCache state = new ProducerStateCache(new MockTime(), producerStateManagerConfig);
        final TopicPartition t1p0 = new TopicPartition("t1", 0);
        state.initializeProducerStateManagerIfNeeded(t1p0);
        assertThatThrownBy(() -> state.update(null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> state.update(CommitBatchResponse.unknownTopicOrPartition())).isInstanceOf(NullPointerException.class);
    }

    @Test
    void testUpdate() {
        // Given a producer state
        final ProducerStateCache state = new ProducerStateCache(new MockTime(), producerStateManagerConfig);
        final TopicIdPartition t1p0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("t1", 0));
        final long producerId = 1L;
        // When the producer state is first initialized
        state.initializeProducerStateManagerIfNeeded(t1p0.topicPartition());
        // Then the producer id is unknown
        assertThat(state.isUnknownProducerId(t1p0.topicPartition(), producerId)).isTrue();

        // When a batch is committed
        final CommitBatchRequest request = CommitBatchRequest.idempotent(t1p0, 10, 100, 10, 19, 120000L, TimestampType.CREATE_TIME, producerId, (short) 1, 10, 19);
        final CommitBatchResponse commitBatchResponse = CommitBatchResponse.success(20, 100, 1, request);
        state.update(commitBatchResponse);
        // Then the producer id is known and the last state entry is updated
        assertThat(state.isUnknownProducerId(t1p0.topicPartition(), producerId)).isFalse();
        final Optional<ProducerStateEntry> producerStateEntry = state.producerStateManager(t1p0.topicPartition()).lastEntry(producerId);
        assertThat(producerStateEntry.isPresent()).isTrue();
        assertThat(producerStateEntry.get().lastDataOffset()).isEqualTo(29);

        // When a duplicate batch is checked
        final Optional<BatchMetadata> maybeDuplicate = state.hasDuplicate(request);
        // Then the duplicate batch is found
        assertThat(maybeDuplicate)
            .contains(new BatchMetadata(19, 29, 9, 120000L));

        // When a new batch is committed
        final CommitBatchRequest request2 = CommitBatchRequest.idempotent(t1p0, 20, 200, 30, 39, 210000L, TimestampType.CREATE_TIME, producerId, (short) 2, 0, 9);
        final CommitBatchResponse commitBatchResponse2 = CommitBatchResponse.success(30, 200, 1, request2);
        state.update(commitBatchResponse2);
        // Then the last state entry is updated
        final Optional<ProducerStateEntry> producerStateEntry2 = state.producerStateManager(t1p0.topicPartition()).lastEntry(producerId);
        assertThat(producerStateEntry2.isPresent()).isTrue();
        assertThat(producerStateEntry2.get().lastDataOffset()).isEqualTo(39);
    }

    @Test
    void duplicateEmptyIfNoProducerId() {
        final ProducerStateCache state = new ProducerStateCache(new MockTime(), producerStateManagerConfig);
        final TopicIdPartition t1p0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("t1", 0));
        state.initializeProducerStateManagerIfNeeded(t1p0.topicPartition());
        final CommitBatchRequest request = CommitBatchRequest.of(t1p0, 10, 100, 10, 19, 120000L, TimestampType.CREATE_TIME);
        assertThat(state.hasDuplicate(request)).isEmpty();
    }

    @Test
    void testMinimumTimestamp() {
        // Given this producerIdExpirationMs
        final int producerIdExpirationMs = 60000;
        final ProducerStateManagerConfig producerStateManagerConfig = new ProducerStateManagerConfig(producerIdExpirationMs, false);

        // When time is zero
        final MockTime time = new MockTime(0, 0, 0);
        final ProducerStateCache state = new ProducerStateCache(time, producerStateManagerConfig);

        // Then the minimum timestamp is zero
        assertThat(state.minimumTimestamp().toEpochMilli()).isEqualTo(0);

        // When time moves forward
        final int later = 140000;
        time.sleep(later);
        // Then the minimum timestamp is expiration time ago
        assertThat(state.minimumTimestamp().toEpochMilli()).isEqualTo(later - producerIdExpirationMs);
    }
}
