// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.internals.log.BatchMetadata;
import org.apache.kafka.storage.internals.log.ProducerStateManagerConfig;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProducerStateManagersTest {

    ProducerStateManagerConfig config = new ProducerStateManagerConfig(86400000, false);
    public static final TopicPartition T1P0 = new TopicPartition("t1", 0);

    @Test
    void validateEmpty() {
        final ProducerStateManagers psm = new ProducerStateManagers(new MockTime(), config);
        var result = psm.validateProducer(Map.of());
        assertThat(result.duplicates()).isEmpty();
    }

    @Test
    void failValidateOnNull() {
        final ProducerStateManagers psm = new ProducerStateManagers(new MockTime(), config);
        assertThatThrownBy(() -> psm.validateProducer(null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("requestsWithProducerId cannot be null");
    }

    @Test
    void failValidateOnBatchWithoutProducerId() {
        final ProducerStateManagers psm = new ProducerStateManagers(new MockTime(), config);
        final RecordBatch batch = MemoryRecords.withRecords(12, Compression.NONE, new SimpleRecord(new byte[10])).firstBatch();
        final var batches = List.of(batch);
        final var request = Map.of(0, Map.of(T1P0, batches));
        assertThatThrownBy(() -> psm.validateProducer(request))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("Batch must have producer id");
    }

    @Test
    void noDuplicatesOnEmptyState() {
        final ProducerStateManagers psm = new ProducerStateManagers(new MockTime(), config);
        psm.producerStateManagers.put(T1P0, psm.getProducerStateManager(T1P0));

        final long producerId = 1L;
        final RecordBatch batch = MemoryRecords.withIdempotentRecords(12, Compression.NONE, producerId, (short) 1, 1, 1, new SimpleRecord(new byte[10])).firstBatch();
        final var batches = List.of(batch);

        final var request = Map.of(0, Map.of(T1P0, batches));
        var result = psm.validateProducer(request);

        assertThat(result.duplicates()).containsOnlyKeys(0).containsValues(Map.of());
    }

    @Test
    void failUpdateOnNull() {
        final ProducerStateManagers psm = new ProducerStateManagers(new MockTime(), config);
        assertThatThrownBy(() -> psm.updateProducer(null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("requests cannot be null");
    }

    @Test
    void noUpdateOnEmptyRequest() {
        final ProducerStateManagers psm = new ProducerStateManagers(new MockTime(), config);
        psm.updateProducer(Map.of());
        assertThat(psm.producerStateManagers).isEmpty();
    }

    @Test
    void noUpdateOnRequestWithoutProducerId() {
        final ProducerStateManagers psm = new ProducerStateManagers(new MockTime(), config);
        psm.updateProducer(Map.of(1, Map.of(
            T1P0,
            List.of(MemoryRecords.withRecords(19, Compression.NONE, new SimpleRecord(new byte[10]), new SimpleRecord(new byte[10]), new SimpleRecord(new byte[10])).firstBatch())
        )));
        assertThat(psm.producerStateManagers).isEmpty();
    }

    @Test
    void validUpdates() {
        final Time time = new MockTime();
        final ProducerStateManagers psm = new ProducerStateManagers(time, config);
        final long producerId = 1L;
        short producerEpoch = (short) 1;
        final var batch = (MutableRecordBatch) MemoryRecords.withIdempotentRecords(19, Compression.NONE, producerId, producerEpoch, 0, 2, new SimpleRecord(new byte[10]), new SimpleRecord(new byte[10]), new SimpleRecord(new byte[10])).firstBatch();
        batch.setMaxTimestamp(TimestampType.CREATE_TIME, time.milliseconds());
        psm.updateProducer(Map.of(0, Map.of(T1P0, List.of(batch))));
        assertThat(psm.producerStateManagers).hasSize(1);
        {
            final var maybeLastEntry = psm.producerStateManagers.get(T1P0).lastEntry(producerId);
            assertThat(maybeLastEntry).isPresent();
            final var lastEntry = maybeLastEntry.get();
            assertThat(lastEntry.producerId()).isEqualTo(producerId);
            assertThat(lastEntry.producerEpoch()).isEqualTo(producerEpoch);
            assertThat(lastEntry.firstDataOffset()).isEqualTo(19);
            assertThat(lastEntry.lastDataOffset()).isEqualTo(21);
            assertThat(lastEntry.lastTimestamp()).isEqualTo(time.milliseconds());
            assertThat(lastEntry.lastSeq()).isEqualTo(2);
        }

        time.sleep(100);
        final MutableRecordBatch batch1 = (MutableRecordBatch) MemoryRecords.withIdempotentRecords(22, Compression.NONE, producerId, producerEpoch, 3, 2, new SimpleRecord(new byte[10])).firstBatch();
        batch1.setMaxTimestamp(TimestampType.CREATE_TIME, time.milliseconds());
        psm.updateProducer(Map.of(0, Map.of(T1P0,
            List.of(batch1)
        )));
        assertThat(psm.producerStateManagers).hasSize(1);
        {
            final var maybeLastEntry = psm.producerStateManagers.get(T1P0).lastEntry(producerId);
            assertThat(maybeLastEntry).isPresent();
            final var lastEntry = maybeLastEntry.get();
            assertThat(lastEntry.producerId()).isEqualTo(producerId);
            assertThat(lastEntry.producerEpoch()).isEqualTo(producerEpoch);
            assertThat(lastEntry.firstDataOffset()).isEqualTo(19);
            assertThat(lastEntry.lastDataOffset()).isEqualTo(22);
            assertThat(lastEntry.lastTimestamp()).isEqualTo(time.milliseconds());
            assertThat(lastEntry.lastSeq()).isEqualTo(3);
        }
    }

    @Test
    void validateOnEmptyState() {
        // Given an existing producer state with empty state
        final ProducerStateManagers psm = new ProducerStateManagers(new MockTime(), config);
        final long producerId = 1L;
        final short producerEpoch = (short) 1;
        final int requestId = 0;
        final MemoryRecords records = MemoryRecords.withIdempotentRecords(
            19,
            Compression.NONE,
            producerId, producerEpoch, 0, 1,
            new SimpleRecord(new byte[10]), new SimpleRecord(new byte[10]), new SimpleRecord(new byte[10])
        );
        final List<RecordBatch> batches = List.of(records.firstBatch());
        // Then validate the producer state and return duplicate's metadata
        final var result = psm.validateProducer(Map.of(requestId, Map.of(T1P0, batches)));
        assertThat(result.duplicates()).hasSize(1);
        assertThat(result.duplicates().get(requestId))
            .hasSize(0);
    }

    @Test
    void validateOnExistingState() {
        final long producerId = 1L;
        // Given an existing producer state with empty state
        final ProducerStateManagers psm = new ProducerStateManagers(new MockTime(), config);
        final short producerEpoch = (short) 1;
        final var previous = Map.of(1, Map.of(T1P0,
            List.of(MemoryRecords.withIdempotentRecords(18, Compression.NONE, producerId, producerEpoch, 2, 2, new SimpleRecord(new byte[10])).firstBatch()))
        );
        psm.updateProducer(previous);
        final int requestId = 0;
        final MemoryRecords records = MemoryRecords.withIdempotentRecords(
            19,
            Compression.NONE,
            producerId, producerEpoch, 0, 1,
            new SimpleRecord(new byte[10]), new SimpleRecord(new byte[10]), new SimpleRecord(new byte[10])
        );
        final List<RecordBatch> batches = List.of(records.firstBatch());
        // Then validate the producer state and return duplicate's metadata
        final var result = psm.validateProducer(Map.of(requestId, Map.of(T1P0, batches)));
        assertThat(result.duplicates()).hasSize(1);
        assertThat(result.duplicates().get(requestId))
            .hasSize(0);
    }

    @Test
    void updateAndValidateDuplicate() {
        final Time time = new MockTime();
        // Given an existing producer state with empty state
        final ProducerStateManagers psm = new ProducerStateManagers(time, config);
        final long producerId = 1L;
        final short producerEpoch = (short) 1;

        // When update and add a new entry
        final List<RecordBatch> batches1 = new ArrayList<>();
        final MutableRecordBatch batch = (MutableRecordBatch) MemoryRecords.withIdempotentRecords(19, Compression.NONE, producerId, producerEpoch, 2, 2, new SimpleRecord(new byte[10])).firstBatch();
        batch.setMaxTimestamp(TimestampType.CREATE_TIME, time.milliseconds());
        batches1.add(batch);
        psm.updateProducer(Map.of(1, Map.of(T1P0,
            batches1)
        ));
        assertThat(psm.producerStateManagers).hasSize(1);

        final int requestId = 0;
        // Given a request with a batch that is a duplicate
        // Then validate the producer state and return duplicate's metadata
        final var result = psm.validateProducer(Map.of(requestId, Map.of(T1P0, batches1)));
        assertThat(result.duplicates()).hasSize(1);
        assertThat(result.duplicates().get(requestId))
            .hasSize(1)
            .containsValues(new DuplicatedBatchMetadata(
                19,
                new BatchMetadata(2, 19, 0, time.milliseconds())
            ));
    }
}