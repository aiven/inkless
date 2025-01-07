// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import org.apache.kafka.common.record.TimestampType;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import io.aiven.inkless.control_plane.CommitBatchRequest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ClosedFileTest {
    @Test
    void startNull() {
        assertThatThrownBy(() -> new ClosedFile(null, Map.of(), Map.of(), List.of(), List.of(), new byte[1]))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("start cannot be null");
    }

    @Test
    void originalRequestsNull() {
        assertThatThrownBy(() -> new ClosedFile(Instant.EPOCH, null, Map.of(), List.of(), List.of(), new byte[1]))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("originalRequests cannot be null");
    }

    @Test
    void awaitingFuturesByRequestNull() {
        assertThatThrownBy(() -> new ClosedFile(Instant.EPOCH, Map.of(), null, List.of(), List.of(), new byte[1]))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("awaitingFuturesByRequest cannot be null");
    }

    @Test
    void commitBatchRequestsNull() {
        assertThatThrownBy(() -> new ClosedFile(Instant.EPOCH, Map.of(), Map.of(), null, List.of(), new byte[1]))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("commitBatchRequests cannot be null");
    }

    @Test
    void requestIdsNull() {
        assertThatThrownBy(() -> new ClosedFile(Instant.EPOCH, Map.of(), Map.of(), List.of(), null, new byte[1]))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("requestIds cannot be null");
    }

    @Test
    void differentLengths1() {
        assertThatThrownBy(() -> new ClosedFile(Instant.EPOCH, Map.of(1, Map.of()), Map.of(), List.of(), List.of(),new byte[1]))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("originalRequests and awaitingFuturesByRequest must be of same size");
    }

    @Test
    void differentLengths2() {
        assertThatThrownBy(() -> new ClosedFile(
            Instant.EPOCH,
            Map.of(), Map.of(),
            List.of(new CommitBatchRequest(null, 0, 0, 0, 0, TimestampType.CREATE_TIME)),
            List.of(),
            new byte[1]))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("commitBatchRequests and requestIds must be of same size");
    }

    @Test
    void dataNull() {
        assertThatThrownBy(() -> new ClosedFile(Instant.EPOCH, Map.of(), Map.of(), List.of(), List.of(), null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("data cannot be null");
    }


    @Test
    void size() {
        final int size = new ClosedFile(Instant.EPOCH, Map.of(), Map.of(), List.of(), List.of(), new byte[10]).size();
        assertThat(size).isEqualTo(10);
    }
}
