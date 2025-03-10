/*
 * Inkless
 * Copyright (C) 2024 - 2025 Aiven OY
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
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
        assertThatThrownBy(() -> new ClosedFile(null, Map.of(), Map.of(), List.of(), new byte[1]))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("start cannot be null");
    }

    @Test
    void originalRequestsNull() {
        assertThatThrownBy(() -> new ClosedFile(Instant.EPOCH, null, Map.of(), List.of(), new byte[1]))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("originalRequests cannot be null");
    }

    @Test
    void awaitingFuturesByRequestNull() {
        assertThatThrownBy(() -> new ClosedFile(Instant.EPOCH, Map.of(), null, List.of(), new byte[1]))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("awaitingFuturesByRequest cannot be null");
    }

    @Test
    void commitBatchRequestsNull() {
        assertThatThrownBy(() -> new ClosedFile(Instant.EPOCH, Map.of(), Map.of(), null, new byte[1]))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("commitBatchRequests cannot be null");
    }

    @Test
    void requestsWithDifferentLengths() {
        assertThatThrownBy(() -> new ClosedFile(Instant.EPOCH, Map.of(1, Map.of()), Map.of(), List.of(), new byte[1]))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("originalRequests and awaitingFuturesByRequest must be of same size");
    }

    @Test
    void dataNull() {
        assertThatThrownBy(() -> new ClosedFile(Instant.EPOCH, Map.of(), Map.of(), List.of(), null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("data cannot be null");
    }

    @Test
    void dataRequestMismatch() {
        assertThatThrownBy(() -> new ClosedFile(Instant.EPOCH, Map.of(), Map.of(), List.of(), new byte[10]))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("data must be empty if commitBatchRequests is empty");
        assertThatThrownBy(() -> new ClosedFile(Instant.EPOCH, Map.of(), Map.of(),
            List.of(CommitBatchRequest.of(1, null, 0, 0, 0, 0, 0, TimestampType.CREATE_TIME)),
            new byte[0]))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("data must be empty if commitBatchRequests is empty");
    }

    @Test
    void size() {
        final int size = new ClosedFile(Instant.EPOCH, Map.of(), Map.of(),
            List.of(CommitBatchRequest.of(1, null, 0, 0, 0, 0, 0, TimestampType.CREATE_TIME)),
            new byte[10]).size();
        assertThat(size).isEqualTo(10);
    }
}
