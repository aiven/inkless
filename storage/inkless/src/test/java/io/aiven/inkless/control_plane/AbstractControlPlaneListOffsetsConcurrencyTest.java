/*
 * Inkless
 * Copyright (C) 2026 Aiven OY
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
package io.aiven.inkless.control_plane;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.common.requests.ListOffsetsRequest.LATEST_TIMESTAMP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

/**
 * Pins that {@link AbstractControlPlane#listOffsets} does not serialize callers.
 * A slow implementation must only block its own request, not every ListOffsets on the broker.
 */
class AbstractControlPlaneListOffsetsConcurrencyTest {
    private static final TopicIdPartition PARTITION = new TopicIdPartition(Uuid.randomUuid(), 0, "topic");

    @Test
    void concurrentCallersAreNotSerialized() throws Exception {
        final int callers = 2;
        final CountDownLatch entered = new CountDownLatch(callers);
        final CountDownLatch release = new CountDownLatch(1);

        final AbstractControlPlane controlPlane = mock(AbstractControlPlane.class,
            withSettings().defaultAnswer(CALLS_REAL_METHODS));
        doAnswer(invocation -> {
            entered.countDown();
            release.await();
            return List.of(ListOffsetsResponse.success(PARTITION, -1, 0)).iterator();
        }).when(controlPlane).listOffsetsForExistingPartitions(any());

        final ExecutorService executor = Executors.newFixedThreadPool(callers);
        try {
            final List<Future<List<ListOffsetsResponse>>> results = List.of(
                executor.submit(() -> controlPlane.listOffsets(requests())),
                executor.submit(() -> controlPlane.listOffsets(requests()))
            );

            assertThat(entered.await(5, TimeUnit.SECONDS))
                .as("both callers should be inside listOffsetsForExistingPartitions at the same time")
                .isTrue();

            release.countDown();
            for (final var result : results) {
                assertThat(result.get(5, TimeUnit.SECONDS)).hasSize(1);
            }
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    private static List<ListOffsetsRequest> requests() {
        return List.of(new ListOffsetsRequest(PARTITION, LATEST_TIMESTAMP));
    }
}
