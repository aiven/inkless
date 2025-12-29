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
package io.aiven.inkless.consume;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.aiven.inkless.cache.FixedBlockAlignment;
import io.aiven.inkless.cache.KeyAlignmentStrategy;
import io.aiven.inkless.cache.NullCache;
import io.aiven.inkless.cache.ObjectCache;
import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.common.PlainObjectKey;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.generated.FileExtent;
import io.aiven.inkless.storage_backend.common.ObjectFetcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class ReaderTest {
    private static final ObjectKeyCreator OBJECT_KEY_CREATOR = ObjectKey.creator("", false);
    private static final KeyAlignmentStrategy KEY_ALIGNMENT_STRATEGY = new FixedBlockAlignment(Integer.MAX_VALUE);
    private static final ObjectCache OBJECT_CACHE = new NullCache();

    @Spy
    private final ExecutorService metadataExecutor = Executors.newSingleThreadExecutor();
    @Spy
    private final ExecutorService dataExecutor = Executors.newSingleThreadExecutor();

    @Mock
    private ControlPlane controlPlane;
    @Mock
    private ObjectFetcher objectFetcher;
    @Mock
    private FetchParams fetchParams;

    private final Time time = new MockTime();

    @Test
    public void testReaderEmptyRequests() throws IOException {
        try(final var reader = new Reader(time, OBJECT_KEY_CREATOR, KEY_ALIGNMENT_STRATEGY, OBJECT_CACHE, controlPlane, objectFetcher, 0, metadataExecutor, dataExecutor, new BrokerTopicStats())) {
            final CompletableFuture<Map<TopicIdPartition, FetchPartitionData>> fetch = reader.fetch(fetchParams, Collections.emptyMap());
            verify(metadataExecutor, atLeastOnce()).execute(any());
            verifyNoInteractions(dataExecutor);
            assertThat(fetch.join()).isEqualTo(Collections.emptyMap());
        }
    }

    @Test
    public void testClose() throws Exception {
        final var reader = new Reader(time, OBJECT_KEY_CREATOR, KEY_ALIGNMENT_STRATEGY, OBJECT_CACHE, controlPlane, objectFetcher, 0, metadataExecutor, dataExecutor, new BrokerTopicStats());
        reader.close();
        verify(metadataExecutor, atLeastOnce()).shutdown();
        verify(dataExecutor, atLeastOnce()).shutdown();
    }

    @Nested
    class AllOfFileExtentsTests {
        /**
         * Tests that allOfFileExtents preserves the original input order regardless of completion order.
         */
        @Test
        public void testAllOfFileExtentsPreservesOrder() {
            // Create file extents with distinct identifiers
            final ObjectKey objectKeyA = PlainObjectKey.create("prefix", "object-a");
            final ObjectKey objectKeyB = PlainObjectKey.create("prefix", "object-b");
            final ObjectKey objectKeyC = PlainObjectKey.create("prefix", "object-c");

            final FileExtent extentA = FileFetchJob.createFileExtent(objectKeyA, new ByteRange(0, 10), ByteBuffer.allocate(10));
            final FileExtent extentB = FileFetchJob.createFileExtent(objectKeyB, new ByteRange(0, 10), ByteBuffer.allocate(10));
            final FileExtent extentC = FileFetchJob.createFileExtent(objectKeyC, new ByteRange(0, 10), ByteBuffer.allocate(10));

            // Create uncompleted futures
            final CompletableFuture<FileExtent> futureA = new CompletableFuture<>();
            final CompletableFuture<FileExtent> futureB = new CompletableFuture<>();
            final CompletableFuture<FileExtent> futureC = new CompletableFuture<>();

            // Complete in reverse order: C, B, A
            futureC.complete(extentC);
            futureB.complete(extentB);
            futureA.complete(extentA);

            // Create the ordered list: A, B, C
            final List<CompletableFuture<FileExtent>> orderedFutures = List.of(futureA, futureB, futureC);

            // Call allOfFileExtents
            final CompletableFuture<List<FileExtent>> resultFuture = Reader.allOfFileExtents(orderedFutures);

            // Verify result order is preserved as A, B, C (not C, B, A which was the completion order)
            final List<FileExtent> result = resultFuture.join();
            assertThat(result)
                .hasSize(3)
                .extracting(FileExtent::object)
                .containsExactly(objectKeyA.value(), objectKeyB.value(), objectKeyC.value());
        }

        /**
         * Tests that allOfFileExtents returns immediately without blocking the calling thread.
         */
        @Test
        public void testAllOfFileExtentsDoesNotBlock() {
            // Create an incomplete future
            final CompletableFuture<FileExtent> incompleteFuture = new CompletableFuture<>();

            final List<CompletableFuture<FileExtent>> futures = List.of(incompleteFuture);

            // Call allOfFileExtents - this should return immediately without blocking
            final CompletableFuture<List<FileExtent>> resultFuture = Reader.allOfFileExtents(futures);

            // Verify the result is not yet complete (proves non-blocking behavior)
            assertThat(resultFuture).isNotCompleted();

            // Complete the input future
            final ObjectKey objectKey = PlainObjectKey.create("prefix", "object");
            final FileExtent extent = FileFetchJob.createFileExtent(objectKey, new ByteRange(0, 10), ByteBuffer.allocate(10));
            incompleteFuture.complete(extent);

            // Verify the result completes and contains the expected value
            assertThat(resultFuture.join())
                .hasSize(1)
                .extracting(FileExtent::object)
                .containsExactly(objectKey.value());
        }
    }






    }
}
