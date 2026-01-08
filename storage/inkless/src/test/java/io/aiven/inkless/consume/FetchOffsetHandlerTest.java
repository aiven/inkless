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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.ListOffsetsRequest;
import io.aiven.inkless.control_plane.ListOffsetsResponse;
import io.aiven.inkless.control_plane.MetadataView;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class FetchOffsetHandlerTest {
    static final String TOPIC_0 = "topic0";
    static final String TOPIC_1 = "topic1";
    static final String TOPIC_CLASSIC = "topic_classic";
    static final Uuid TOPIC_ID_0 = new Uuid(0, 1);
    static final Uuid TOPIC_ID_1 = new Uuid(0, 2);
    static final TopicPartition T0P0 = new TopicPartition(TOPIC_0, 0);
    static final TopicPartition T0P1 = new TopicPartition(TOPIC_0, 1);
    static final TopicPartition T1P1 = new TopicPartition(TOPIC_1, 1);
    static final TopicIdPartition TIDP_T0P0 = new TopicIdPartition(TOPIC_ID_0, T0P0);
    static final TopicIdPartition TIDP_T0P1 = new TopicIdPartition(TOPIC_ID_0, T0P1);
    static final TopicIdPartition TIDP_T1P1 = new TopicIdPartition(TOPIC_ID_1, T1P1);

    Time time = new MockTime();
    InklessFetchOffsetMetrics metrics = new InklessFetchOffsetMetrics(time);
    @Mock
    MetadataView metadataView;
    @Mock
    ControlPlane controlPlane;
    @Mock
    private ExecutorService executor;
    @Captor
    ArgumentCaptor<Runnable> runnableCaptor;

    @Test
    void mustHandle() {
        when(metadataView.isDisklessTopic(TOPIC_0)).thenReturn(true);
        when(metadataView.isDisklessTopic(TOPIC_1)).thenReturn(true);
        when(metadataView.isDisklessTopic(TOPIC_CLASSIC)).thenReturn(false);

        final FetchOffsetHandler.Job job = new FetchOffsetHandler.Job(metadataView, controlPlane, executor, time, metrics);
        assertThat(job.mustHandle(TOPIC_0)).isTrue();
        assertThat(job.mustHandle(TOPIC_1)).isTrue();
        assertThat(job.mustHandle(TOPIC_CLASSIC)).isFalse();
    }

    @Test
    void empty() {
        final FetchOffsetHandler.Job job = new FetchOffsetHandler.Job(metadataView, controlPlane, executor, time, metrics);

        job.start();

        verify(executor, never()).submit((Runnable) any());
        verify(controlPlane, never()).listOffsets(any());
    }

    @Test
    void globalSuccess() throws ExecutionException, InterruptedException {
        when(metadataView.getTopicId(TOPIC_0)).thenReturn(TOPIC_ID_0);
        when(metadataView.getTopicId(TOPIC_1)).thenReturn(TOPIC_ID_1);

        when(controlPlane.listOffsets(any())).thenAnswer((invocation) -> {
            // The order may be arbitrary.
            final List<ListOffsetsRequest> requests = invocation.getArgument(0);
            return requests.stream().map(r -> {
                if (r.topicIdPartition().equals(TIDP_T0P0)) {
                    return ListOffsetsResponse.success(TIDP_T0P0, -1, 100);
                } else if (r.topicIdPartition().equals(TIDP_T0P1)) {
                    return ListOffsetsResponse.unknownServerError(TIDP_T0P1);
                } else if (r.topicIdPartition().equals(TIDP_T1P1)) {
                    return ListOffsetsResponse.success(TIDP_T1P1, -3, 200);
                } else {
                    throw new RuntimeException();
                }
            }).toList();
        });

        final FetchOffsetHandler.Job job = new FetchOffsetHandler.Job(metadataView, controlPlane, executor, time, metrics);
        final var future1 = job.add(T0P0, new ListOffsetsRequestData.ListOffsetsPartition().setPartitionIndex(0).setTimestamp(-1));
        final var future2 = job.add(T0P1, new ListOffsetsRequestData.ListOffsetsPartition().setPartitionIndex(1).setTimestamp(1));
        final var future3 = job.add(T1P1, new ListOffsetsRequestData.ListOffsetsPartition().setPartitionIndex(1).setTimestamp(-3));

        job.start();

        verify(executor).submit(runnableCaptor.capture());
        runnableCaptor.getValue().run();

        assertThat(future1.isDone()).isTrue();
        assertThat(future1.get().exception()).isEmpty();
        assertThat(future1.get().timestampAndOffset()).contains(new FileRecords.TimestampAndOffset(-1, 100, Optional.of(0)));

        assertThat(future2.isDone()).isTrue();
        assertThat(future2.get().exception()).isNotEmpty();
        assertThat(future2.get().exception().get()).isInstanceOf(UnknownServerException.class);
        assertThat(future2.get().exception().get()).message().isEqualTo("The server experienced an unexpected error when processing the request.");
        assertThat(future2.get().timestampAndOffset()).isEmpty();

        assertThat(future3.isDone()).isTrue();
        assertThat(future3.get().exception()).isEmpty();
        assertThat(future3.get().timestampAndOffset()).contains(new FileRecords.TimestampAndOffset(-3, 200, Optional.of(0)));
    }

    @Test
    void globalFailure() throws ExecutionException, InterruptedException {
        when(metadataView.getTopicId(TOPIC_0)).thenReturn(TOPIC_ID_0);
        when(metadataView.getTopicId(TOPIC_1)).thenReturn(TOPIC_ID_1);

        when(controlPlane.listOffsets(any())).thenThrow(new UnknownServerException("error"));

        final FetchOffsetHandler.Job job = new FetchOffsetHandler.Job(metadataView, controlPlane, executor, time, metrics);
        final var future1 = job.add(T0P0, new ListOffsetsRequestData.ListOffsetsPartition().setPartitionIndex(0).setTimestamp(-1));
        final var future2 = job.add(T0P1, new ListOffsetsRequestData.ListOffsetsPartition().setPartitionIndex(1).setTimestamp(1));
        final var future3 = job.add(T1P1, new ListOffsetsRequestData.ListOffsetsPartition().setPartitionIndex(1).setTimestamp(-3));

        job.start();

        verify(executor).submit(runnableCaptor.capture());
        runnableCaptor.getValue().run();

        for (final var future : List.of(future1, future2, future3)) {
            assertThat(future.isDone()).isTrue();
            assertThat(future.get().exception()).isNotEmpty();
            assertThat(future.get().exception().get()).isInstanceOf(UnknownServerException.class);
            assertThat(future.get().exception().get()).message().isEqualTo("error");
            assertThat(future.get().timestampAndOffset()).isEmpty();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void cancellation(final boolean cancelBeforeStart) {
        when(metadataView.getTopicId(TOPIC_0)).thenReturn(TOPIC_ID_0);
        when(metadataView.getTopicId(TOPIC_1)).thenReturn(TOPIC_ID_1);

        final Future<?> submittedFuture = mock(Future.class);
        doReturn(submittedFuture).when(executor).submit((Runnable) any());

        final FetchOffsetHandler.Job job = new FetchOffsetHandler.Job(metadataView, controlPlane, executor, time, metrics);
        job.add(T0P0, new ListOffsetsRequestData.ListOffsetsPartition().setPartitionIndex(0).setTimestamp(-1));
        job.add(T0P1, new ListOffsetsRequestData.ListOffsetsPartition().setPartitionIndex(1).setTimestamp(1));
        job.add(T1P1, new ListOffsetsRequestData.ListOffsetsPartition().setPartitionIndex(1).setTimestamp(-3));

        if (cancelBeforeStart) {
            job.cancelHandler().cancel(true);
            job.start();
        } else {
            job.start();
            job.cancelHandler().cancel(true);
        }

        verify(submittedFuture).cancel(eq(true));
    }

    @Test
    void closeShutdownsExecutorService() throws IOException, InterruptedException {
        when(executor.awaitTermination(5, TimeUnit.SECONDS)).thenReturn(true);

        final InklessFetchOffsetMetrics metricsToClose = new InklessFetchOffsetMetrics(time);
        final FetchOffsetHandler handler = new FetchOffsetHandler(null, executor, time, metricsToClose);

        handler.close();

        verify(executor).shutdown();
        verify(executor).awaitTermination(5, TimeUnit.SECONDS);
    }
}
