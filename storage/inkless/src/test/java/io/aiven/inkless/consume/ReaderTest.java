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

import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import io.aiven.inkless.cache.FixedBlockAlignment;
import io.aiven.inkless.cache.KeyAlignmentStrategy;
import io.aiven.inkless.cache.NullCache;
import io.aiven.inkless.cache.ObjectCache;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.MetadataView;
import io.aiven.inkless.storage_backend.common.ObjectFetcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class ReaderTest {
    private static final ObjectKeyCreator OBJECT_KEY_CREATOR = ObjectKey.creator("", false);
    private static final KeyAlignmentStrategy KEY_ALIGNMENT_STRATEGY = new FixedBlockAlignment(Integer.MAX_VALUE);
    private static final ObjectCache OBJECT_CACHE = new NullCache();
    @Mock
    private ControlPlane controlPlane;
    @Mock
    private MetadataView metadataView;
    @Mock
    private ObjectFetcher objectFetcher;
    @Mock
    private ExecutorService metadataExecutor;
    @Mock
    private ExecutorService fetchPlannerExecutor;
    @Mock
    private ExecutorService dataExecutor;
    @Mock
    private ExecutorService fetchCompleterExecutor;
    @Mock
    private FetchParams fetchParams;

    private final Time time = new MockTime();
    private Reader reader;

    @BeforeEach
    public void setup() {
        reader = new Reader(time, OBJECT_KEY_CREATOR, KEY_ALIGNMENT_STRATEGY, OBJECT_CACHE, controlPlane, objectFetcher, metadataExecutor, fetchPlannerExecutor, dataExecutor, fetchCompleterExecutor, new BrokerTopicStats());
    }

    @Test
    public void testReaderEmptyRequests() {
        when(metadataExecutor.submit(any(FindBatchesJob.class))).thenReturn(CompletableFuture.completedFuture(Collections.emptyMap()));
        when(fetchPlannerExecutor.submit(any(FetchPlannerJob.class))).thenReturn(CompletableFuture.completedFuture(Collections.emptyList()));
        doAnswer(invocation -> {
            invocation.getArgument(0, Runnable.class).run();
            return null;
        }).when(fetchCompleterExecutor).execute(any());
        assertThat(reader.fetch(fetchParams, Collections.emptyMap()))
                .isCompletedWithValue(Collections.emptyMap());
    }

    @Test
    public void testClose() {
        reader.close();
        verify(metadataExecutor, atLeastOnce()).shutdown();
        verify(metadataExecutor, atLeastOnce()).shutdown();
        verify(metadataExecutor, atLeastOnce()).shutdown();
        verify(metadataExecutor, atLeastOnce()).shutdown();
    }
}
