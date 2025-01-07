/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aiven.inkless.consume;

import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.storage.log.FetchParams;

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
import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.common.PlainObjectKey;
import io.aiven.inkless.control_plane.ControlPlane;
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
    private static final ObjectKeyCreator OBJECT_KEY_CREATOR = PlainObjectKey.creator("");
    private static final KeyAlignmentStrategy KEY_ALIGNMENT_STRATEGY = new FixedBlockAlignment(Integer.MAX_VALUE);
    private static final ObjectCache OBJECT_CACHE = new NullCache();
    @Mock
    private ControlPlane controlPlane;
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
        reader = new Reader(time, OBJECT_KEY_CREATOR, KEY_ALIGNMENT_STRATEGY, OBJECT_CACHE, controlPlane, objectFetcher, metadataExecutor, fetchPlannerExecutor, dataExecutor, fetchCompleterExecutor);
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
