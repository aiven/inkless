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
package io.aiven.inkless.common;

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import io.aiven.inkless.config.InklessConfig;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.MetadataView;
import io.aiven.inkless.storage_backend.common.StorageBackend;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class SharedStateTest {

    @Mock
    InklessConfig config;

    @Mock
    MetadataView metadataView;

    @Mock
    ControlPlane controlPlane;

    @Mock
    BrokerTopicStats brokerTopicStats;

    @Mock
    StorageBackend firstBackend;

    @Mock
    StorageBackend secondBackend;

    @Mock
    StorageBackend thirdBackend;

    @Mock
    StorageBackend fourthBackend;

    @BeforeEach
    void setupConfig() {
        when(config.fileCleanerRetentionPeriod()).thenReturn(Duration.ofMillis(2000));
        when(config.isBatchCoordinateCacheEnabled()).thenReturn(true);
        when(config.batchCoordinateCacheTtl()).thenReturn(Duration.ofMillis(100));
        when(config.cacheMaxCount()).thenReturn(10L);
        when(config.cacheExpirationLifespanSec()).thenReturn(30);
        when(config.cacheExpirationMaxIdleSec()).thenReturn(10);
        when(config.fetchLaggingConsumerThreadPoolSize()).thenReturn(1);
    }

    @Test
    void shouldCloseResourcesInReverseOrderOnFailure() throws Exception {
        final AtomicInteger storageCallCount = new AtomicInteger();

        when(config.storage(any(Metrics.class))).thenAnswer(invocation -> {
            int count = storageCallCount.incrementAndGet();
            if (count == 3) {
                throw new RuntimeException("Failure creating third storage");
            }
            return switch (count) {
                case 1 -> firstBackend;
                case 2 -> secondBackend;
                case 4 -> fourthBackend;
                default -> thirdBackend;
            };
        });

        assertThatThrownBy(() -> SharedState.initialize(
                Time.SYSTEM,
                1,
                config,
                metadataView,
                controlPlane,
                brokerTopicStats,
                () -> mock(LogConfig.class)
        )).isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to initialize SharedState");

        final InOrder inOrder = inOrder(firstBackend, secondBackend, thirdBackend, fourthBackend);
        inOrder.verify(firstBackend).close();
        inOrder.verify(secondBackend).close();
        inOrder.verify(thirdBackend, times(0)).close();
        inOrder.verify(fourthBackend, times(0)).close();
    }
}
