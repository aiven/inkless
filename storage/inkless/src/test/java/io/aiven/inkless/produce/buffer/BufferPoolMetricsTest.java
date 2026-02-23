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
package io.aiven.inkless.produce.buffer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * Tests for BufferPoolMetrics integration with BufferPool implementations.
 */
class BufferPoolMetricsTest {

    private static final int ONE_MB = 1024 * 1024;

    private ElasticBufferPool pool;
    private BufferPoolMetrics metrics;

    @BeforeEach
    void setUp() {
        pool = new ElasticBufferPool(4);
        metrics = new BufferPoolMetrics(pool);
    }

    @AfterEach
    void tearDown() {
        if (metrics != null) {
            metrics.close();
        }
        if (pool != null) {
            pool.close();
        }
    }

    @Test
    void construction_succeeds() {
        // Metrics was constructed successfully in setUp
        assertThat(metrics).isNotNull();
    }

    @Test
    void close_isIdempotent() {
        metrics.close();
        assertDoesNotThrow(() -> metrics.close());
        assertDoesNotThrow(() -> metrics.close());
        metrics = null;
    }

    @Test
    void metricsReflectPoolState_afterAcquire() {
        // Initial state
        assertThat(pool.activeBufferCount()).isZero();

        // Acquire buffers
        PooledBuffer buffer1 = pool.acquire(ONE_MB);
        assertThat(pool.activeBufferCount()).isEqualTo(1);
        assertThat(pool.poolGrowthCount()).isEqualTo(1);

        PooledBuffer buffer2 = pool.acquire(ONE_MB);
        assertThat(pool.activeBufferCount()).isEqualTo(2);
        assertThat(pool.poolGrowthCount()).isEqualTo(2);

        // Release
        buffer1.release();
        assertThat(pool.activeBufferCount()).isEqualTo(1);

        buffer2.release();
        assertThat(pool.activeBufferCount()).isZero();
    }

    @Test
    void metricsReflectPoolState_heapFallback() {
        // Exhaust the pool
        for (int i = 0; i < 4; i++) {
            pool.acquire(ONE_MB);
        }

        // Force heap fallback
        pool.acquire(ONE_MB);

        assertThat(pool.heapFallbackCount()).isEqualTo(1);
        assertThat(pool.heapFallbackBytes()).isEqualTo(ONE_MB);
    }

    @Test
    void metricsReflectPoolState_poolHit() {
        // Acquire and release to populate pool
        PooledBuffer buffer = pool.acquire(ONE_MB);
        buffer.release();

        // Next acquire should hit the pool
        PooledBuffer buffer2 = pool.acquire(ONE_MB);
        assertThat(pool.poolHitCount()).isEqualTo(1);

        buffer2.release();
    }

    @Test
    void metricsCanBeClosedAndRecreated() {
        metrics.close();

        // Recreate with same pool
        BufferPoolMetrics newMetrics = assertDoesNotThrow(() -> new BufferPoolMetrics(pool));
        newMetrics.close();
    }

    @Nested
    class NonElasticPoolTests {

        private BufferPoolMetrics nonElasticMetrics;

        @AfterEach
        void tearDownNonElastic() {
            if (nonElasticMetrics != null) {
                nonElasticMetrics.close();
            }
        }

        @Test
        void metricsWorkWithGenericBufferPool() {
            // Create a minimal BufferPool implementation (not ElasticBufferPool)
            BufferPool simplePool = new BufferPool() {
                @Override
                public PooledBuffer acquire(int sizeBytes) {
                    throw new UnsupportedOperationException("Not used in this test");
                }

                @Override
                public int activeBufferCount() {
                    return 42;
                }

                @Override
                public long totalPoolSizeBytes() {
                    return 1024 * 1024;
                }

                @Override
                public long heapFallbackCount() {
                    return 5;
                }

                @Override
                public long heapFallbackBytes() {
                    return 500;
                }

                @Override
                public void close() {
                    // No-op
                }
            };

            // Should not throw - elastic-specific metrics should be skipped
            nonElasticMetrics = assertDoesNotThrow(() -> new BufferPoolMetrics(simplePool));
            assertThat(nonElasticMetrics).isNotNull();

            // Close should not throw even without elastic-specific metrics
            assertDoesNotThrow(() -> nonElasticMetrics.close());
            nonElasticMetrics = null;
        }
    }
}
