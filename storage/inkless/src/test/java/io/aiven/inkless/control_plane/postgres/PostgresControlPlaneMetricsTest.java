/*
 * Inkless
 * Copyright (C) 2024 - 2026 Aiven OY
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
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.utils.MockTime;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PostgresControlPlaneMetricsTest {

    MockTime time;
    PostgresControlPlaneMetrics metrics;

    @BeforeEach
    void setUp() {
        time = new MockTime(0, 0, 0);
        metrics = new PostgresControlPlaneMetrics(time);
    }

    @AfterEach
    void tearDown() {
        metrics.close();
    }

    @Test
    void lastSuccessfulQueryAgeMs_isMinusOneBeforeFirstQuery() {
        assertThat(metrics.commitFileMetrics.lastSuccessfulQueryTimeMs.get()).isEqualTo(-1L);
    }

    @Test
    void lastSuccessfulQueryAgeMs_recordedOnQueryCompletion() {
        time.setCurrentTimeMs(1_000L);
        metrics.onCommitFileCompleted(50L);

        assertThat(metrics.commitFileMetrics.lastSuccessfulQueryTimeMs.get()).isEqualTo(1_000L);
    }

    @Test
    void lastSuccessfulQueryAgeMs_updatesOnSubsequentQueries() {
        time.setCurrentTimeMs(1_000L);
        metrics.onCommitFileCompleted(50L);

        time.setCurrentTimeMs(3_000L);
        metrics.onCommitFileCompleted(30L);

        assertThat(metrics.commitFileMetrics.lastSuccessfulQueryTimeMs.get()).isEqualTo(3_000L);
    }

    @Test
    void lastSuccessfulQueryAgeMs_independentPerQueryType() {
        time.setCurrentTimeMs(1_000L);
        metrics.onCommitFileCompleted(50L);

        // FindBatches has not been called — its timestamp should still be -1.
        assertThat(metrics.commitFileMetrics.lastSuccessfulQueryTimeMs.get()).isEqualTo(1_000L);
        assertThat(metrics.findBatchesMetrics.lastSuccessfulQueryTimeMs.get()).isEqualTo(-1L);
    }
}
