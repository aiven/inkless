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
package io.aiven.inkless.metadata;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClientAzAwarenessMetricsTest {

    @Test
    void testRecordClientAzUnaware() {
        final ClientAzAwarenessMetrics metrics = new ClientAzAwarenessMetrics();
        metrics.recordClientAz(null, false);
        assertEquals(0, metrics.clientAzHitRate.count());
        assertEquals(0, metrics.clientAzMissRate.count());
        assertEquals(1, metrics.clientAzUnawareRate.count());
        metrics.close();
    }

    @Test
    void testRecordClientAzMiss() {
        final ClientAzAwarenessMetrics metrics = new ClientAzAwarenessMetrics();
        metrics.recordClientAz("us-east-1a", false);
        assertEquals(0, metrics.clientAzHitRate.count());
        assertEquals(1, metrics.clientAzMissRate.count());
        assertEquals(0, metrics.clientAzUnawareRate.count());
        metrics.close();
    }

    @Test
    void testRecordClientAzHit() {
        final ClientAzAwarenessMetrics metrics = new ClientAzAwarenessMetrics();
        metrics.recordClientAz("us-east-1a", true);
        assertEquals(1, metrics.clientAzHitRate.count());
        assertEquals(0, metrics.clientAzMissRate.count());
        assertEquals(0, metrics.clientAzUnawareRate.count());
        assertTrue(metrics.clientAzHitRatesPerAz.containsKey("us-east-1a"));
        assertEquals(1, metrics.clientAzHitRatesPerAz.get("us-east-1a").count());
        metrics.close();
    }
}