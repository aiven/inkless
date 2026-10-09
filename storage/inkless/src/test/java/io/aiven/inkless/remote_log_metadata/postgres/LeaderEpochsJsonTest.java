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
package io.aiven.inkless.remote_log_metadata.postgres;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LeaderEpochsJsonTest {
    @Test
    void roundTrip() {
        final Map<Integer, Long> epochs = new TreeMap<>(Map.of(0, 0L, 1, 20L, 12, 10_000_000_000L));
        assertEquals("{\"0\":0,\"1\":20,\"12\":10000000000}", RemoteLogMetadataDao.leaderEpochsToJson(epochs));
        assertEquals(epochs, RemoteLogMetadataDao.leaderEpochsFromJson("{\"0\":0,\"1\":20,\"12\":10000000000}"));
    }

    @Test
    void singleEntry() {
        assertEquals(Map.of(3, 42L), RemoteLogMetadataDao.leaderEpochsFromJson("{\"3\":42}"));
    }

    @Test
    void rejectsEmptyAndMalformed() {
        assertThrows(IllegalStateException.class, () -> RemoteLogMetadataDao.leaderEpochsFromJson("{}"));
        assertThrows(IllegalStateException.class, () -> RemoteLogMetadataDao.leaderEpochsFromJson(""));
        assertThrows(IllegalStateException.class, () -> RemoteLogMetadataDao.leaderEpochsFromJson("{0:0}"));
        assertThrows(IllegalStateException.class, () -> RemoteLogMetadataDao.leaderEpochsFromJson("{\"0\":}"));
        assertThrows(IllegalStateException.class, () -> RemoteLogMetadataDao.leaderEpochsFromJson("{\"x\":0}"));
        assertThrows(IllegalStateException.class,
            () -> RemoteLogMetadataDao.leaderEpochsFromJson("{\"0\":0},{\"1\":1}"));
    }
}
