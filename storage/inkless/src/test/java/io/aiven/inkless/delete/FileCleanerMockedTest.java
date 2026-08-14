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
package io.aiven.inkless.delete;

import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.DeleteFilesRequest;
import io.aiven.inkless.control_plane.FileToDelete;
import io.aiven.inkless.storage_backend.common.StorageBackend;
import io.aiven.inkless.storage_backend.common.StorageBackendException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class FileCleanerMockedTest {
    public static final Duration RETENTION_PERIOD = Duration.ofMinutes(10);
    public static final int MAX_FILES_PER_CYCLE = 3;
    Time time = new MockTime();
    
    @Mock
    ControlPlane controlPlane;
    @Mock
    StorageBackend storageBackend;

    static final ObjectKeyCreator OBJECT_KEY_CREATOR = ObjectKey.creator("", false);

    @Test
    void empty() throws Exception {
        final var cleaner = new FileCleaner(time, controlPlane, storageBackend, OBJECT_KEY_CREATOR, RETENTION_PERIOD, MAX_FILES_PER_CYCLE);
        when(controlPlane.getFilesToDelete(any(), anyInt())).thenReturn(List.of());

        cleaner.run();

        verify(storageBackend, times(0)).delete(Set.of());
    }

    @Test
    void passesGracePeriodAndMaxFilesPerCycleToControlPlane() throws Exception {
        final var cleaner = new FileCleaner(time, controlPlane, storageBackend, OBJECT_KEY_CREATOR, RETENTION_PERIOD, MAX_FILES_PER_CYCLE);
        final var now = TimeUtils.now(time);
        when(controlPlane.getFilesToDelete(any(), anyInt())).thenReturn(List.of());

        cleaner.run();

        // One beyond the cap: the extra row is the saturation probe, not part of the delete budget.
        verify(controlPlane, times(1)).getFilesToDelete(eq(now.minus(RETENTION_PERIOD)), eq(MAX_FILES_PER_CYCLE + 1));
        verify(storageBackend, times(0)).delete(Set.of());
    }

    @Test
    void single() throws Exception {
        final var cleaner = new FileCleaner(time, controlPlane, storageBackend, OBJECT_KEY_CREATOR, RETENTION_PERIOD, MAX_FILES_PER_CYCLE);
        final var objectKey = OBJECT_KEY_CREATOR.from("key");
        final var now = TimeUtils.now(time);
        when(controlPlane.getFilesToDelete(any(), anyInt()))
            .thenReturn(List.of(new FileToDelete(objectKey.value(), now.minus(Duration.ofMinutes(15)))));
        when(storageBackend.delete(Set.of(objectKey))).thenReturn(Set.of(objectKey));

        cleaner.run();

        verify(storageBackend, times(1)).delete(Set.of(objectKey));
        verify(controlPlane, times(1)).deleteFiles(new DeleteFilesRequest(Set.of(objectKey.value())));
        assertEquals(0, cleaner.metrics.fileCleanerFilesFailed.sum());
    }

    @Test
    void multiple() throws Exception {
        final var cleaner = new FileCleaner(time, controlPlane, storageBackend, OBJECT_KEY_CREATOR, RETENTION_PERIOD, MAX_FILES_PER_CYCLE);
        final var objectKeys = List.of(OBJECT_KEY_CREATOR.from("key1"), OBJECT_KEY_CREATOR.create("key3"));
        when(controlPlane.getFilesToDelete(any(), anyInt()))
            .thenReturn(List.of(
                new FileToDelete(objectKeys.get(0).value(), TimeUtils.now(time).minus(Duration.ofMinutes(15))),
                new FileToDelete(objectKeys.get(1).value(), TimeUtils.now(time).minus(Duration.ofMinutes(15)))
            ));
        when(storageBackend.delete(new HashSet<>(objectKeys))).thenReturn(new HashSet<>(objectKeys));

        cleaner.run();

        verify(storageBackend, times(1)).delete(new HashSet<>(objectKeys));
        verify(controlPlane, times(1)).deleteFiles(new DeleteFilesRequest(objectKeys.stream().map(ObjectKey::value).collect(Collectors.toSet())));
    }

    @Test
    void dereferencesOnlyKeysConfirmedDeleted() throws Exception {
        final var cleaner = new FileCleaner(time, controlPlane, storageBackend, OBJECT_KEY_CREATOR, RETENTION_PERIOD, MAX_FILES_PER_CYCLE);
        final var deleted = OBJECT_KEY_CREATOR.from("deleted");
        final var throttled = OBJECT_KEY_CREATOR.from("throttled");
        final var now = TimeUtils.now(time);
        when(controlPlane.getFilesToDelete(any(), anyInt()))
            .thenReturn(List.of(
                new FileToDelete(deleted.value(), now.minus(Duration.ofMinutes(15))),
                new FileToDelete(throttled.value(), now.minus(Duration.ofMinutes(15)))
            ));
        // Storage confirms only one key; the other was not deleted (e.g. throttled).
        when(storageBackend.delete(Set.of(deleted, throttled))).thenReturn(Set.of(deleted));

        cleaner.run();

        // Only the confirmed key is dereferenced; the throttled one stays for the next cycle.
        verify(controlPlane, times(1)).deleteFiles(new DeleteFilesRequest(Set.of(deleted.value())));
        assertEquals(1, cleaner.metrics.fileCleanerFilesFailed.sum());
    }

    @Test
    void skipsControlPlaneWhenNothingDeleted() throws Exception {
        final var cleaner = new FileCleaner(time, controlPlane, storageBackend, OBJECT_KEY_CREATOR, RETENTION_PERIOD, MAX_FILES_PER_CYCLE);
        final var objectKey = OBJECT_KEY_CREATOR.from("key");
        final var now = TimeUtils.now(time);
        when(controlPlane.getFilesToDelete(any(), anyInt()))
            .thenReturn(List.of(new FileToDelete(objectKey.value(), now.minus(Duration.ofMinutes(15)))));
        when(storageBackend.delete(Set.of(objectKey))).thenReturn(Set.of());

        cleaner.run();

        // Nothing drained (e.g. throttled): the control plane is left untouched so the keys stay marked
        // for the next cycle. Cadence between cycles is set by the scheduler, not an in-run sleep.
        verify(controlPlane, times(0)).deleteFiles(any());
    }

    @Test
    void swallowsStorageFailureWithoutTouchingControlPlane() throws Exception {
        final var cleaner = new FileCleaner(time, controlPlane, storageBackend, OBJECT_KEY_CREATOR, RETENTION_PERIOD, MAX_FILES_PER_CYCLE);
        final var objectKey = OBJECT_KEY_CREATOR.from("key");
        final var now = TimeUtils.now(time);
        when(controlPlane.getFilesToDelete(any(), anyInt()))
            .thenReturn(List.of(new FileToDelete(objectKey.value(), now.minus(Duration.ofMinutes(15)))));
        when(storageBackend.delete(Set.of(objectKey))).thenThrow(new StorageBackendException("boom"));

        // run() catches the failure, records an error, and neither propagates nor touches the control plane.
        cleaner.run();

        verify(controlPlane, times(0)).deleteFiles(any());
    }

    @Test
    void tracksLastSuccessfulCycle() throws Exception {
        final var cleaner = new FileCleaner(time, controlPlane, storageBackend, OBJECT_KEY_CREATOR, RETENTION_PERIOD, MAX_FILES_PER_CYCLE);
        when(controlPlane.getFilesToDelete(any(), anyInt())).thenReturn(List.of());

        assertEquals(-1, cleaner.metrics.lastSuccessfulCleanupTimeMs.get());

        cleaner.run();

        // A cycle with no work still counts: the gauge answers "is the cleaner running", not "is it deleting".
        assertEquals(time.milliseconds(), cleaner.metrics.lastSuccessfulCleanupTimeMs.get());
    }

    @Test
    void doesNotTrackFailedCycleAsSuccessful() throws Exception {
        final var cleaner = new FileCleaner(time, controlPlane, storageBackend, OBJECT_KEY_CREATOR, RETENTION_PERIOD, MAX_FILES_PER_CYCLE);
        when(controlPlane.getFilesToDelete(any(), anyInt())).thenThrow(new RuntimeException("boom"));

        cleaner.run();

        assertEquals(-1, cleaner.metrics.lastSuccessfulCleanupTimeMs.get());
    }

    @Test
    void doesNotRecordSaturatedCycleWhenMaxFilesPerCycleReached() throws Exception {
        final var cleaner = new FileCleaner(time, controlPlane, storageBackend, OBJECT_KEY_CREATOR, RETENTION_PERIOD, MAX_FILES_PER_CYCLE);
        final var now = TimeUtils.now(time);
        final List<FileToDelete> returned = new ArrayList<>();
        for (int i = 0; i < MAX_FILES_PER_CYCLE; i++) {
            returned.add(new FileToDelete(OBJECT_KEY_CREATOR.from("key" + i).value(), now.minus(Duration.ofMinutes(15))));
        }
        when(controlPlane.getFilesToDelete(any(), eq(MAX_FILES_PER_CYCLE + 1))).thenReturn(returned);

        cleaner.run();

        assertEquals(0, cleaner.metrics.fileCleanerCycleSaturated.sum());
        verify(storageBackend).delete(argThat((Set<ObjectKey> keys) -> keys.size() == MAX_FILES_PER_CYCLE));
    }

    @Test
    void recordsSaturatedCycleWhenMaxFilesPlusOnePerCycleReached() throws Exception {
        final var cleaner = new FileCleaner(time, controlPlane, storageBackend, OBJECT_KEY_CREATOR, RETENTION_PERIOD, MAX_FILES_PER_CYCLE);
        final var now = TimeUtils.now(time);
        final List<FileToDelete> returned = new ArrayList<>();
        // max + 1 to calculate saturation
        for (int i = 0; i < MAX_FILES_PER_CYCLE + 1; i++) {
            returned.add(new FileToDelete(OBJECT_KEY_CREATOR.from("key" + i).value(), now.minus(Duration.ofMinutes(15))));
        }
        when(controlPlane.getFilesToDelete(any(), eq(MAX_FILES_PER_CYCLE + 1))).thenReturn(returned);

        cleaner.run();

        assertEquals(1, cleaner.metrics.fileCleanerCycleSaturated.sum());
        verify(storageBackend).delete(argThat((Set<ObjectKey> keys) -> keys.size() == MAX_FILES_PER_CYCLE));
    }

    @Test
    void doesNotOverflowTheProbeAtMaxFilesPerCycleLimit() throws Exception {
        final var cleaner = new FileCleaner(time, controlPlane, storageBackend, OBJECT_KEY_CREATOR, RETENTION_PERIOD, Integer.MAX_VALUE);
        final var now = TimeUtils.now(time);
        when(controlPlane.getFilesToDelete(any(), anyInt()))
            .thenReturn(List.of(new FileToDelete(OBJECT_KEY_CREATOR.from("key").value(), now.minus(Duration.ofMinutes(15)))));

        cleaner.run();

        // A negative limit would read as unbounded in both control planes, removing the cap entirely.
        verify(controlPlane, times(1)).getFilesToDelete(any(), eq(Integer.MAX_VALUE));
        assertEquals(0, cleaner.metrics.fileCleanerCycleSaturated.sum());
    }

    @Test
    void doesNotRecordSaturatedCycleBelowMaxFilesPerCycle() throws Exception {
        final var cleaner = new FileCleaner(time, controlPlane, storageBackend, OBJECT_KEY_CREATOR, RETENTION_PERIOD, MAX_FILES_PER_CYCLE);
        final var now = TimeUtils.now(time);
        when(controlPlane.getFilesToDelete(any(), anyInt()))
            .thenReturn(List.of(new FileToDelete(OBJECT_KEY_CREATOR.from("key").value(), now.minus(Duration.ofMinutes(15)))));

        cleaner.run();

        assertEquals(0, cleaner.metrics.fileCleanerCycleSaturated.sum());
    }
}
