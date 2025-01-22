// Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
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

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class FileCleanerTest {
    public static final Duration RETENTION_PERIOD = Duration.ofMinutes(10);
    Time time = new MockTime();
    
    @Mock
    ControlPlane controlPlane;
    @Mock
    StorageBackend storageBackend;

    static final ObjectKeyCreator OBJECT_KEY_CREATOR = ObjectKey.creator("", false);

    @Test
    void empty() throws Exception {
        final var cleaner = new FileCleaner(time, controlPlane, storageBackend, OBJECT_KEY_CREATOR, RETENTION_PERIOD);
        when(controlPlane.getFilesToDelete()).thenReturn(List.of());

        cleaner.run();

        verify(storageBackend, times(0)).delete(Set.of());
    }

    @Test
    void singleWithinRetention() throws Exception {
        final var cleaner = new FileCleaner(time, controlPlane, storageBackend, OBJECT_KEY_CREATOR, RETENTION_PERIOD);
        final var objectKey = OBJECT_KEY_CREATOR.from("key");
        final var now = TimeUtils.now(time);
        when(controlPlane.getFilesToDelete())
            .thenReturn(List.of(new FileToDelete(objectKey.value(), now.minus(Duration.ofMinutes(15)))));

        cleaner.run();

        verify(storageBackend, times(1)).delete(Set.of(objectKey));
        verify(controlPlane, times(1)).deleteFiles(new DeleteFilesRequest(Set.of(objectKey.value())));
    }

    @Test
    void singleOutsideRetention() throws Exception {
        final var cleaner = new FileCleaner(time, controlPlane, storageBackend, OBJECT_KEY_CREATOR, RETENTION_PERIOD);
        final var objectKey = OBJECT_KEY_CREATOR.from("key");
        final var now = TimeUtils.now(time);
        when(controlPlane.getFilesToDelete())
            .thenReturn(List.of(new FileToDelete(objectKey.value(), now.minus(Duration.ofMinutes(5)))));

        cleaner.run();

        verify(storageBackend, times(0)).delete(Set.of());
    }

    @Test
    void multiple() throws Exception {
        final var cleaner = new FileCleaner(time, controlPlane, storageBackend, OBJECT_KEY_CREATOR, RETENTION_PERIOD);
        final var objectKeys = List.of(OBJECT_KEY_CREATOR.from("key1"), OBJECT_KEY_CREATOR.create("key3"));
        when(controlPlane.getFilesToDelete())
            .thenReturn(List.of(
                new FileToDelete(objectKeys.get(0).value(), TimeUtils.now(time).minus(Duration.ofMinutes(15))),
                new FileToDelete(OBJECT_KEY_CREATOR.create("key2").value(), TimeUtils.now(time).minus(Duration.ofMinutes(5))),
                new FileToDelete(objectKeys.get(1).value(), TimeUtils.now(time).minus(Duration.ofMinutes(15)))
            ));

        cleaner.run();

        verify(storageBackend, times(1)).delete(new HashSet<>(objectKeys));
        verify(controlPlane, times(1)).deleteFiles(new DeleteFilesRequest(objectKeys.stream().map(ObjectKey::value).collect(Collectors.toSet())));
    }
}
