package io.aiven.inkless.control_plane;

import java.time.Instant;

public record FileToDelete(String objectKey,
                           Instant markedForDeletionAt) {
}
