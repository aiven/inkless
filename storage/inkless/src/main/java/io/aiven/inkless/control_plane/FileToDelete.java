package io.aiven.inkless.control_plane;

import java.time.Instant;

public record FileToDelete(String objectKey,
                           Instant markedForDeletionAt) {
    // TODO there will be more fields here, such as various timestamps to the deletion deadline, deletion moment, etc.
}
