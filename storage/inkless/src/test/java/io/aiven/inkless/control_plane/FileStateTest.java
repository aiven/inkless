// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FileStateTest {
    @Test
    void fromNameWrite() {
        assertThat(FileState.fromName("uploaded")).isEqualTo(FileState.UPLOADED);
        assertThat(FileState.fromName("deleting")).isEqualTo(FileState.DELETING);
    }

    @Test
    void fileNameUnknown() {
        assertThatThrownBy(() -> FileState.fromName("x"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Unknown name x");
    }
}
