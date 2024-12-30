// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FileReasonTest {
    @Test
    void fromNameWrite() {
        assertThat(FileReason.fromName("produce")).isEqualTo(FileReason.PRODUCE);
    }

    @Test
    void fileNameUnknown() {
        assertThatThrownBy(() -> FileReason.fromName("x"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Unknown name x");
    }
}
