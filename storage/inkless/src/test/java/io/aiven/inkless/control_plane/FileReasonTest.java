// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FileReasonTest {
    @Test
    void fromName() {
        assertThat(FileReason.fromName("produce")).isEqualTo(FileReason.PRODUCE);
        assertThat(FileReason.fromName("merge")).isEqualTo(FileReason.MERGE);
    }

    @Test
    void nameUnknown() {
        assertThatThrownBy(() -> FileReason.fromName("x"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Unknown name x");
    }
}
