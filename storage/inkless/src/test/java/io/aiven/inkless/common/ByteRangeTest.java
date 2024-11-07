// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.common;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ByteRangeTest {
    @Test
    void negativeOffset() {
        assertThatThrownBy(() -> new ByteRange(-1, 1))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("offset cannot be negative, -1 given");
    }

    @Test
    void negativeSize() {
        assertThatThrownBy(() -> new ByteRange(1, -1))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("size cannot be negative, -1 given");
    }

    @Test
    void empty() {
        assertThat(new ByteRange(1, 0).empty()).isTrue();
        assertThat(new ByteRange(1, 1).empty()).isFalse();
    }

    @Test
    void endOffset() {
        assertThat(new ByteRange(1, 1).endOffset()).isEqualTo(1);
        assertThat(new ByteRange(1, 2).endOffset()).isEqualTo(2);
    }

    @Test
    void maxRange() {
        assertThat(ByteRange.maxRange()).isEqualTo(new ByteRange(0, Integer.MAX_VALUE));
    }
}