// Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.common;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MaskedPrefixObjectKeyTest {
    @Test
    void notAllowNullPrefix() {
        assertThatThrownBy(() -> PlainObjectKey.create(null, "name"))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("prefix cannot be null");
    }

    @Test
    void notAllowNullMainPath() {
        assertThatThrownBy(() -> MaskedPrefixObjectKey.create("prefix", null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("name cannot be null");
    }

    @Test
    void value() {
        assertThat(MaskedPrefixObjectKey.create("realPrefix", "name").value())
            .isEqualTo("realPrefix/name");
    }

    @Test
    void testToString() {
        assertThat(MaskedPrefixObjectKey.from("realPrefix/name").toString())
            .isEqualTo("<prefix>/name");
    }
}
