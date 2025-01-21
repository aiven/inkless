// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.common;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PlainObjectKeyTest {
    @Test
    void notAllowNullPrefix() {
        assertThatThrownBy(() -> PlainObjectKey.create(null, "suffix"))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("prefix cannot be null");
    }

    @Test
    void notAllowNullMainPath() {
        assertThatThrownBy(() -> PlainObjectKey.create("prefix", null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("name cannot be null");
    }

    @Test
    void value() {
        assertThat(PlainObjectKey.create("prefix", "suffix").value())
            .isEqualTo("prefix/suffix");
    }

    @Test
    void testToString() {
        assertThat(PlainObjectKey.from("prefix/suffix").toString())
            .isEqualTo("prefix/suffix");
    }
}
