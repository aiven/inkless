// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.common;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PlainObjectKeyTest {
    @Test
    void notAllowNullPrefix() {
        assertThatThrownBy(() -> new PlainObjectKey(null, "/suffix"))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("prefix cannot be null");
    }

    @Test
    void notAllowNullMainPath() {
        assertThatThrownBy(() -> new PlainObjectKey("/prefix", null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("mainPath cannot be null");
    }

    @Test
    void value() {
        assertThat(new PlainObjectKey("/prefix", "/suffix").value())
            .isEqualTo("/prefix/suffix");
    }

    @Test
    void testToString() {
        assertThat(new PlainObjectKey("/prefix", "/suffix").toString())
            .isEqualTo("/prefix/suffix");
    }
}
