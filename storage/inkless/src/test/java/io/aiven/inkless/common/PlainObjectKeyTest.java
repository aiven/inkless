// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.common;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PlainObjectKeyTest {
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
