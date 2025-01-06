// Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.common;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MaskedPrefixObjectKeyTest {
    @Test
    void notAllowNullPrefix() {
        assertThatThrownBy(() -> new PlainObjectKey(null, "/mainPath"))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("prefix cannot be null");
    }

    @Test
    void notAllowNullMainPath() {
        assertThatThrownBy(() -> new MaskedPrefixObjectKey("/prefix", null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("mainPath cannot be null");
    }

    @Test
    void value() {
        assertThat(new MaskedPrefixObjectKey("/realPrefix", "/mainPath").value())
            .isEqualTo("/realPrefix/mainPath");
    }

    @Test
    void testToString() {
        assertThat(new MaskedPrefixObjectKey("/realPrefix", "/mainPath").toString())
            .isEqualTo("<prefix>/mainPath");
    }
}
