// Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.common.config.validators;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.ObjectKeyCreator;

import static org.assertj.core.api.Assertions.assertThat;

class ObjectKeyTest {
    @ParameterizedTest
    @CsvSource({
        "true, <prefix>/mainPath",
        "false, /realPrefix/mainPath"
    })
    void testCreator(boolean masked, String printed) {
        ObjectKeyCreator objectKeyCreator = ObjectKey.create("/realPrefix", masked);
        final ObjectKey objectKey = objectKeyCreator.create("/mainPath");
        assertThat(objectKey.value()).isEqualTo("/realPrefix/mainPath");
        assertThat(objectKey.toString()).isEqualTo(printed);
    }

}
