// Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.common;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ObjectKeyTest {
    @ParameterizedTest
    @CsvSource({
        "true, <prefix>/name",
        "false, realPrefix/name"
    })
    void testCreator(boolean masked, String printed) {
        ObjectKeyCreator objectKeyCreator = ObjectKey.creator("realPrefix", masked);
        final ObjectKey objectKey = objectKeyCreator.create("name");
        assertThat(objectKey.value()).isEqualTo("realPrefix/name");
        assertThat(objectKey.toString()).isEqualTo(printed);
        assertThat(objectKey).isEqualTo(objectKeyCreator.from("realPrefix/name"));
    }

    @ParameterizedTest
    @CsvSource({
        "prefix/,name", // prefix cannot end with a separator
        "/prefix,name", // prefix cannot start with a separator
        "prefix,/name", // name cannot start with a separator
        "prefix/other,na/me" // name cannot contain a separator
    })
    void testCreate_InvalidPath(String prefix, String name) {
        assertThatThrownBy(() -> ObjectKey.Path.create(prefix, name))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @CsvSource({
        "name,'',name",
        "prefix/name,prefix,name",
        "prefix/name/other,prefix/name,other",
        "prefix/name/other/and/so/on,prefix/name/other/and/so,on"
    })
    void testFrom_ValidPath(String value, String prefix, String name) {
        final ObjectKey.Path path = ObjectKey.Path.from(value);
        assertThat(path.prefix()).isEqualTo(prefix);
        assertThat(path.name()).isEqualTo(name);
    }
}
