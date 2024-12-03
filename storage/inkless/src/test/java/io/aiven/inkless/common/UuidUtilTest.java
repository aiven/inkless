// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.common;

import org.apache.kafka.common.Uuid;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class UuidUtilTest {
    @ParameterizedTest
    @ValueSource(strings = {
        "00000000-0000-0000-0000-000000000000",
        "f5d96e6d-7302-4ab9-88aa-6bf7bc7c2056",
        "5f9c26e9-877a-48ba-8c32-2df1f511c6fb",
        "ffffffff-ffff-ffff-ffff-ffffffffffff"
    })
    void test(final String str) {
        final UUID uuidJava = UUID.fromString(str);
        final Uuid uuidKafka = UuidUtil.fromJava(uuidJava);
        final UUID uuidJava2 = UuidUtil.toJava(uuidKafka);
        assertThat(uuidJava2).isEqualTo(uuidJava);
    }
}
