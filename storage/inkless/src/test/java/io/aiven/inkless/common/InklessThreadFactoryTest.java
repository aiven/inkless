// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.common;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

class InklessThreadFactoryTest {
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void test(final boolean daemon) {
        final var factory = new InklessThreadFactory("prefix-", daemon);

        final Thread t0 = factory.newThread(() -> {});
        assertThat(t0.isDaemon()).isEqualTo(daemon);
        assertThat(t0.getName()).isEqualTo("prefix-0");

        final Thread t1 = factory.newThread(() -> {});
        assertThat(t1.isDaemon()).isEqualTo(daemon);
        assertThat(t1.getName()).isEqualTo("prefix-1");
    }
}
