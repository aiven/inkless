/*
 * Inkless
 * Copyright (C) 2025 Aiven OY
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
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
