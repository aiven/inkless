/*
 * Inkless
 * Copyright (C) 2024 - 2026 Aiven OY
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
package io.aiven.inkless.storage_backend.common;

import org.junit.jupiter.api.Test;

import io.aiven.inkless.common.ObjectKey;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SizedReadableByteChannelExactLengthTest {

    private static final ObjectKey KEY = ObjectKey.creator("prefix/", false).from("prefix/obj");

    @Test
    void returnsTheLengthWhenItFitsInAnInt() throws Exception {
        assertThat(SizedReadableByteChannel.exactLength(KEY, 0L)).isZero();
        assertThat(SizedReadableByteChannel.exactLength(KEY, 42L)).isEqualTo(42);
        assertThat(SizedReadableByteChannel.exactLength(KEY, Integer.MAX_VALUE))
            .isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    void rejectsALengthAnIntCannotHoldNamingTheObjectAndTheLength() {
        assertThatThrownBy(() -> SizedReadableByteChannel.exactLength(KEY, Integer.MAX_VALUE + 1L))
            .isInstanceOf(StorageBackendException.class)
            .hasMessageContaining("prefix/obj")
            .hasMessageContaining(String.valueOf(Integer.MAX_VALUE + 1L));
    }

    @Test
    void rejectsANegativeLength() {
        assertThatThrownBy(() -> SizedReadableByteChannel.exactLength(KEY, -1L))
            .isInstanceOf(StorageBackendException.class)
            .hasMessageContaining("prefix/obj");
    }
}
