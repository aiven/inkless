/*
 * Inkless
 * Copyright (C) 2024 - 2025 Aiven OY
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
