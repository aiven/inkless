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
package io.aiven.inkless.control_plane;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FileStateTest {
    @Test
    void fromName() {
        assertThat(FileState.fromName("uploaded")).isEqualTo(FileState.UPLOADED);
        assertThat(FileState.fromName("deleting")).isEqualTo(FileState.DELETING);
    }

    @Test
    void nameUnknown() {
        assertThatThrownBy(() -> FileState.fromName("x"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Unknown name x");
    }
}
