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
package io.aiven.inkless.log;

import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.PlainObjectKey;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RangeBlockingMultiMapTest {
    static final PlainObjectKey KEY1 = PlainObjectKey.create("x", "y1");
    static final PlainObjectKey KEY2 = PlainObjectKey.create("x", "y2");
    static final PlainObjectKey KEY3 = PlainObjectKey.create("x", "y3");

    static final ByteRange BR1 = new ByteRange(0, 1);
    static final ByteRange BR2 = new ByteRange(0, 2);
    static final ByteRange BR3 = new ByteRange(0, 3);

    @Test
    void removeEmpty() {
        final RangeBlockingMultiMap map = new RangeBlockingMultiMap();
        assertThat(map.remove(KEY1)).isNull();
    }

    @Test
    void addOneAndRemove() {
        final RangeBlockingMultiMap map = new RangeBlockingMultiMap();
        map.put(KEY1, BR1);
        assertThat(map.remove(KEY1)).containsExactly(BR1);
    }

    @Test
    void addMultipleAndRemove() {
        final RangeBlockingMultiMap map = new RangeBlockingMultiMap();
        map.put(KEY1, BR1);
        map.put(KEY1, BR2);
        assertThat(map.remove(KEY1)).containsExactly(BR1, BR2);
    }

    @Test
    void addAfterRemove() {
        final RangeBlockingMultiMap map = new RangeBlockingMultiMap();
        map.put(KEY1, BR1);
        map.remove(KEY1);
        map.put(KEY1, BR2);
        assertThat(map.remove(KEY1)).containsExactly(BR2);
    }

    @Test
    void addAndRemoveMultipleKeys() {
        final RangeBlockingMultiMap map = new RangeBlockingMultiMap();
        map.put(KEY1, BR1);
        map.put(KEY2, BR2);
        map.put(KEY1, BR3);
        assertThat(map.remove(KEY1)).containsExactly(BR1, BR3);
        assertThat(map.remove(KEY2)).containsExactly(BR2);
        assertThat(map.remove(KEY3)).isNull();
    }
}