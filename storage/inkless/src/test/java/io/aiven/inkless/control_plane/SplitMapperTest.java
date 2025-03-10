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
package io.aiven.inkless.control_plane;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SplitMapperTest {
    @Test
    void empty() {
        final SplitMapper<String, String> mapper = new SplitMapper<>(List.of(), e -> true);
        mapper.setFalseOut(Collections.emptyIterator());
        mapper.setTrueOut(Collections.emptyIterator());
        assertThat(mapper.getOut()).isEmpty();
    }

    @Test
    void allTrue() {
        final SplitMapper<Integer, String> mapper = new SplitMapper<>(List.of(1, 2, 3, 4, 5, 6), e -> true);
        mapper.setFalseOut(Collections.emptyIterator());
        mapper.setTrueOut(mapper.getTrueIn().map(i -> Integer.toString(i)).iterator());

        assertThat(mapper.getOut()).containsExactly("1", "2", "3", "4", "5", "6");
    }

    @Test
    void allFalse() {
        final SplitMapper<Integer, String> mapper = new SplitMapper<>(List.of(1, 2, 3, 4, 5, 6), e -> false);
        mapper.setTrueOut(Collections.emptyIterator());
        mapper.setFalseOut(mapper.getFalseIn().map(i -> Integer.toString(i)).iterator());

        assertThat(mapper.getOut()).containsExactly("1", "2", "3", "4", "5", "6");
    }

    @Test
    void mixed() {
        final SplitMapper<Integer, String> mapper = new SplitMapper<>(List.of(0, 2, 3, 5, 6, 7, 8, 9, 10, 12), e -> e % 2 == 0);
        mapper.setTrueOut(mapper.getTrueIn().map(i -> "Even: " + i).iterator());
        mapper.setFalseOut(mapper.getFalseIn().map(i -> "Odd: " + i).iterator());

        assertThat(mapper.getOut()).containsExactly(
            "Even: 0",
            "Even: 2",
            "Odd: 3",
            "Odd: 5",
            "Even: 6",
            "Odd: 7",
            "Even: 8",
            "Odd: 9",
            "Even: 10",
            "Even: 12"
        );
    }

    @Test
    void constructorNulls() {
        assertThatThrownBy(() -> new SplitMapper<>(null, e -> true))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("in cannot be null");
        assertThatThrownBy(() -> new SplitMapper<>(List.of(), null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("predicate cannot be null");
    }

    @Test
    void setOutNulls() {
        final SplitMapper<Object, Object> mapper = new SplitMapper<>(List.of(), e -> true);
        assertThatThrownBy(() -> mapper.setTrueOut(null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("trueOut cannot be null");
        assertThatThrownBy(() -> mapper.setFalseOut(null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("falseOut cannot be null");
    }

    @Test
    void trueOutNotSet() {
        final SplitMapper<String, String> mapper = new SplitMapper<>(List.of(), e -> true);
        mapper.setFalseOut(Collections.emptyIterator());
        assertThatThrownBy(mapper::getOut)
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("True out is not set");
    }

    @Test
    void falseOutNotSet() {
        final SplitMapper<String, String> mapper = new SplitMapper<>(List.of(), e -> true);
        mapper.setTrueOut(Collections.emptyIterator());
        assertThatThrownBy(mapper::getOut)
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("False out is not set");
    }

    @Test
    void trueOutIsTooShort() {
        final SplitMapper<Integer, Integer> mapper = new SplitMapper<>(List.of(1), e -> true);
        mapper.setFalseOut(Collections.emptyIterator());
        mapper.setTrueOut(Collections.emptyIterator());
        assertThatThrownBy(mapper::getOut)
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("True out is exhausted");
    }

    @Test
    void trueOutIsTooLong() {
        final SplitMapper<Integer, Integer> mapper = new SplitMapper<>(List.of(), e -> true);
        mapper.setFalseOut(Collections.emptyIterator());
        mapper.setTrueOut(List.of(1).iterator());
        assertThatThrownBy(mapper::getOut)
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("True out is not exhausted");
    }

    @Test
    void falseOutIsTooShort() {
        final SplitMapper<Integer, Integer> mapper = new SplitMapper<>(List.of(1), e -> false);
        mapper.setFalseOut(Collections.emptyIterator());
        mapper.setTrueOut(Collections.emptyIterator());
        assertThatThrownBy(mapper::getOut)
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("False out is exhausted");
    }

    @Test
    void falseOutIsTooLong() {
        final SplitMapper<Integer, Integer> mapper = new SplitMapper<>(List.of(), e -> false);
        mapper.setFalseOut(List.of(1).iterator());
        mapper.setTrueOut(Collections.emptyIterator());
        assertThatThrownBy(mapper::getOut)
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("False out is not exhausted");
    }
}
