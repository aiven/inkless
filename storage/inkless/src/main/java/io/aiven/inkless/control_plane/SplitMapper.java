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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * The class to split the list by a predicate, transform independently, and merge back preserving the order.
 *
 * <pre>
 *     +---+              +----+              +----+
 *     | 0 |              | t0 |              | t0 |
 *     +---+              +----+              +----+
 *     | 1 |              | t1 |              | t1 |
 *     +---+    ===>      +----+    ===>      +----+
 *     | 2 |              | t3 |              | f2 |
 *     +---+              +----+              +----+
 *     | 3 |                                  | t3 |
 *     +---+                                  +----+
 *                        +----+
 *                        | f2 |
 *                        +----+
 * </pre>
 *
 * @param <TIn> The type of input objects.
 * @param <TOut> The type of output objects.
 */
public class SplitMapper<TIn, TOut> {
    private final List<TIn> in;
    private final List<Boolean> marks;
    private Iterator<TOut> trueOut;
    private Iterator<TOut> falseOut;

    public SplitMapper(final List<TIn> in, final Predicate<TIn> predicate) {
        Objects.requireNonNull(in, "in cannot be null");
        Objects.requireNonNull(predicate, "predicate cannot be null");
        this.in = new ArrayList<>(in.size());
        this.marks = new ArrayList<>(in.size());
        for (final TIn el : in) {
            this.in.add(el);
            this.marks.add(predicate.test(el));
        }
    }

    public Stream<TIn> getTrueIn() {
        return getGetStream(true);
    }

    public Stream<TIn> getFalseIn() {
        return getGetStream(false);
    }

    private Stream<TIn> getGetStream(final boolean which) {
        return IntStream.range(0, in.size())
            .filter(i -> marks.get(i) == which)
            .mapToObj(in::get);
    }

    public void setTrueOut(final Iterator<TOut> trueOut) {
        this.trueOut = Objects.requireNonNull(trueOut, "trueOut cannot be null");
    }

    public void setFalseOut(final Iterator<TOut> falseOut) {
        this.falseOut = Objects.requireNonNull(falseOut, "falseOut cannot be null");
    }

    public List<TOut> getOut() {
        if (this.trueOut == null) {
            throw new IllegalStateException("True out is not set");
        }
        if (this.falseOut == null) {
            throw new IllegalStateException("False out is not set");
        }

        final List<TOut> result = new ArrayList<>(in.size());
        for (final Boolean mark : marks) {
            try {
                final Iterator<TOut> iter = mark ? trueOut : falseOut;
                result.add(iter.next());
            } catch (final NoSuchElementException e) {
                final String iterName = mark ? "True out" : "False out";
                throw new IllegalStateException(iterName + " is exhausted", e);
            }
        }

        if (trueOut.hasNext()) {
            throw new IllegalStateException("True out is not exhausted");
        }
        if (falseOut.hasNext()) {
            throw new IllegalStateException("False out is not exhausted");
        }
        // Did we miss some check above? We'll see in test.
        assert result.size() == in.size();

        return result;
    }
}
