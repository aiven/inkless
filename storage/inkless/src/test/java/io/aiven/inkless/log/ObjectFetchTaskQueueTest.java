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

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.record.TimestampType;

import io.aiven.inkless.control_plane.BatchInfo;
import io.aiven.inkless.control_plane.BatchMetadata;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class ObjectFetchTaskQueueTest {
    static final BatchInfo B1 = new BatchInfo(1, "x1",
        new BatchMetadata(
            (byte) 0, null, 0, 10, 0, 0, 0, 0,
            TimestampType.NO_TIMESTAMP_TYPE));
    static final BatchInfo B2 = new BatchInfo(2, "x2",
        new BatchMetadata(
            (byte) 0, null, 0, 20, 0, 0, 0, 0,
            TimestampType.NO_TIMESTAMP_TYPE));

    @Mock
    CompletableFuture<ByteBuffer> F1;
    @Mock
    CompletableFuture<ByteBuffer> F2;

    @Test
    void testPeek() {
        final ObjectFetchTaskQueue q = new ObjectFetchTaskQueue();
        assertThat(q.peek()).isNull();

        final ObjectFetchTask expected1 = new ObjectFetchTask(B1, F1);
        final ObjectFetchTask expected2 = new ObjectFetchTask(B2, F2);

        q.add(expected1);
        assertThat(q.peek()).isSameAs(expected1);
        assertThat(q.peek()).isSameAs(expected1);

        q.add(expected2);
        assertThat(q.peek()).isSameAs(expected1);
        assertThat(q.peek()).isSameAs(expected1);

        q.poll();
        assertThat(q.peek()).isSameAs(expected2);

        q.poll();
        assertThat(q.peek()).isNull();
    }

    @Test
    void testPoll() {
        final ObjectFetchTaskQueue q = new ObjectFetchTaskQueue();
        assertThat(q.poll()).isNull();

        final ObjectFetchTask expected1 = new ObjectFetchTask(B1, F1);
        final ObjectFetchTask expected2 = new ObjectFetchTask(B2, F2);

        q.add(expected1);
        q.add(expected2);
        assertThat(q.poll()).isSameAs(expected1);
        assertThat(q.poll()).isSameAs(expected2);
        assertThat(q.poll()).isNull();
    }

    @Test
    void testTotalByteSize() {
        final ObjectFetchTaskQueue q = new ObjectFetchTaskQueue();
        assertThat(q.totalByteSize()).isZero();

        final ObjectFetchTask expected1 = new ObjectFetchTask(B1, F1);
        final ObjectFetchTask expected2 = new ObjectFetchTask(B2, F2);

        q.add(expected1);
        assertThat(q.totalByteSize()).isEqualTo(B1.metadata().byteSize());

        q.add(expected2);
        assertThat(q.totalByteSize()).isEqualTo(B1.metadata().byteSize() + B2.metadata().byteSize());

        q.poll();
        assertThat(q.totalByteSize()).isEqualTo(B2.metadata().byteSize());

        q.poll();
        assertThat(q.totalByteSize()).isZero();
    }
}
