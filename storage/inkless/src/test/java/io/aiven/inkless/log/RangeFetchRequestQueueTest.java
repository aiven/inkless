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

import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.PlainObjectKey;
import io.aiven.inkless.control_plane.BatchInfo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RangeFetchRequestQueueTest {
    static final PlainObjectKey KEY1 = PlainObjectKey.create("x", "y1");
    static final PlainObjectKey KEY2 = PlainObjectKey.create("x", "y2");
    static final PlainObjectKey KEY3 = PlainObjectKey.create("x", "y3");
    static final PlainObjectKey[] KEYS = new PlainObjectKey[]{KEY1, KEY2, KEY3};

    static final ByteRange BR1 = new ByteRange(0, 1);
    static final ByteRange BR2 = new ByteRange(0, 2);
    static final ByteRange BR3 = new ByteRange(0, 3);

    static final CompletableFuture<ByteBuffer> F1 = new CompletableFuture<>();
    static final CompletableFuture<ByteBuffer> F2 = new CompletableFuture<>();
    static final CompletableFuture<ByteBuffer> F3 = new CompletableFuture<>();

    @Mock
    BatchInfo batch1;
    @Mock
    BatchInfo batch2;
    @Mock
    BatchInfo batch3;

    @Test
    void empty() throws InterruptedException {
        final MockTime time = new MockTime();
        final RangeFetchRequestQueue queue = new RangeFetchRequestQueue(time, 100);

        final RangeFetchRequests result = queue.poll(100, TimeUnit.MILLISECONDS);
        assertThat(result).isNull();
    }

    @Test
    void singleKey() throws InterruptedException {
        final MockTime time = new MockTime();
        final RangeFetchRequestQueue queue = new RangeFetchRequestQueue(time, 100);

        final ByteRangeWithFetchTask r1 = new ByteRangeWithFetchTask(BR1, new ObjectFetchTask(batch1, F1));
        final ByteRangeWithFetchTask r2 = new ByteRangeWithFetchTask(BR2, new ObjectFetchTask(batch2, F2));
        queue.addRequest(KEY1, r1);
        queue.addRequest(KEY1, r2);
        time.sleep(100);

        final RangeFetchRequests result = queue.poll(100, TimeUnit.MILLISECONDS);
        assertThat(result.objectKey()).isEqualTo(KEY1);
        assertThat(result.requests()).containsExactly(r1, r2);
    }

    @Test
    void multipleKeys() throws InterruptedException {
        final MockTime time = new MockTime();
        final RangeFetchRequestQueue queue = new RangeFetchRequestQueue(time, 100);

        final ByteRangeWithFetchTask r1 = new ByteRangeWithFetchTask(BR1, new ObjectFetchTask(batch1, F1));
        final ByteRangeWithFetchTask r2 = new ByteRangeWithFetchTask(BR2, new ObjectFetchTask(batch2, F2));
        final ByteRangeWithFetchTask r3 = new ByteRangeWithFetchTask(BR3, new ObjectFetchTask(batch3, F3));
        queue.addRequest(KEY1, r1);
        queue.addRequest(KEY2, r2);
        queue.addRequest(KEY1, r3);
        time.sleep(100);

        final RangeFetchRequests result1 = queue.poll(100, TimeUnit.MILLISECONDS);
        assertThat(result1.objectKey()).isEqualTo(KEY1);
        assertThat(result1.requests()).containsExactly(r1, r3);

        final RangeFetchRequests result2 = queue.poll(100, TimeUnit.MILLISECONDS);
        assertThat(result2.objectKey()).isEqualTo(KEY2);
        assertThat(result2.requests()).containsExactly(r2);
    }

    @Test
    void addBeforeAndAfterPoll() throws InterruptedException {
        final MockTime time = new MockTime();
        final RangeFetchRequestQueue queue = new RangeFetchRequestQueue(time, 100);

        final ByteRangeWithFetchTask r1 = new ByteRangeWithFetchTask(BR1, new ObjectFetchTask(batch1, F1));
        final ByteRangeWithFetchTask r2 = new ByteRangeWithFetchTask(BR2, new ObjectFetchTask(batch2, F2));
        queue.addRequest(KEY1, r1);
        time.sleep(20);
        queue.addRequest(KEY1, r2);
        time.sleep(100);

        final RangeFetchRequests result1 = queue.poll(100, TimeUnit.MILLISECONDS);
        assertThat(result1.objectKey()).isEqualTo(KEY1);
        assertThat(result1.requests()).containsExactly(r1, r2);

        final ByteRangeWithFetchTask r3 = new ByteRangeWithFetchTask(BR3, new ObjectFetchTask(batch3, F3));
        queue.addRequest(KEY1, r3);
        time.sleep(100);

        final RangeFetchRequests result2 = queue.poll(100, TimeUnit.MILLISECONDS);
        assertThat(result2.objectKey()).isEqualTo(KEY1);
        assertThat(result2.requests()).containsExactly(r3);
    }

    @Test
    void multipleRequestsWithSameRange() throws InterruptedException {
        final MockTime time = new MockTime();
        final RangeFetchRequestQueue queue = new RangeFetchRequestQueue(time, 100);

        final ObjectFetchTask task1 = new ObjectFetchTask(batch1, F1);
        final ObjectFetchTask task2 = new ObjectFetchTask(batch2, F2);
        final ObjectFetchTask task3 = new ObjectFetchTask(batch3, F3);
        queue.addRequest(KEY1, new ByteRangeWithFetchTask(BR1, task1));
        queue.addRequest(KEY1, new ByteRangeWithFetchTask(BR1, task2));
        queue.addRequest(KEY1, new ByteRangeWithFetchTask(BR1, task3));
        time.sleep(100);

        final RangeFetchRequests result = queue.poll(100, TimeUnit.MILLISECONDS);
        assertThat(result.objectKey()).isEqualTo(KEY1);
        assertThat(result.requests()).containsExactly(
            new ByteRangeWithFetchTask(BR1, task1),
            new ByteRangeWithFetchTask(BR1, task2),
            new ByteRangeWithFetchTask(BR1, task3)
        );
    }

    @Test
    void zeroDelay() throws InterruptedException {
        final MockTime time = new MockTime();
        final RangeFetchRequestQueue queue = new RangeFetchRequestQueue(time, 0);

        final ObjectFetchTask task = new ObjectFetchTask(batch1, F1);
        queue.addRequest(KEY1, new ByteRangeWithFetchTask(BR1, task));

        // Should be immediately available.
        final RangeFetchRequests result = queue.poll(0, TimeUnit.MILLISECONDS);
        assertThat(result.objectKey()).isEqualTo(KEY1);
        assertThat(result.requests()).containsExactly(new ByteRangeWithFetchTask(BR1, task));
    }

    @Test
    void concurrentAccessNoRequestLost() throws InterruptedException {
        final int requestsPerProducer = 1000;

        final RangeFetchRequestQueue queue = new RangeFetchRequestQueue(Time.SYSTEM, 10);

        final ConcurrentLinkedQueue<RangeFetchRequests> sentRequests = new ConcurrentLinkedQueue<>();
        final ConcurrentLinkedQueue<RangeFetchRequests> receivedRequests = new ConcurrentLinkedQueue<>();

        final List<Thread> allThreads = new ArrayList<>();
        // 3 producers
        for (int threadId = 0; threadId < 3; threadId++) {
            final int finalThreadId = threadId;
            final Thread thread = new Thread(() -> {
                final Random random = new Random();
                for (int reqId = 0; reqId < requestsPerProducer; reqId++) {
                    final var objectKey = KEYS[random.nextInt(3)];
                    final long offset = 100_000_000 * finalThreadId + reqId;
                    final long size = random.nextLong(1000);
                    final CompletableFuture<ByteBuffer> f = new CompletableFuture<>();
                    final ByteRangeWithFetchTask r = new ByteRangeWithFetchTask(new ByteRange(offset, size), new ObjectFetchTask(batch1, f));
                    queue.addRequest(objectKey, r);
                    sentRequests.add(new RangeFetchRequests(objectKey, List.of(r)));

                    final int sleep = random.nextInt(5);
                    try {
                        Thread.sleep(sleep);
                    } catch (final InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
            allThreads.add(thread);
        }

        // 3 consumers
        for (int i = 0; i < 3; i++) {
            final Thread thread = new Thread(() -> {
                final Random random = new Random();
                int waitedUnsuccessfully = 0;
                while (waitedUnsuccessfully < 10) {
                    final RangeFetchRequests result;
                    try {
                        result = queue.poll(100, TimeUnit.MILLISECONDS);
                    } catch (final InterruptedException e) {
                        throw new RuntimeException(e);
                    }

                    if (result == null) {
                        waitedUnsuccessfully += 1;
                    } else {
                        receivedRequests.add(result);
                    }

                    final int sleep = random.nextInt(100);
                    try {
                        Thread.sleep(sleep);
                    } catch (final InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
            allThreads.add(thread);
        }

        for (final Thread t : allThreads) {
            t.start();
        }
        for (final Thread t : allThreads) {
            t.join();
        }

        final HashMap<ObjectKey, Set<ByteRangeWithFetchTask>> sentAggregated = new HashMap<>();
        for (final var r : sentRequests) {
            sentAggregated.putIfAbsent(r.objectKey(), new HashSet<>());
            sentAggregated.get(r.objectKey()).addAll(r.requests());
        }

        final HashMap<ObjectKey, Set<ByteRangeWithFetchTask>> receivedAggregated = new HashMap<>();
        for (final var r : receivedRequests) {
            receivedAggregated.putIfAbsent(r.objectKey(), new HashSet<>());
            receivedAggregated.get(r.objectKey()).addAll(r.requests());
        }

        assertThat(receivedAggregated).isEqualTo(sentAggregated);
    }

    @Test
    void constructorValidation() {
        assertThatThrownBy(() -> new RangeFetchRequestQueue(null, 1000))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("time cannot be null");

        assertThatThrownBy(() -> new RangeFetchRequestQueue(new MockTime(), -1))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("delayMs cannot be negative");
    }

    @Test
    void addRequestValidation() {
        final RangeFetchRequestQueue queue = new RangeFetchRequestQueue(new MockTime(), 1000);
        assertThatThrownBy(() -> queue.addRequest(null, new ByteRangeWithFetchTask(BR1, new ObjectFetchTask(batch1, F1))))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("objectKey cannot be null");
        assertThatThrownBy(() -> queue.addRequest(KEY1, null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("range cannot be null");
    }
}
