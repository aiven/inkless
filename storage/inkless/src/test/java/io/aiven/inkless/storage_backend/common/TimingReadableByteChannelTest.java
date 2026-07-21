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

import org.apache.kafka.common.utils.MockTime;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

import io.aiven.inkless.TimeUtils;

import static org.assertj.core.api.Assertions.assertThat;

class TimingReadableByteChannelTest {

    @Test
    void callbackInvokedOnFirstRead() throws IOException {
        final MockTime time = new MockTime();
        final AtomicLong recordedTtfb = new AtomicLong(-1);
        final Instant startTime = TimeUtils.durationMeasurementNow(time);
        final ReadableByteChannel delegate = Channels.newChannel(new ByteArrayInputStream(new byte[]{1, 2, 3}));

        final TimingReadableByteChannel channel = new TimingReadableByteChannel(delegate, time, startTime, recordedTtfb::set);

        // Advance time before first read
        time.sleep(42);

        final ByteBuffer buf = ByteBuffer.allocate(2);
        final int bytesRead = channel.read(buf);

        assertThat(bytesRead).isEqualTo(2);
        assertThat(recordedTtfb.get()).isEqualTo(42);
    }

    @Test
    void callbackInvokedOnlyOnce() throws IOException {
        final MockTime time = new MockTime();
        final AtomicLong callCount = new AtomicLong(0);
        final Instant startTime = TimeUtils.durationMeasurementNow(time);
        final ReadableByteChannel delegate = Channels.newChannel(new ByteArrayInputStream(new byte[]{1, 2, 3, 4}));

        final TimingReadableByteChannel channel = new TimingReadableByteChannel(delegate, time, startTime, ttfb -> callCount.incrementAndGet());

        time.sleep(10);
        channel.read(ByteBuffer.allocate(2));
        time.sleep(20);
        channel.read(ByteBuffer.allocate(2));

        assertThat(callCount.get()).isEqualTo(1);
    }

    @Test
    void callbackNotInvokedOnEmptyRead() throws IOException {
        final MockTime time = new MockTime();
        final AtomicLong recordedTtfb = new AtomicLong(-1);
        final Instant startTime = TimeUtils.durationMeasurementNow(time);
        // Empty stream returns -1 on read
        final ReadableByteChannel delegate = Channels.newChannel(new ByteArrayInputStream(new byte[0]));

        final TimingReadableByteChannel channel = new TimingReadableByteChannel(delegate, time, startTime, recordedTtfb::set);

        time.sleep(10);
        final int bytesRead = channel.read(ByteBuffer.allocate(10));

        assertThat(bytesRead).isEqualTo(-1);
        assertThat(recordedTtfb.get()).isEqualTo(-1); // callback never invoked
    }

    @Test
    void delegatesIsOpenAndClose() throws IOException {
        final MockTime time = new MockTime();
        final Instant startTime = TimeUtils.durationMeasurementNow(time);
        final ReadableByteChannel delegate = Channels.newChannel(new ByteArrayInputStream(new byte[]{1}));

        final TimingReadableByteChannel channel = new TimingReadableByteChannel(delegate, time, startTime, ttfb -> {});

        assertThat(channel.isOpen()).isTrue();
        channel.close();
        assertThat(channel.isOpen()).isFalse();
    }
}
