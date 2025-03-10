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
package io.aiven.inkless.cache;

import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.MessageSizeAccumulator;
import org.apache.kafka.common.protocol.ObjectSerializationCache;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.ByteBufferFactory;
import org.infinispan.commons.marshall.BufferSizePredictor;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.factories.annotations.Inject;

import java.io.IOException;

import io.aiven.inkless.generated.CacheKey;
import io.aiven.inkless.generated.FileExtent;

public class KafkaMarshaller implements Marshaller {

    private static final short CACHE_KEY_ID = new CacheKey().apiKey();
    private static final short FILE_EXTENT_ID = new FileExtent().apiKey();

    @Inject
    ByteBufferFactory bufferFactory;

    @Override
    public byte[] objectToByteBuffer(Object obj, int estimatedSize) throws IOException, InterruptedException {
        return objectToByteBuffer(obj);
    }

    @Override
    public byte[] objectToByteBuffer(Object obj) throws IOException, InterruptedException {
        if (obj instanceof ApiMessage message) {
            short messageId = message.apiKey();
            short version = message.highestSupportedVersion();
            MessageSizeAccumulator accumulator = new MessageSizeAccumulator();
            accumulator.addBytes(2); // short messageId
            accumulator.addBytes(2); // short version
            ObjectSerializationCache cache = new ObjectSerializationCache();
            message.addSize(accumulator, cache, version);
            java.nio.ByteBuffer buffer = java.nio.ByteBuffer.allocate(accumulator.totalSize());
            buffer.putShort(messageId);
            buffer.putShort(version);
            message.write(new ByteBufferAccessor(buffer), cache, version);
            return buffer.array();
        } else {
            throw new IOException("Unable to marshall object of type " + (obj != null ? obj.getClass() : null));
        }
    }

    @Override
    public Object objectFromByteBuffer(byte[] buf) throws IOException, ClassNotFoundException {
        return deserialize(java.nio.ByteBuffer.wrap(buf));
    }

    private static ApiMessage deserialize(java.nio.ByteBuffer buffer) throws IOException {
        short messageId = buffer.getShort();
        short version = buffer.getShort();
        ByteBufferAccessor readable = new ByteBufferAccessor(buffer);
        if (messageId == CACHE_KEY_ID) {
            return new CacheKey(readable, version);
        } else if (messageId == FILE_EXTENT_ID) {
            return new FileExtent(readable, version);
        } else {
            throw new IOException("Undefined api key" + messageId);
        }
    }

    @Override
    public Object objectFromByteBuffer(byte[] buf, int offset, int length) throws IOException, ClassNotFoundException {
        return deserialize(java.nio.ByteBuffer.wrap(buf, offset, length));
    }

    @Override
    public ByteBuffer objectToBuffer(Object o) throws IOException, InterruptedException {
        return bufferFactory.newByteBuffer(objectToByteBuffer(0));
    }

    @Override
    public boolean isMarshallable(Object o) throws Exception {
        return o instanceof CacheKey || o instanceof FileExtent;
    }

    @Override
    public BufferSizePredictor getBufferSizePredictor(Object o) {
        return new BufferSizePredictor() {
            @Override
            public int nextSize(Object obj) {
                if (obj instanceof ApiMessage message) {
                    short version = message.highestSupportedVersion();
                    MessageSizeAccumulator accumulator = new MessageSizeAccumulator();
                    accumulator.addBytes(2); // short messageId
                    accumulator.addBytes(2); // short version
                    ObjectSerializationCache cache = new ObjectSerializationCache();
                    message.addSize(accumulator, cache, version);
                    return accumulator.totalSize();
                }
                return 0;
            }

            @Override
            public void recordSize(int previousSize) {
            }
        };
    }

    @Override
    public MediaType mediaType() {
        return MediaType.APPLICATION_OCTET_STREAM;
    }
}
