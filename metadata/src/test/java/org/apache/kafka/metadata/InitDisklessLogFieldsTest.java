/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.metadata;

import org.apache.kafka.common.protocol.types.RawTaggedField;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class InitDisklessLogFieldsTest {

    @Test
    public void testClassicPrefixInRemoteStorageTagNumber() {
        assertEquals(103, InitDisklessLogFields.CLASSIC_PREFIX_IN_REMOTE_STORAGE_TAG);
        assertEquals(103, InitDisklessLogFields.encodeClassicPrefixInRemoteStorage().tag());
    }

    @Test
    public void testClassicPrefixInRemoteStorageEncodesOneByteTrue() {
        RawTaggedField field = InitDisklessLogFields.encodeClassicPrefixInRemoteStorage();
        assertArrayEquals(new byte[]{1}, field.data());
    }

    @Test
    public void testClassicPrefixInRemoteStorageRoundTrip() {
        List<RawTaggedField> fields = List.of(InitDisklessLogFields.encodeClassicPrefixInRemoteStorage());

        assertEquals(Optional.of(true), InitDisklessLogFields.decodeClassicPrefixInRemoteStorageIfPresent(fields));
        assertTrue(InitDisklessLogFields.decodeClassicPrefixInRemoteStorage(fields));
    }

    @Test
    public void testClassicPrefixInRemoteStorageAbsentDecodesToFalse() {
        List<RawTaggedField> fields = List.of();

        assertEquals(Optional.empty(), InitDisklessLogFields.decodeClassicPrefixInRemoteStorageIfPresent(fields));
        assertFalse(InitDisklessLogFields.decodeClassicPrefixInRemoteStorage(fields));
    }

    @Test
    public void testClassicPrefixInRemoteStorageIgnoresOtherTags() {
        List<RawTaggedField> fields = List.of(
            InitDisklessLogFields.encodeClassicToDisklessStartOffset(42L),
            InitDisklessLogFields.encodeDisklessLeaderEpoch(7));

        assertEquals(Optional.empty(), InitDisklessLogFields.decodeClassicPrefixInRemoteStorageIfPresent(fields));
        assertFalse(InitDisklessLogFields.decodeClassicPrefixInRemoteStorage(fields));
    }

    @Test
    public void testClassicPrefixInRemoteStorageReadsOnlyFirstByte() {
        assertDecodes(false, new byte[0]);
        assertDecodes(false, new byte[]{0});
        assertDecodes(true, new byte[]{1});
        // The encoder never writes 2. The case pins the contract: only 1 is true.
        assertDecodes(false, new byte[]{2});
        // Only the first byte is read.
        assertDecodes(true, new byte[]{1, 0, 0, 0});
    }

    @Test
    public void testClassicPrefixInRemoteStorageCoexistsWithOtherTags() {
        List<InitDisklessLogFields.ProducerStateEntry> producerStates = List.of(
            new InitDisklessLogFields.ProducerStateEntry(1L, (short) 0, 0, 10, 100L, 5000L));
        List<RawTaggedField> fields = new ArrayList<>();
        fields.add(InitDisklessLogFields.encodeClassicToDisklessStartOffset(42L));
        fields.add(InitDisklessLogFields.encodeProducerStates(producerStates));
        fields.add(InitDisklessLogFields.encodeDisklessLeaderEpoch(7));
        fields.add(InitDisklessLogFields.encodeClassicPrefixInRemoteStorage());

        assertEquals(OptionalLong.of(42L), InitDisklessLogFields.decodeClassicToDisklessStartOffsetIfPresent(fields));
        assertEquals(Optional.of(producerStates), InitDisklessLogFields.decodeProducerStatesIfPresent(fields));
        assertEquals(7, InitDisklessLogFields.decodeDisklessLeaderEpoch(fields));
        assertTrue(InitDisklessLogFields.decodeClassicPrefixInRemoteStorage(fields));
    }

    private static void assertDecodes(boolean expected, byte[] payload) {
        List<RawTaggedField> fields = List.of(
            new RawTaggedField(InitDisklessLogFields.CLASSIC_PREFIX_IN_REMOTE_STORAGE_TAG, payload));

        assertEquals(Optional.of(expected), InitDisklessLogFields.decodeClassicPrefixInRemoteStorageIfPresent(fields));
        assertEquals(expected, InitDisklessLogFields.decodeClassicPrefixInRemoteStorage(fields));
    }
}
