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
package kafka.server.mirror;

/**
 * Represents the lifecycle states of a mirror partition.
 * FAILED state can be entered from PREPARING or MIRRORING when errors occur.
 * <pre>
 * 1. INITIALIZING
 *    Triggered by: AddTopicsToMirror API call
 *    Waits for: Metadata update (partition becomes read-only leader)
 *
 * 2. PREPARING
 *    Triggered by: Metadata update callback in MirrorMetadataManager
 *    Actions: Fetch last mirrored offsets, schedule truncation
 *
 * 3. MIRRORING
 *    Triggered by: ISR truncation completion in Partition.checkIsrTruncationAndTransition
 *    Actions: Start MirrorFetcherThread to replicate data
 *
 * 4. STOPPING
 *    Triggered by: RemoveTopicsFromMirror API or topic deletion
 *    Actions: Record last mirrored offsets to internal topic
 *
 * 5. STOPPED
 *    Triggered by: Last mirrored offsets persisted
 *    Result: Topic becomes writable on destination cluster
 * </pre>
 */
public enum MirrorPartitionState {
    /** Topics are being prepared for mirroring (truncation may be needed) */
    PREPARING((byte) 0),

    /** Active mirroring from source cluster is in progress */
    MIRRORING((byte) 1),

    /** Mirroring is being gracefully stopped */
    STOPPING((byte) 2),

    /** Mirroring has stopped; topic is now writable on this cluster */
    STOPPED((byte) 4),

    /** Error occurred during preparation or mirroring */
    FAILED((byte) 8),

    /** Unknown state */
    UNKNOWN((byte) 16);

    private final byte value;

    MirrorPartitionState(byte value) {
        this.value = value;
    }

    public byte value() {
        return value;
    }

    public static MirrorPartitionState fromValue(byte value) {
        switch (value) {
            case 0:
                return PREPARING;
            case 1:
                return MIRRORING;
            case 2:
                return STOPPING;
            case 4:
                return STOPPED;
            case 8:
                return FAILED;
            case 16:
                return UNKNOWN;
        }
        throw new IllegalArgumentException("Illegal mirror state: " + value);
    }

    public static boolean isValidTransition(MirrorPartitionState source, MirrorPartitionState target) {
        if (source == target) {
            return true;
        }
        switch (target) {
            case PREPARING:
                return source == null
                        || source == MirrorPartitionState.UNKNOWN
                        || source == MirrorPartitionState.STOPPED
                        || source == MirrorPartitionState.FAILED;
            case MIRRORING:
                return source == MirrorPartitionState.PREPARING;
            case STOPPING:
                return source == MirrorPartitionState.PREPARING
                        || source == MirrorPartitionState.MIRRORING;
            case STOPPED:
                return source == MirrorPartitionState.STOPPING;
            case FAILED:
                return true;
            default:
                return false;
        }
    }
}
