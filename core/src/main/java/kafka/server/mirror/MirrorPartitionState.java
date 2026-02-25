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
 * FAILED state can be entered from any state when errors occur.
 * <pre>
 * 1. PREPARING
 *    Triggered by: Metadata update callback in MirrorMetadataManager
 *    Actions: Fetch last mirrored offsets, schedule truncation
 *
 * 2. MIRRORING
 *    Triggered by: ISR truncation completion in Partition.checkIsrTruncationAndTransition
 *    Actions: Start MirrorFetcherThread to replicate data
 *
 * 3. PAUSING
 *    Triggered by: PauseMirrorTopics API (appends .paused suffix to mirror.name)
 *    Actions: Remove fetchers, record current LEO offsets
 *
 * 4. PAUSED
 *    Triggered by: Paused offsets persisted
 *    Result: Partition stays read-only, no active fetchers, metadata sync continues
 *
 * 5. STOPPING
 *    Triggered by: RemoveTopicsFromMirror API or topic deletion
 *    Actions: Record last mirrored offsets to internal topic
 *
 * 6. STOPPED
 *    Triggered by: Last mirrored offsets persisted
 *    Result: Topic becomes writable on destination cluster
 * </pre>
 */
public enum MirrorPartitionState {
    /** Topics are being prepared for mirroring (truncation may be needed) */
    PREPARING((byte) 0),

    /** Active mirroring from source cluster is in progress */
    MIRRORING((byte) 1),

    /** Mirroring is being paused; fetchers removed and offsets recorded */
    PAUSING((byte) 2),

    /** Mirroring is paused; partition stays read-only with no active fetchers */
    PAUSED((byte) 3),

    /** Mirroring is being gracefully stopped */
    STOPPING((byte) 4),

    /** Mirroring has stopped; topic is now writable on this cluster */
    STOPPED((byte) 5),

    /** Error occurred during preparation or mirroring */
    FAILED((byte) 6),

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
                return PAUSING;
            case 3:
                return PAUSED;
            case 4:
                return STOPPING;
            case 5:
                return STOPPED;
            case 6:
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
                        || source == MirrorPartitionState.PAUSED
                        || source == MirrorPartitionState.FAILED;
            case MIRRORING:
                return source == MirrorPartitionState.PREPARING;
            case PAUSING:
                return source == MirrorPartitionState.MIRRORING
                        || source == MirrorPartitionState.PREPARING;
            case PAUSED:
                return source == MirrorPartitionState.PAUSING;
            case STOPPING:
                return source == MirrorPartitionState.PREPARING
                        || source == MirrorPartitionState.MIRRORING
                        || source == MirrorPartitionState.PAUSED;
            case STOPPED:
                return source == MirrorPartitionState.STOPPING;
            case FAILED:
                return true;
            default:
                return false;
        }
    }
}
