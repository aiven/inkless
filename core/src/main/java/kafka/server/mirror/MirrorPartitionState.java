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
 */
public enum MirrorPartitionState {
    /**
     * The coordinator detects via onMetadataUpdate that it leads a mirror partition.
     * It fetches last mirrored offsets from the source cluster and truncates logs to
     * align the local log with the source.
     * Valid from: null, UNKNOWN, STOPPED, FAILED.
     */
    PREPARING((byte) 0),

    /**
     * Checking if bumping the leader epoch is necessary to ensure local leader epoch > source leader epoch.
     * Valid from: UNKNOWN, PREPARING, STOPPED, FAILED.
     */
    EPOCH_FENCING((byte) 1),

    /**
     * All ISR members have completed truncation and leader epoch bumping completes. A MirrorFetcherThread is started to
     * continuously replicate records from the source cluster.
     * Valid from: EPOCH_FENCING, PAUSED.
     */
    MIRRORING((byte) 2),

    /**
     * Triggered by PauseMirrorTopics API (appends .paused suffix to mirror.name config).
     * The system removes fetchers for the affected partitions.
     * Valid from: MIRRORING only.
     */
    PAUSING((byte) 3),

    /**
     * Fetchers have been removed. The partition stays read-only with no active fetchers
     * and no metadata sync (configs, consumer groups, ACLs). On resume, transitions
     * directly to MIRRORING (fetchers resume from local LEO, no truncation needed).
     * Valid from: PAUSING only.
     */
    PAUSED((byte) 4),

    /**
     * Triggered by StopMirrorTopics API (failover) or topic deletion on the source.
     * The system records the last mirrored offset to the internal topic.
     * Valid from: PREPARING, MIRRORING, PAUSING (race guard), PAUSED, FAILED.
     */
    STOPPING((byte) 5),

    /**
     * Last mirrored offsets have been persisted. The topic becomes writable on the
     * destination cluster (fetcher removed, read-only flag cleared).
     * Valid from: STOPPING only.
     */
    STOPPED((byte) 6),

    /**
     * An error occurred. Can transition back to PREPARING to retry.
     * Valid from: any state.
     */
    FAILED((byte) 7),

    /**
     * No cached state (broker just became leader, state not loaded yet).
     * Not an explicit API-driven state, just the absence of state.
     */
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
                return EPOCH_FENCING;
            case 2:
                return MIRRORING;
            case 3:
                return PAUSING;
            case 4:
                return PAUSED;
            case 5:
                return STOPPING;
            case 6:
                return STOPPED;
            case 7:
                return FAILED;
            case 16:
                return UNKNOWN;
        }
        throw new IllegalArgumentException("Illegal mirror state: " + value);
    }

    @SuppressWarnings("checkstyle:cyclomaticComplexity")
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
            case EPOCH_FENCING:
                return source == MirrorPartitionState.MIRRORING;
            case MIRRORING:
                return source == MirrorPartitionState.PREPARING
                        || source == MirrorPartitionState.EPOCH_FENCING
                        || source == MirrorPartitionState.PAUSED;
            case PAUSING:
                return source == MirrorPartitionState.MIRRORING;
            case PAUSED:
                return source == MirrorPartitionState.PAUSING;
            case STOPPING:
                // TODO: remove PAUSING once state transitions are serialized via the shared queue
                return source == MirrorPartitionState.PREPARING
                        || source == MirrorPartitionState.EPOCH_FENCING
                        || source == MirrorPartitionState.MIRRORING
                        || source == MirrorPartitionState.PAUSING
                        || source == MirrorPartitionState.PAUSED
                        || source == MirrorPartitionState.FAILED;
            case STOPPED:
                return source == MirrorPartitionState.STOPPING;
            case FAILED:
                return true;
            default:
                return false;
        }
    }
}
