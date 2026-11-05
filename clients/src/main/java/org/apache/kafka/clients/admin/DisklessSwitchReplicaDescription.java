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

package org.apache.kafka.clients.admin;

import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Objects;

/**
 * Describes one broker's replica of a partition that is switching from classic to diskless.
 *
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Unstable
public class DisklessSwitchReplicaDescription {
    private final boolean offline;
    private final long logStartOffset;
    private final long logEndOffset;
    private final long highWatermark;
    private final long crossTierLogStartOffset;
    private final long remoteStartOffset;
    private final long remoteEndOffset;

    public DisklessSwitchReplicaDescription(boolean offline, long logStartOffset, long logEndOffset, long highWatermark,
                                            long crossTierLogStartOffset, long remoteStartOffset, long remoteEndOffset) {
        this.offline = offline;
        this.logStartOffset = logStartOffset;
        this.logEndOffset = logEndOffset;
        this.highWatermark = highWatermark;
        this.crossTierLogStartOffset = crossTierLogStartOffset;
        this.remoteStartOffset = remoteStartOffset;
        this.remoteEndOffset = remoteEndOffset;
    }

    /**
     * True if the broker took the partition offline because its local classic prefix is incomplete;
     * false otherwise.
     */
    public boolean offline() {
        return offline;
    }

    /**
     * Returns the start offset of the local log, or -1 if the broker has no local log.
     */
    public long logStartOffset() {
        return logStartOffset;
    }

    /**
     * Returns the end offset of the local log, or -1 if the broker has no local log.
     */
    public long logEndOffset() {
        return logEndOffset;
    }

    /**
     * Returns the high watermark of the local log, or -1 if the broker has no local log.
     */
    public long highWatermark() {
        return highWatermark;
    }

    /**
     * Returns the earliest offset of the partition across remote storage and the diskless log, or -1 if
     * the control plane doesn't know it.
     */
    public long crossTierLogStartOffset() {
        return crossTierLogStartOffset;
    }

    /**
     * Returns the first offset at or above the cross-tier log start that remote storage holds, or -1 if
     * remote storage holds none or the broker can't list the remote segments.
     */
    public long remoteStartOffset() {
        return remoteStartOffset;
    }

    /**
     * Returns the offset after the last record of the run of remote segments that starts at
     * {@link #remoteStartOffset()}, capped at the seal. If remote storage holds no segment at or above
     * the cross-tier log start, returns the cross-tier log start. Returns -1 if the broker can't list
     * the remote segments or doesn't know the cross-tier log start.
     */
    public long remoteEndOffset() {
        return remoteEndOffset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DisklessSwitchReplicaDescription that = (DisklessSwitchReplicaDescription) o;
        return offline == that.offline &&
            logStartOffset == that.logStartOffset &&
            logEndOffset == that.logEndOffset &&
            highWatermark == that.highWatermark &&
            crossTierLogStartOffset == that.crossTierLogStartOffset &&
            remoteStartOffset == that.remoteStartOffset &&
            remoteEndOffset == that.remoteEndOffset;
    }

    @Override
    public int hashCode() {
        return Objects.hash(offline, logStartOffset, logEndOffset, highWatermark, crossTierLogStartOffset,
            remoteStartOffset, remoteEndOffset);
    }

    @Override
    public String toString() {
        return "DisklessSwitchReplicaDescription(" +
            "offline=" + offline +
            ", logStartOffset=" + logStartOffset +
            ", logEndOffset=" + logEndOffset +
            ", highWatermark=" + highWatermark +
            ", crossTierLogStartOffset=" + crossTierLogStartOffset +
            ", remoteStartOffset=" + remoteStartOffset +
            ", remoteEndOffset=" + remoteEndOffset +
            ')';
    }
}
