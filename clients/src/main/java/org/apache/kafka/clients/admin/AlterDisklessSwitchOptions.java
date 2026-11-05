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

/**
 * Options for {@link Admin#alterDisklessSwitch(String, int, long, AlterDisklessSwitchOptions)}.
 *
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Unstable
public class AlterDisklessSwitchOptions extends AbstractOptions<AlterDisklessSwitchOptions> {
    private boolean clearProducerStates = false;
    private int leaderId = -1;
    private boolean classicPrefixInRemoteStorage = false;

    /**
     * When forcing a seal (offset {@code >= 0}), whether to clear the committed producer states.
     * Ignored for negative seal offsets, which always clear them.
     */
    public AlterDisklessSwitchOptions clearProducerStates(boolean clearProducerStates) {
        this.clearProducerStates = clearProducerStates;
        return this;
    }

    public boolean clearProducerStates() {
        return clearProducerStates;
    }

    /**
     * In phase 1 of the switch, the replica to elect as the only member of the ISR. The replica must hold
     * the whole classic prefix, and the seal offset must be the committed seal. The controller elects it
     * even if it's outside the ISR. The default, -1, keeps the leader.
     */
    public AlterDisklessSwitchOptions leaderId(int leaderId) {
        this.leaderId = leaderId;
        return this;
    }

    public int leaderId() {
        return leaderId;
    }

    /**
     * In phase 1 of the switch, whether to set {@code classicPrefixInRemoteStorage} and start phase 2 with
     * the seal offset as the seal. The seal offset must be at most the committed seal. The records from the
     * seal offset to the committed seal are lost.
     */
    public AlterDisklessSwitchOptions classicPrefixInRemoteStorage(boolean classicPrefixInRemoteStorage) {
        this.classicPrefixInRemoteStorage = classicPrefixInRemoteStorage;
        return this;
    }

    public boolean classicPrefixInRemoteStorage() {
        return classicPrefixInRemoteStorage;
    }
}
