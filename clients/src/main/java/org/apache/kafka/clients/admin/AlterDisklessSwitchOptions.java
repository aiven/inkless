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

import org.apache.kafka.common.annotation.InterfaceAudience;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.OptionalInt;

/**
 * Options for {@link Admin#alterDisklessSwitch(String, int, long, AlterDisklessSwitchOptions)}.
 *
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class AlterDisklessSwitchOptions extends AbstractOptions<AlterDisklessSwitchOptions> {
    private OptionalInt leaderId = OptionalInt.empty();

    /**
     * Sets the replica to elect as the leader in phase 1, even if it's outside the ISR. The request must
     * carry a seal offset of {@code -1}, which keeps the seal.
     *
     * @param leaderId The broker ID of the replica to elect.
     * @return This options object.
     */
    public AlterDisklessSwitchOptions leaderId(int leaderId) {
        this.leaderId = OptionalInt.of(leaderId);
        return this;
    }

    /**
     * Returns the replica to elect as the leader, or empty to keep the leader.
     */
    public OptionalInt leaderId() {
        return leaderId;
    }
}
