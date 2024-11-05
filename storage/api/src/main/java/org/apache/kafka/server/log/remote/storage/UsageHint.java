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
package org.apache.kafka.server.log.remote.storage;


import java.time.Duration;

/**
 * Optional hints about the usage pattern of objects.
 */
public class UsageHint {

    // The expected durability of this object
    private final Durability durability;
    // The expected lifetime of this object
    private final Duration lifetime;
    // The expected total size of the file
    private final long size;
    // The expected number of reads for this object over its lifetime
    private final int reads;

    public UsageHint(Durability durability, Duration lifetime, long size, int reads) {
        this.durability = durability;
        this.lifetime = lifetime;
        this.size = size;
        this.reads = reads;
    }

    /**
     * An abstraction of storage durability, in terms of the number of 9s.
     * Unknown and Cache imply no durability requirements (data may be discarded immediately).
     * Best Effort durability is for data which should be kept in 90% of cases, and may be used for working sets.
     * High durability is data which is not expected to be lost, and could cause client-facing data loss.
     */
    public enum Durability {
        UNKNOWN(-1),
        CACHE(0),
        BEST_EFFORT(1),
        HIGH(10);

        private final int nines;

        Durability(int nines) {
            this.nines = nines;
        }

        int nines() {
            return nines;
        }
    }

}
