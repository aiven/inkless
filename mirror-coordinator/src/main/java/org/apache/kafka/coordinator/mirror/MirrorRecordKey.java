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
package org.apache.kafka.coordinator.mirror;

import java.util.Objects;

/**
 * This key is used to uniquely identify a cluster mirror by its name.
 */
public record MirrorRecordKey(String mirrorName) {
    public MirrorRecordKey(String mirrorName) {
        this.mirrorName = Objects.requireNonNull(mirrorName, "Mirror name cannot be null");
    }

    public static MirrorRecordKey getInstance(String key) {
        validate(key);
        return new MirrorRecordKey(key);
    }

    public String asCoordinatorKey() {
        return asCoordinatorKey(mirrorName);
    }

    public static String asCoordinatorKey(String mirrorName) {
        return mirrorName;
    }

    public static void validate(String key) {
        Objects.requireNonNull(key, "Mirror name cannot be null");
        if (key.isEmpty()) {
            throw new IllegalArgumentException("Mirror name cannot be empty");
        }
    }
}
