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

public enum MirrorState {
    NONE((byte) 0),
    PREPARING_MIRRORING((byte) 1),
    MIRRORING((byte) 2),
    STOPPING((byte) 4),
    STOPPED((byte) 8),
    FAILED((byte) 16);

    private final byte value;

    MirrorState(byte value) {
        this.value = value;
    }

    public byte value() {
        return value;
    }

    public static MirrorState fromValue(byte value) {
        switch (value) {
            case 0:
                return NONE;
            case 1:
                return PREPARING_MIRRORING;
            case 2:
                return MIRRORING;
            case 4:
                return STOPPING;
            case 8:
                return STOPPED;
            case 16:
                return FAILED;
        }
        throw new IllegalArgumentException("Unknown mirror state: " + value);
    }
}
