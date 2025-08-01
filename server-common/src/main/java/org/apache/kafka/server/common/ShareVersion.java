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
package org.apache.kafka.server.common;

import java.util.Map;

public enum ShareVersion implements FeatureVersion {

    // Version 0 does not enable share groups.
    SV_0(0, MetadataVersion.MINIMUM_VERSION, Map.of()),

    // Version 1 enables share groups (KIP-932).
    // This is a preview in 4.1, and expected to be production-ready in 4.2.
    SV_1(1, MetadataVersion.IBP_4_2_IV0, Map.of());

    public static final String FEATURE_NAME = "share.version";

    public static final ShareVersion LATEST_PRODUCTION = SV_1;

    private final short featureLevel;
    private final MetadataVersion bootstrapMetadataVersion;
    private final Map<String, Short> dependencies;

    ShareVersion(
        int featureLevel,
        MetadataVersion bootstrapMetadataVersion,
        Map<String, Short> dependencies
    ) {
        this.featureLevel = (short) featureLevel;
        this.bootstrapMetadataVersion = bootstrapMetadataVersion;
        this.dependencies = dependencies;
    }

    @Override
    public short featureLevel() {
        return featureLevel;
    }

    @Override
    public String featureName() {
        return FEATURE_NAME;
    }

    @Override
    public MetadataVersion bootstrapMetadataVersion() {
        return bootstrapMetadataVersion;
    }

    @Override
    public Map<String, Short> dependencies() {
        return dependencies;
    }

    public boolean supportsShareGroups() {
        return featureLevel >= SV_1.featureLevel;
    }

    public static ShareVersion fromFeatureLevel(short version) {
        switch (version) {
            case 0:
                return SV_0;
            case 1:
                return SV_1;
            default:
                throw new RuntimeException("Unknown share feature level: " + (int) version);
        }
    }
}
