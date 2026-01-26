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
 * A listing of a cluster mirror.
 */
@InterfaceStability.Evolving
public class MirrorListing {
    private final String mirrorName;
    private final String sourceBootstrap;

    /**
     * Create an instance with the specified parameters.
     *
     * @param mirrorName Mirror name
     * @param sourceBootstrap Source cluster bootstrap servers
     */
    public MirrorListing(String mirrorName, String sourceBootstrap) {
        this.mirrorName = mirrorName;
        this.sourceBootstrap = sourceBootstrap;
    }

    /**
     * The mirror name.
     *
     * @return Mirror name
     */
    public String mirrorName() {
        return mirrorName;
    }

    /**
     * The source cluster bootstrap servers.
     *
     * @return Source bootstrap servers, or null if not available
     */
    public String sourceBootstrap() {
        return sourceBootstrap;
    }

    @Override
    public String toString() {
        return "MirrorListing(mirrorName='" + mirrorName + "', sourceBootstrap='" + sourceBootstrap + "')";
    }

    @Override
    public int hashCode() {
        return Objects.hash(mirrorName, sourceBootstrap);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MirrorListing)) return false;
        MirrorListing that = (MirrorListing) o;
        return Objects.equals(mirrorName, that.mirrorName) &&
               Objects.equals(sourceBootstrap, that.sourceBootstrap);
    }
}
