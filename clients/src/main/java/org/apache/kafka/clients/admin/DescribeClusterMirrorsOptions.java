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
import org.apache.kafka.common.message.DescribeClusterMirrorsRequestData;

import java.util.Collections;
import java.util.List;

/**
 * Options for {@link Admin#describeClusterMirrors(java.util.Collection, DescribeClusterMirrorsOptions)}.
 *
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Evolving
public class DescribeClusterMirrorsOptions extends AbstractOptions<DescribeClusterMirrorsOptions> {

    private boolean includeAuthorizedOperations = false;
    private String clusterId;
    private List<DescribeClusterMirrorsRequestData.LmeLookup> lmeLookups = Collections.emptyList();

    /**
     * Set whether authorized operations should be included in the response.
     *
     * @param includeAuthorizedOperations whether to include authorized operations
     * @return this instance
     */
    public DescribeClusterMirrorsOptions includeAuthorizedOperations(boolean includeAuthorizedOperations) {
        this.includeAuthorizedOperations = includeAuthorizedOperations;
        return this;
    }

    /**
     * Return true if authorized operations should be included in the response.
     */
    public boolean includeAuthorizedOperations() {
        return includeAuthorizedOperations;
    }

    public DescribeClusterMirrorsOptions clusterId(String clusterId) {
        this.clusterId = clusterId;
        return this;
    }

    public String clusterId() {
        return clusterId;
    }

    public DescribeClusterMirrorsOptions lmeLookups(List<DescribeClusterMirrorsRequestData.LmeLookup> lmeLookups) {
        this.lmeLookups = lmeLookups;
        return this;
    }

    public List<DescribeClusterMirrorsRequestData.LmeLookup> lmeLookups() {
        return lmeLookups;
    }
}
