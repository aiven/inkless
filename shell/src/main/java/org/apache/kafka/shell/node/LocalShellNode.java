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

package org.apache.kafka.shell.node;

import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.image.node.MetadataLeafNode;
import org.apache.kafka.image.node.MetadataNode;

import java.util.Collection;
import java.util.List;

/**
 * The /local node of the metadata shell, which contains information about the shell itself.
 */
public class LocalShellNode implements MetadataNode {
    /**
     * The name of this node.
     */
    public static final String NAME = "local";

    /**
     * Contains the shell software version.
     */
    public static final String VERSION = "version";

    /**
     * Contains the shell software commit id.
     */
    public static final String COMMIT_ID = "commitId";

    @Override
    public Collection<String> childNames() {
        return List.of(VERSION, COMMIT_ID);
    }

    @Override
    public MetadataNode child(String name) {
        if (name.equals(VERSION)) {
            return new MetadataLeafNode(AppInfoParser.getVersion());
        } else if (name.equals(COMMIT_ID)) {
            return new MetadataLeafNode(AppInfoParser.getCommitId());
        } else {
            return null;
        }
    }
}
