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

package kafka.server.metadata;

import kafka.server.MetadataCache;

import org.apache.kafka.server.common.KRaftVersion;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

class InklessKRaftMetadataCacheTest {

    @ParameterizedTest
    @CsvSource({
        "__consumer_offsets,false", "__transaction_state,false", "__share_group_state,false", "__cluster_metadata,false",
        "__other_topic1,true", "__other_topic2__,true", "_other_topic3,true", "other_topic4,true"})
    void isInklessTopic(final String topicName, final boolean expectedIsInkless) {
        final KRaftMetadataCache cache = MetadataCache.kRaftMetadataCache(1, () -> KRaftVersion.KRAFT_VERSION_0);
        assertEquals(expectedIsInkless, cache.isInklessTopic(topicName));
    }
}
