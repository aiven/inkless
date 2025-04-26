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

import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.image.ClusterImage;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.server.common.KRaftVersion;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import scala.collection.Map;
import scala.jdk.javaapi.CollectionConverters;

import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;

class InklessKRaftMetadataCacheTest {

    @ParameterizedTest
    @CsvSource({
        "__consumer_offsets,false", "__transaction_state,false", "__share_group_state,false", "__cluster_metadata,false",
        "__other_topic1,false", "__other_topic2__,true", "_other_topic3,false", "other_topic4,true"})
    void isInklessTopic(final String topicName, final boolean expectedIsInkless) {
        Supplier<Map<?, ?>> defaultConfig = () -> CollectionConverters.asScala(Collections.emptyMap());
        // Given a cache with a couple of inkless topics
        final KRaftMetadataCache cache = MetadataCache.kRaftMetadataCache(1, () -> KRaftVersion.KRAFT_VERSION_0);
        final List<ApiMessage> configRecords = List.of(new ConfigRecord()
                .setResourceType(ResourceType.TOPIC.code())
                .setResourceName("__other_topic2__")
                .setName(TopicConfig.INKLESS_ENABLE_CONFIG)
                .setValue("true"),
            new ConfigRecord()
                .setResourceType(ResourceType.TOPIC.code())
                .setResourceName("other_topic4")
                .setName(TopicConfig.INKLESS_ENABLE_CONFIG)
                .setValue("true"));
        updateCache(cache, configRecords);
        // When checking if a topic is inkless, then the expected result is returned
        assertEquals(expectedIsInkless, cache.isInklessTopic(topicName, defaultConfig));
    }

    // Similar to {@link kafka.server.MetadataCacheTest#updateCache}
    private static void updateCache(KRaftMetadataCache c, List<ApiMessage> configRecords) {
        var image = c.currentImage();
        var partialImage = new MetadataImage(
            new MetadataProvenance(100L, 10, 1000L, true),
            image.features(),
            ClusterImage.EMPTY,
            image.topics(),
            image.configs(),
            image.clientQuotas(),
            image.producerIds(),
            image.acls(),
            image.scram(),
            image.delegationTokens());
        var delta = new MetadataDelta.Builder().setImage(partialImage).build();
        configRecords.forEach(delta::replay);
        c.setImage(delta.apply(new MetadataProvenance(100L, 10, 1000L, true)));
    }

}
