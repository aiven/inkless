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
package org.apache.kafka.storage.log.metrics;

import com.yammer.metrics.core.Meter;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for BrokerTopicStats focusing on how updateBytesOut properly routes to:
 * - All-topics metrics with correct topicType tags (classic/diskless)
 * - Topic-specific metrics (tagged only by topic name)
 */
public class BrokerTopicStatsTest {
    private BrokerTopicStats brokerTopicStats;

    @BeforeEach
    public void setup() {
        brokerTopicStats = new BrokerTopicStats(false);
    }

    @AfterEach
    public void teardown() {
        brokerTopicStats.close();
    }

    @Test
    public void testUpdateBytesOutForClassicTopicUpdatesCorrectMeters() {
        String topic = "classic-topic";
        long bytesToMark = 1000L;

        Meter topicMeter = brokerTopicStats.topicStats(topic).bytesOutRate();
        Meter allTopicsClassicMeter = brokerTopicStats.allTopicsStats().bytesOutRate(false);
        Meter allTopicsDisklessMeter = brokerTopicStats.allTopicsStats().bytesOutRate(true);

        long topicInitial = topicMeter.count();
        long allTopicsClassicInitial = allTopicsClassicMeter.count();
        long allTopicsDisklessInitial = allTopicsDisklessMeter.count();

        // Update for classic topic (isDiskless=false or default)
        brokerTopicStats.updateBytesOut(topic, false, false, bytesToMark, false);

        // Verify:
        // - Topic-specific meter updated (tagged by topic name only)
        // - All-topics classic meter updated (tagged with topicType=classic)
        // - All-topics diskless meter NOT updated (different topicType tag)
        assertEquals(topicInitial + bytesToMark, topicMeter.count(), 
            "Topic-specific meter should be updated (tagged by topic name)");
        assertEquals(allTopicsClassicInitial + bytesToMark, allTopicsClassicMeter.count(), 
            "All-topics classic meter should be updated (topicType=classic tag)");
        assertEquals(allTopicsDisklessInitial, allTopicsDisklessMeter.count(), 
            "All-topics diskless meter should NOT be updated (different topicType tag)");
    }

    @Test
    public void testUpdateBytesOutForDisklessTopicUpdatesCorrectMeters() {
        String topic = "diskless-topic";
        long bytesToMark = 2000L;

        Meter topicMeter = brokerTopicStats.topicStats(topic).bytesOutRate();
        Meter allTopicsClassicMeter = brokerTopicStats.allTopicsStats().bytesOutRate(false);
        Meter allTopicsDisklessMeter = brokerTopicStats.allTopicsStats().bytesOutRate(true);

        long topicInitial = topicMeter.count();
        long allTopicsClassicInitial = allTopicsClassicMeter.count();
        long allTopicsDisklessInitial = allTopicsDisklessMeter.count();

        // Update for diskless topic (isDiskless=true)
        brokerTopicStats.updateBytesOut(topic, false, false, bytesToMark, true);

        // Verify:
        // - Topic-specific meter updated (tagged by topic name only, no topicType)
        // - All-topics diskless meter updated (tagged with topicType=diskless)
        // - All-topics classic meter NOT updated (different topicType tag)
        assertEquals(topicInitial + bytesToMark, topicMeter.count(), 
            "Topic-specific meter should be updated (tagged by topic name only)");
        assertEquals(allTopicsDisklessInitial + bytesToMark, allTopicsDisklessMeter.count(), 
            "All-topics diskless meter should be updated (topicType=diskless tag)");
        assertEquals(allTopicsClassicInitial, allTopicsClassicMeter.count(), 
            "All-topics classic meter should NOT be updated (different topicType tag)");
    }

    @Test
    public void testUpdateBytesOutDefaultBehaviorIsClassic() {
        String topic = "default-topic";
        long bytesToMark = 1500L;

        Meter allTopicsClassicMeter = brokerTopicStats.allTopicsStats().bytesOutRate(false);
        Meter allTopicsDisklessMeter = brokerTopicStats.allTopicsStats().bytesOutRate(true);

        long classicInitial = allTopicsClassicMeter.count();
        long disklessInitial = allTopicsDisklessMeter.count();

        // Call without isDiskless parameter (defaults to false/classic)
        brokerTopicStats.updateBytesOut(topic, false, false, bytesToMark);

        // Should update classic, not diskless
        assertEquals(classicInitial + bytesToMark, allTopicsClassicMeter.count(), 
            "Default updateBytesOut should update classic meter (topicType=classic tag)");
        assertEquals(disklessInitial, allTopicsDisklessMeter.count(), 
            "Default updateBytesOut should NOT update diskless meter");
    }

    @Test
    public void testMixedClassicAndDisklessTopicsUpdateIndependently() {
        String classicTopic = "classic-topic";
        String disklessTopic = "diskless-topic";
        long classicBytes = 1000L;
        long disklessBytes = 2000L;

        Meter allTopicsClassicMeter = brokerTopicStats.allTopicsStats().bytesOutRate(false);
        Meter allTopicsDisklessMeter = brokerTopicStats.allTopicsStats().bytesOutRate(true);

        long classicInitial = allTopicsClassicMeter.count();
        long disklessInitial = allTopicsDisklessMeter.count();

        // Update classic topic
        brokerTopicStats.updateBytesOut(classicTopic, false, false, classicBytes, false);
        
        // Update diskless topic
        brokerTopicStats.updateBytesOut(disklessTopic, false, false, disklessBytes, true);

        // Verify each counter updated independently with correct topicType tags
        assertEquals(classicInitial + classicBytes, allTopicsClassicMeter.count(), 
            "Classic meter should only have classic bytes (topicType=classic tag)");
        assertEquals(disklessInitial + disklessBytes, allTopicsDisklessMeter.count(), 
            "Diskless meter should only have diskless bytes (topicType=diskless tag)");
        
        // Update classic topic again
        brokerTopicStats.updateBytesOut(classicTopic, false, false, classicBytes, false);
        
        // Verify only classic increased
        assertEquals(classicInitial + (2 * classicBytes), allTopicsClassicMeter.count(), 
            "Classic meter should accumulate classic bytes");
        assertEquals(disklessInitial + disklessBytes, allTopicsDisklessMeter.count(), 
            "Diskless meter should remain unchanged");
    }

    @Test
    public void testBytesInForClassicAndDisklessTopics() {
        String classicTopic = "classic-bytes-in";
        String disklessTopic = "diskless-bytes-in";
        long classicBytes = 3000L;
        long disklessBytes = 4000L;

        // Get meters
        Meter classicTopicMeter = brokerTopicStats.topicStats(classicTopic).bytesInRate();
        Meter disklessTopicMeter = brokerTopicStats.topicStats(disklessTopic).bytesInRate();
        Meter allTopicsClassicMeter = brokerTopicStats.allTopicsStats().bytesInRate(false);
        Meter allTopicsDisklessMeter = brokerTopicStats.allTopicsStats().bytesInRate(true);

        long allClassicInitial = allTopicsClassicMeter.count();
        long allDisklessInitial = allTopicsDisklessMeter.count();

        // Mark classic topic bytes in
        classicTopicMeter.mark(classicBytes);
        allTopicsClassicMeter.mark(classicBytes);

        // Mark diskless topic bytes in
        disklessTopicMeter.mark(disklessBytes);
        allTopicsDisklessMeter.mark(disklessBytes);

        // Verify topic-specific meters updated (tagged by topic name only)
        assertEquals(classicBytes, classicTopicMeter.count(), 
            "Classic topic meter should have classic bytes (tagged by topic name)");
        assertEquals(disklessBytes, disklessTopicMeter.count(), 
            "Diskless topic meter should have diskless bytes (tagged by topic name)");

        // Verify all-topics meters updated with correct topicType tags
        assertEquals(allClassicInitial + classicBytes, allTopicsClassicMeter.count(), 
            "All-topics classic meter should have classic bytes (topicType=classic tag)");
        assertEquals(allDisklessInitial + disklessBytes, allTopicsDisklessMeter.count(), 
            "All-topics diskless meter should have diskless bytes (topicType=diskless tag)");
    }

    @Test
    public void testTopicStatsManagement() {
        String topic = "managed-topic";
        
        assertFalse(brokerTopicStats.isTopicStatsExisted(topic), 
            "Topic stats should not exist initially");
        
        BrokerTopicMetrics metrics = brokerTopicStats.topicStats(topic);
        assertNotNull(metrics, "Topic stats should be created");
        
        assertTrue(brokerTopicStats.isTopicStatsExisted(topic), 
            "Topic stats should exist after creation");
        
        brokerTopicStats.removeMetrics(topic);
        assertFalse(brokerTopicStats.isTopicStatsExisted(topic), 
            "Topic stats should be removed");
    }
}
