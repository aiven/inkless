/*
 * Inkless
 * Copyright (C) 2024 - 2025 Aiven OY
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.aiven.inkless.control_plane;

import org.apache.kafka.common.Uuid;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CreateTopicAndPartitionsRequestTest {
    private static final Uuid TOPIC_ID = new Uuid(10, 12);
    private static final String TOPIC = "topic1";

    @Test
    void topicCreationCoversEveryPartition() {
        assertThat(new CreateTopicAndPartitionsRequest(TOPIC_ID, TOPIC, 4).firstPartition()).isZero();
        assertThat(new CreateTopicAndPartitionsRequest(TOPIC_ID, TOPIC, 4).partitionsToCreate()).isEqualTo(4);
    }

    @Test
    void partitionIncreaseCoversOnlyTheAddedPartitions() {
        assertThat(new CreateTopicAndPartitionsRequest(TOPIC_ID, TOPIC, 2, 4).partitionsToCreate()).isEqualTo(2);
        assertThat(new CreateTopicAndPartitionsRequest(TOPIC_ID, TOPIC, 4, 4).partitionsToCreate()).isZero();
    }

    @Test
    void rejectsFirstPartitionOutsideThePartitionRange() {
        assertThatThrownBy(() -> new CreateTopicAndPartitionsRequest(TOPIC_ID, TOPIC, 3, 2))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("firstPartition");
        assertThatThrownBy(() -> new CreateTopicAndPartitionsRequest(TOPIC_ID, TOPIC, -1, 2))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("firstPartition");
    }
}
