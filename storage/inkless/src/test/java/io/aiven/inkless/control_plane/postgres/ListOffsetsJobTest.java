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
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;
import java.util.Set;

import io.aiven.inkless.common.ObjectFormat;
import io.aiven.inkless.control_plane.CommitBatchRequest;
import io.aiven.inkless.control_plane.CreateTopicAndPartitionsRequest;
import io.aiven.inkless.control_plane.ListOffsetsRequest;
import io.aiven.inkless.control_plane.ListOffsetsResponse;
import io.aiven.inkless.test_utils.InklessPostgreSQLContainer;
import io.aiven.inkless.test_utils.PostgreSQLTestContainer;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class ListOffsetsJobTest {
    @Container
    static final InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();

    static final int BROKER_ID = 11;
    static final long FILE_SIZE = 123456;

    static final String TOPIC_0 = "topic0";
    static final Uuid TOPIC_ID_0 = new Uuid(10, 12);
    static final TopicIdPartition T0P0 = new TopicIdPartition(TOPIC_ID_0, 0, TOPIC_0);

    Time time = new MockTime();

    @BeforeEach
    void setUp(final TestInfo testInfo) {
        pgContainer.createDatabase(testInfo);
        pgContainer.migrate();

        final Set<CreateTopicAndPartitionsRequest> createTopicAndPartitionsRequests = Set.of(
            new CreateTopicAndPartitionsRequest(TOPIC_ID_0, TOPIC_0, 2)
        );
        new TopicsAndPartitionsCreateJob(Time.SYSTEM, pgContainer.getJooqCtx(), createTopicAndPartitionsRequests, duration -> {}).run();
    }

    @Test
    void simpleListOffsets() {
        final String objectKey1 = "obj1";

        final CommitFileJob commitJob = new CommitFileJob(
            time, pgContainer.getJooqCtx(), objectKey1, ObjectFormat.WRITE_AHEAD_MULTI_SEGMENT, BROKER_ID, FILE_SIZE,
            List.of(
                CommitBatchRequest.of(0, T0P0, 0, 1234, 0, 11, 1000, TimestampType.CREATE_TIME)
            ),
            duration -> {}
        );
        assertThat(commitJob.call()).isNotEmpty();

        final ListOffsetsJob job = new ListOffsetsJob(
            time, pgContainer.getJooqCtx(),
            List.of(
                new ListOffsetsRequest(T0P0, -1)
            )
        );
        final List<ListOffsetsResponse> result = job.call();

        System.out.println(result);
        assertThat(result).containsExactly(
            new ListOffsetsResponse(Errors.NONE, T0P0, -1, 12)
        );
    }
}
