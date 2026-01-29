/*
 * Inkless
 * Copyright (C) 2026 Aiven OY
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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.AfterEach;
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

import io.aiven.inkless.control_plane.GetDisklessLogRequest;
import io.aiven.inkless.control_plane.GetDisklessLogResponse;
import io.aiven.inkless.control_plane.InitDisklessLogRequest;
import io.aiven.inkless.test_utils.InklessPostgreSQLContainer;
import io.aiven.inkless.test_utils.PostgreSQLTestContainer;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class GetDisklessLogJobTest {
    @Container
    static final InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();

    static final String TOPIC_1 = "topic1";
    static final Uuid TOPIC_ID1 = new Uuid(10, 12);
    static final String TOPIC_2 = "topic2";
    static final Uuid TOPIC_ID2 = new Uuid(20, 24);

    @BeforeEach
    void setUp(final TestInfo testInfo) {
        pgContainer.createDatabase(testInfo);
        pgContainer.migrate();
    }

    @AfterEach
    void tearDown() {
        pgContainer.tearDown();
    }

    @Test
    void emptyRequestList() throws Exception {
        final GetDisklessLogJob job = new GetDisklessLogJob(
            Time.SYSTEM, pgContainer.getJooqCtx(), List.of(), durationMs -> {});
        final List<GetDisklessLogResponse> responses = job.call();
        assertThat(responses).isEmpty();
    }

    @Test
    void unknownTopicOrPartition() throws Exception {
        // Query for a partition that doesn't exist
        final List<GetDisklessLogRequest> requests = List.of(
            new GetDisklessLogRequest(TOPIC_ID1, 0)
        );
        final GetDisklessLogJob job = new GetDisklessLogJob(
            Time.SYSTEM, pgContainer.getJooqCtx(), requests, durationMs -> {});
        final List<GetDisklessLogResponse> responses = job.call();

        assertThat(responses).hasSize(1);
        final GetDisklessLogResponse response = responses.get(0);
        assertThat(response.topicId()).isEqualTo(TOPIC_ID1);
        assertThat(response.partition()).isEqualTo(0);
        assertThat(response.error()).isEqualTo(Errors.UNKNOWN_TOPIC_OR_PARTITION);
        assertThat(response.logStartOffset()).isEqualTo(GetDisklessLogResponse.INVALID_OFFSET);
        assertThat(response.highWatermark()).isEqualTo(GetDisklessLogResponse.INVALID_OFFSET);
        assertThat(response.disklessStartOffset()).isEqualTo(GetDisklessLogResponse.INVALID_OFFSET);
    }

    @Test
    void getExistingLog() throws Exception {
        // First, create a log entry
        final Set<InitDisklessLogRequest> initRequests = Set.of(
            new InitDisklessLogRequest(TOPIC_ID1, TOPIC_1, 0, 100L, 200L, 5, List.of())
        );
        new InitDisklessLogJob(Time.SYSTEM, pgContainer.getJooqCtx(), initRequests, durationMs -> {}).call();

        // Now query for it
        final List<GetDisklessLogRequest> requests = List.of(
            new GetDisklessLogRequest(TOPIC_ID1, 0)
        );
        final GetDisklessLogJob job = new GetDisklessLogJob(
            Time.SYSTEM, pgContainer.getJooqCtx(), requests, durationMs -> {});
        final List<GetDisklessLogResponse> responses = job.call();

        assertThat(responses).hasSize(1);
        final GetDisklessLogResponse response = responses.get(0);
        assertThat(response.topicId()).isEqualTo(TOPIC_ID1);
        assertThat(response.partition()).isEqualTo(0);
        assertThat(response.error()).isEqualTo(Errors.NONE);
        assertThat(response.logStartOffset()).isEqualTo(100L);
        assertThat(response.highWatermark()).isEqualTo(200L);
        // disklessStartOffset is set to highWatermark when initialized
        assertThat(response.disklessStartOffset()).isEqualTo(200L);
    }

    @Test
    void getMultiplePartitions() throws Exception {
        // Create multiple log entries
        final Set<InitDisklessLogRequest> initRequests = Set.of(
            new InitDisklessLogRequest(TOPIC_ID1, TOPIC_1, 0, 100L, 200L, 5, List.of()),
            new InitDisklessLogRequest(TOPIC_ID1, TOPIC_1, 1, 50L, 150L, 3, List.of()),
            new InitDisklessLogRequest(TOPIC_ID2, TOPIC_2, 0, 0L, 500L, 1, List.of())
        );
        new InitDisklessLogJob(Time.SYSTEM, pgContainer.getJooqCtx(), initRequests, durationMs -> {}).call();

        // Query for all of them
        final List<GetDisklessLogRequest> requests = List.of(
            new GetDisklessLogRequest(TOPIC_ID1, 0),
            new GetDisklessLogRequest(TOPIC_ID1, 1),
            new GetDisklessLogRequest(TOPIC_ID2, 0)
        );
        final GetDisklessLogJob job = new GetDisklessLogJob(
            Time.SYSTEM, pgContainer.getJooqCtx(), requests, durationMs -> {});
        final List<GetDisklessLogResponse> responses = job.call();

        assertThat(responses).hasSize(3);

        // Check topic1 partition 0
        final GetDisklessLogResponse topic1Part0 = responses.stream()
            .filter(r -> r.topicId().equals(TOPIC_ID1) && r.partition() == 0)
            .findFirst()
            .orElseThrow();
        assertThat(topic1Part0.error()).isEqualTo(Errors.NONE);
        assertThat(topic1Part0.logStartOffset()).isEqualTo(100L);
        assertThat(topic1Part0.highWatermark()).isEqualTo(200L);
        assertThat(topic1Part0.disklessStartOffset()).isEqualTo(200L);

        // Check topic1 partition 1
        final GetDisklessLogResponse topic1Part1 = responses.stream()
            .filter(r -> r.topicId().equals(TOPIC_ID1) && r.partition() == 1)
            .findFirst()
            .orElseThrow();
        assertThat(topic1Part1.error()).isEqualTo(Errors.NONE);
        assertThat(topic1Part1.logStartOffset()).isEqualTo(50L);
        assertThat(topic1Part1.highWatermark()).isEqualTo(150L);
        assertThat(topic1Part1.disklessStartOffset()).isEqualTo(150L);

        // Check topic2 partition 0
        final GetDisklessLogResponse topic2Part0 = responses.stream()
            .filter(r -> r.topicId().equals(TOPIC_ID2) && r.partition() == 0)
            .findFirst()
            .orElseThrow();
        assertThat(topic2Part0.error()).isEqualTo(Errors.NONE);
        assertThat(topic2Part0.logStartOffset()).isEqualTo(0L);
        assertThat(topic2Part0.highWatermark()).isEqualTo(500L);
        assertThat(topic2Part0.disklessStartOffset()).isEqualTo(500L);
    }

    @Test
    void duplicateRequestsReturnDuplicateResponses() throws Exception {
        // Create a log entry
        final Set<InitDisklessLogRequest> initRequests = Set.of(
            new InitDisklessLogRequest(TOPIC_ID1, TOPIC_1, 0, 100L, 200L, 5, List.of())
        );
        new InitDisklessLogJob(Time.SYSTEM, pgContainer.getJooqCtx(), initRequests, durationMs -> {}).call();

        // Query for the same partition twice
        final List<GetDisklessLogRequest> requests = List.of(
            new GetDisklessLogRequest(TOPIC_ID1, 0),
            new GetDisklessLogRequest(TOPIC_ID1, 0)
        );
        final GetDisklessLogJob job = new GetDisklessLogJob(
            Time.SYSTEM, pgContainer.getJooqCtx(), requests, durationMs -> {});
        final List<GetDisklessLogResponse> responses = job.call();

        assertThat(responses).hasSize(2);
        for (final GetDisklessLogResponse response : responses) {
            assertThat(response.topicId()).isEqualTo(TOPIC_ID1);
            assertThat(response.partition()).isEqualTo(0);
            assertThat(response.error()).isEqualTo(Errors.NONE);
            assertThat(response.logStartOffset()).isEqualTo(100L);
            assertThat(response.highWatermark()).isEqualTo(200L);
        }
    }
}
