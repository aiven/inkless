/*
 * Inkless
 * Copyright (C) 2025 Aiven OY
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
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import io.aiven.inkless.control_plane.CreateTopicAndPartitionsRequest;
import io.aiven.inkless.control_plane.EnforceRetentionRequest;
import io.aiven.inkless.control_plane.EnforceRetentionResponse;
import io.aiven.inkless.test_utils.InklessPostgreSQLContainer;
import io.aiven.inkless.test_utils.PostgreSQLTestContainer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The majority of testing is done in AbstractControlPlaneTest. Here we only test some behaviors specific to PG.
 */
@Testcontainers
class EnforceRetentionJobTest {
    @Container
    static final InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();

    static final String TOPIC_0 = "topic0";
    static final String TOPIC_1 = "topic1";
    static final Uuid TOPIC_ID_0 = new Uuid(10, 12);
    static final Uuid TOPIC_ID_1 = new Uuid(555, 333);

    Time time = new MockTime();
    Consumer<Long> durationCallback = duration -> {};

    @BeforeEach
    void setUp(final TestInfo testInfo) {
        pgContainer.createDatabase(testInfo);
        pgContainer.migrate();

        final Set<CreateTopicAndPartitionsRequest> createTopicAndPartitionsRequests = Set.of(
            new CreateTopicAndPartitionsRequest(TOPIC_ID_0, TOPIC_0, 100),
            new CreateTopicAndPartitionsRequest(TOPIC_ID_1, TOPIC_1, 100)
        );
        new TopicsAndPartitionsCreateJob(Time.SYSTEM, pgContainer.getJooqCtx(), createTopicAndPartitionsRequests, durationCallback)
            .run();
    }

    @AfterEach
    void tearDown() {
        pgContainer.tearDown();
    }

    @Test
    void forMultiplePartitionsInArbitraryOrder() throws Exception {
        final EnforceRetentionJob job = new EnforceRetentionJob(
            time,
            pgContainer.getJooqCtx(),
            List.of(
                new EnforceRetentionRequest(TOPIC_ID_0, 99, 1, 1),
                new EnforceRetentionRequest(TOPIC_ID_1, 1, 1, 1),
                new EnforceRetentionRequest(TOPIC_ID_1, 0, 1, 1),
                new EnforceRetentionRequest(TOPIC_ID_0, 3, 1, 1),
                new EnforceRetentionRequest(TOPIC_ID_0, 1000, 1, 1),  // non-existent
                new EnforceRetentionRequest(TOPIC_ID_1, 5, 1, 1),
                new EnforceRetentionRequest(TOPIC_ID_1, 5, 1, 1)  // duplicate
            ),
            0,
            durationCallback);
        assertThat(job.call()).containsExactly(
            EnforceRetentionResponse.success(0, 0, 0),
            EnforceRetentionResponse.success(0, 0, 0),
            EnforceRetentionResponse.success(0, 0, 0),
            EnforceRetentionResponse.success(0, 0, 0),
            EnforceRetentionResponse.unknownTopicOrPartition(),
            EnforceRetentionResponse.success(0, 0, 0),
            EnforceRetentionResponse.success(0, 0, 0)
        );
    }
}
