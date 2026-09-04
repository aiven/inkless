/*
 * Inkless
 * Copyright (C) 2024 - 2026 Aiven OY
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
package io.aiven.inkless.storage_backend.gcs.integration;

import org.apache.kafka.common.metrics.Metrics;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.net.SocketTimeoutException;
import java.util.Map;

import io.aiven.inkless.storage_backend.common.StorageBackendTimeoutException;
import io.aiven.inkless.storage_backend.common.fixtures.TestObjectKey;
import io.aiven.inkless.storage_backend.gcs.GcsStorage;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.any;
import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Tag("integration")
@WireMockTest
class GcsErrorHandlingTest {
    private static final String BUCKET_NAME = "test-bucket";
    private final GcsStorage storage = new GcsStorage(new Metrics());

    @AfterEach
    void tearDown() throws Exception {
        storage.close();
    }

    @Test
    void uploadReadTimeout(final WireMockRuntimeInfo wmRuntimeInfo) {
        configure(wmRuntimeInfo);
        stubFor(any(anyUrl()).willReturn(aResponse().withFixedDelay(100)));

        final byte[] data = "content".getBytes();
        assertThatThrownBy(() -> storage.upload(new TestObjectKey("key"), new ByteArrayInputStream(data), data.length))
            .isExactlyInstanceOf(StorageBackendTimeoutException.class)
            .hasMessage("Timed out to upload key")
            // The client wraps the socket timeout, so this pins the cause-chain walk in isTimeout.
            .hasRootCauseInstanceOf(SocketTimeoutException.class);
    }

    private void configure(final WireMockRuntimeInfo wmRuntimeInfo) {
        storage.configure(Map.of(
            "gcs.bucket.name", BUCKET_NAME,
            "gcs.endpoint.url", wmRuntimeInfo.getHttpBaseUrl(),
            "gcs.credentials.default", "false",
            "gcs.read.timeout", 1L));
    }
}
