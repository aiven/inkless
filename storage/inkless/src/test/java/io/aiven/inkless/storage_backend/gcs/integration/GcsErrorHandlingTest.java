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
import java.util.Set;

import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.storage_backend.common.KeyNotFoundException;
import io.aiven.inkless.storage_backend.common.StorageBackendException;
import io.aiven.inkless.storage_backend.common.StorageBackendTimeoutException;
import io.aiven.inkless.storage_backend.common.fixtures.TestObjectKey;
import io.aiven.inkless.storage_backend.gcs.GcsStorage;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.any;
import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Tag("integration")
@WireMockTest
class GcsErrorHandlingTest {
    private static final String BUCKET_NAME = "test-bucket";
    private static final String BATCH_BOUNDARY = "batch_boundary";
    private static final byte[] DATA = "content".getBytes();
    private final Metrics metrics = new Metrics();
    private final GcsStorage storage = new GcsStorage(metrics);

    @AfterEach
    void tearDown() throws Exception {
        storage.close();
        metrics.close();
    }

    @Test
    void uploadReadTimeout(final WireMockRuntimeInfo wmRuntimeInfo) {
        configure(wmRuntimeInfo, 1L);
        stubFor(any(anyUrl()).willReturn(aResponse().withFixedDelay(100)));

        assertThatThrownBy(() -> upload())
            .isExactlyInstanceOf(StorageBackendTimeoutException.class)
            .hasMessage("Timed out to upload key")
            // The client wraps the socket timeout, so this pins the cause-chain walk in isTimeout.
            .hasRootCauseInstanceOf(SocketTimeoutException.class);
    }

    @Test
    void uploadThrottled(final WireMockRuntimeInfo wmRuntimeInfo) {
        configure(wmRuntimeInfo, 10_000L);
        stubFor(any(anyUrl()).willReturn(aResponse().withStatus(429)));

        assertThatThrownBy(() -> upload()).isExactlyInstanceOf(StorageBackendException.class);
        assertThat(metricTotal("throttling-errors")).isEqualTo(1.0);
    }

    @Test
    void uploadServerError(final WireMockRuntimeInfo wmRuntimeInfo) {
        configure(wmRuntimeInfo, 10_000L);
        stubFor(any(anyUrl()).willReturn(aResponse().withStatus(500)));

        assertThatThrownBy(() -> upload()).isExactlyInstanceOf(StorageBackendException.class);
        assertThat(metricTotal("server-errors")).isEqualTo(1.0);
    }

    @Test
    void fetchMissingKeyCountsAsOtherError(final WireMockRuntimeInfo wmRuntimeInfo) {
        configure(wmRuntimeInfo, 10_000L);
        stubFor(any(anyUrl()).willReturn(aResponse().withStatus(404)));

        assertThatThrownBy(() -> storage.fetch(new TestObjectKey("key"), null))
            .isInstanceOf(KeyNotFoundException.class);
        assertThat(metricTotal("other-errors")).isEqualTo(1.0);
    }

    @Test
    void batchDeleteRequestFailureIsCounted(final WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
        configure(wmRuntimeInfo, 10_000L);
        stubFor(post(urlPathEqualTo("/batch/storage/v1")).willReturn(aResponse().withStatus(503)));

        // The batch request bypasses the client's retry loop, so this pins the caller-side count.
        assertThat(storage.delete(Set.of(new TestObjectKey("key1"), new TestObjectKey("key2")))).isEmpty();
        assertThat(metricTotal("throttling-errors")).isEqualTo(1.0);
    }

    @Test
    void batchDeleteConfirmsOnlyDeletedKeys(final WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
        configure(wmRuntimeInfo, 10_000L);
        stubBatchResponse(deletedSubResponse(), failedSubResponse(403), failedSubResponse(403));

        final Set<ObjectKey> keys = Set.of(
            new TestObjectKey("key1"), new TestObjectKey("key2"), new TestObjectKey("key3"));
        // One of the three is confirmed, and which one depends on the order the keys are batched in.
        // Size is what matters: a pass that confirmed all three would dereference two live files, and
        // an unmatched stub would confirm none.
        assertThat(storage.delete(keys)).hasSize(1).isSubsetOf(keys);
        assertThat(metricTotal("other-errors")).isEqualTo(2.0);
        assertThat(metricTotal("object-delete")).isEqualTo(3.0);
    }

    @Test
    void batchDeleteConfirmsAbsentKeys(final WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
        configure(wmRuntimeInfo, 10_000L);
        stubBatchResponse(failedSubResponse(404), failedSubResponse(404));

        // A 404 leaves nothing to delete, so both keys are confirmed and stop being retried.
        final Set<ObjectKey> keys = Set.of(new TestObjectKey("key1"), new TestObjectKey("key2"));
        assertThat(storage.delete(keys)).isEqualTo(keys);
        assertThat(metricTotal("other-errors")).isZero();
        assertThat(metricTotal("object-delete")).isEqualTo(2.0);
    }

    /**
     * Stubs the batch endpoint with one sub-response per part, matched to the sub-requests by position.
     */
    private static void stubBatchResponse(final String... parts) {
        final StringBuilder body = new StringBuilder();
        for (final String part : parts) {
            body.append("--").append(BATCH_BOUNDARY).append("\r\n")
                .append("Content-Type: application/http\r\n")
                .append("\r\n")
                .append(part);
        }
        body.append("--").append(BATCH_BOUNDARY).append("--\r\n");

        stubFor(post(urlPathEqualTo("/batch/storage/v1")).willReturn(aResponse()
            .withHeader("Content-Type", "multipart/mixed; boundary=" + BATCH_BOUNDARY)
            .withBody(body.toString())));
    }

    private static String deletedSubResponse() {
        return "HTTP/1.1 204 No Content\r\n\r\n";
    }

    private static String failedSubResponse(final int code) {
        return "HTTP/1.1 " + code + " \r\n"
            + "Content-Type: application/json; charset=UTF-8\r\n"
            + "\r\n"
            + "{\"error\":{\"code\":" + code + ",\"message\":\"failed\"}}\r\n";
    }

    private void upload() throws Exception {
        storage.upload(new TestObjectKey("key"), new ByteArrayInputStream(DATA), DATA.length);
    }

    private double metricTotal(final String sensor) {
        return (double) metrics.metric(metrics.metricName(sensor + "-total", "gcs-client-metrics")).metricValue();
    }

    private void configure(final WireMockRuntimeInfo wmRuntimeInfo, final long readTimeoutMs) {
        storage.configure(Map.of(
            "gcs.bucket.name", BUCKET_NAME,
            "gcs.endpoint.url", wmRuntimeInfo.getHttpBaseUrl(),
            "gcs.credentials.default", "false",
            "gcs.read.timeout", readTimeoutMs));
    }
}
