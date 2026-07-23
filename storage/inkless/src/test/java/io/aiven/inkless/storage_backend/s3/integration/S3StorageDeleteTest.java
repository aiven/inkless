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
package io.aiven.inkless.storage_backend.s3.integration;

import org.apache.kafka.common.metrics.Metrics;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.storage_backend.common.fixtures.TestObjectKey;
import io.aiven.inkless.storage_backend.s3.S3Storage;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.regions.Region;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static com.github.tomakehurst.wiremock.client.WireMock.exactly;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static com.github.tomakehurst.wiremock.common.ContentTypes.CONTENT_TYPE;
import static org.apache.hc.core5.http.ContentType.APPLICATION_XML;
import static org.assertj.core.api.Assertions.assertThat;

@Tag("integration")
@WireMockTest
class S3StorageDeleteTest {
    private static final String BUCKET_NAME = "test-bucket";

    private final Metrics metrics = new Metrics();
    private final S3Storage storage = new S3Storage(metrics);

    @AfterEach
    void tearDown() throws Exception {
        storage.close();
    }

    private void configure(final WireMockRuntimeInfo wmRuntimeInfo) {
        storage.configure(Map.of(
            "s3.bucket.name", BUCKET_NAME,
            "s3.region", Region.US_EAST_1.id(),
            "s3.endpoint.url", wmRuntimeInfo.getHttpBaseUrl(),
            "s3.path.style.access.enabled", "true",
            "aws.credentials.provider.class", AnonymousCredentialsProvider.class.getName()
        ));
    }

    private static String deleteResult(final String body) {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
            + "<DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">" + body + "</DeleteResult>";
    }

    private static String deleted(final String key) {
        return "<Deleted><Key>" + key + "</Key></Deleted>";
    }

    private static String error(final String key, final String code) {
        return "<Error><Key>" + key + "</Key><Code>" + code + "</Code><Message>msg</Message></Error>";
    }

    private static Set<String> values(final Set<ObjectKey> keys) {
        return keys.stream().map(ObjectKey::value).collect(Collectors.toSet());
    }

    @Test
    void returnsOnlyConfirmedDeletionsAndDoesNotRetry(final WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
        configure(wmRuntimeInfo);
        // key0 deleted; key1 throttled; key2 hard error. Neither failed key is retried in-call:
        // both are left for the next FileCleaner cycle, and only the confirmed deletion is returned.
        stubFor(post(anyUrl())
            .willReturn(aResponse().withStatus(200)
                .withHeader(CONTENT_TYPE, APPLICATION_XML.getMimeType())
                .withBody(deleteResult(
                    deleted("key0") + error("key1", "SlowDown") + error("key2", "AccessDenied")))));

        final Set<ObjectKey> deleted = storage.delete(Set.of(
            new TestObjectKey("key0"), new TestObjectKey("key1"), new TestObjectKey("key2")));

        assertThat(values(deleted)).containsExactlyInAnyOrder("key0");
        verify(exactly(1), postRequestedFor(anyUrl()));
    }

    @Test
    void doesNotThrowWhenWholeRequestFails(final WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
        configure(wmRuntimeInfo);
        // A whole-request throttle (503) that the SDK's adaptive retry exhausts must not propagate:
        // the keys are simply reported as not deleted and left for the next cycle.
        stubFor(post(anyUrl()).willReturn(aResponse().withStatus(503)
            .withHeader(CONTENT_TYPE, APPLICATION_XML.getMimeType())
            .withBody("<Error><Code>SlowDown</Code><Message>Please reduce your request rate.</Message></Error>")));

        final Set<ObjectKey> deleted =
            storage.delete(Set.of(new TestObjectKey("key0"), new TestObjectKey("key1")));

        assertThat(deleted).isEmpty();
    }
}
