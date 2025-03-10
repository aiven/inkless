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
package io.aiven.inkless.storage_backend.s3.integration;

import com.github.tomakehurst.wiremock.http.Fault;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.lang.management.ManagementFactory;
import java.util.Map;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.storage_backend.common.StorageBackendException;
import io.aiven.inkless.storage_backend.common.StorageBackendTimeoutException;
import io.aiven.inkless.storage_backend.common.fixtures.TestObjectKey;
import io.aiven.inkless.storage_backend.s3.S3Storage;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.http.HttpStatusCode;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.model.S3Exception;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.any;
import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.common.ContentTypes.CONTENT_TYPE;
import static org.apache.hc.core5.http.ContentType.APPLICATION_XML;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.assertj.core.api.InstanceOfAssertFactories.DOUBLE;

@Tag("integration")
@WireMockTest
class S3ErrorMetricsTest {
    private static final String ERROR_RESPONSE_TEMPLATE = "<Error><Code>%s</Code></Error>";
    private static final MBeanServer MBEAN_SERVER = ManagementFactory.getPlatformMBeanServer();
    private static final String BUCKET_NAME = "test-bucket";
    private S3Storage storage;
    private ObjectName s3MetricsObjectName;

    @BeforeEach
    void setUp() throws MalformedObjectNameException {
        s3MetricsObjectName = ObjectName.getInstance("aiven.inkless.server.s3:type=s3-client-metrics");
        storage = new S3Storage();
    }

    @ParameterizedTest
    @CsvSource({
        HttpStatusCode.INTERNAL_SERVER_ERROR + ", server-errors",
        HttpStatusCode.THROTTLING + ", throttling-errors",
    })
    void testS3ServerExceptions(final int statusCode,
                                final String metricName,
                                final WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
        final Map<String, Object> configs = Map.of(
            "s3.bucket.name", BUCKET_NAME,
            "s3.region", Region.US_EAST_1.id(),
            "s3.endpoint.url", wmRuntimeInfo.getHttpBaseUrl(),
            "s3.path.style.access.enabled", "true",
            "aws.credentials.provider.class", AnonymousCredentialsProvider.class.getName()
        );
        storage.configure(configs);

        stubFor(any(anyUrl())
            .willReturn(aResponse().withStatus(statusCode)
                .withHeader(CONTENT_TYPE, APPLICATION_XML.getMimeType())
                .withBody(String.format(ERROR_RESPONSE_TEMPLATE, statusCode))));
        final StorageBackendException storageBackendException = catchThrowableOfType(
            StorageBackendException.class,
            () -> storage.upload(new TestObjectKey("key"), new byte[0]));
        assertThat(storageBackendException.getCause()).isInstanceOf(S3Exception.class);
        final S3Exception s3Exception = (S3Exception) storageBackendException.getCause();

        assertThat(s3Exception.statusCode()).isEqualTo(statusCode);

        // Comparing to 4 since the SDK makes 3 retries by default.
        assertThat(MBEAN_SERVER.getAttribute(s3MetricsObjectName, metricName + "-total"))
            .isEqualTo(4.0);
        assertThat(MBEAN_SERVER.getAttribute(s3MetricsObjectName, metricName + "-rate"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
    }

    @Test
    void apiCallAttemptTimeout(final WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
        final Map<String, Object> configs = Map.of(
            "s3.bucket.name", BUCKET_NAME,
            "s3.region", Region.US_EAST_1.id(),
            "s3.endpoint.url", wmRuntimeInfo.getHttpBaseUrl(),
            "s3.path.style.access.enabled", "true",
            "s3.api.call.attempt.timeout", 1,
            "aws.credentials.provider.class", AnonymousCredentialsProvider.class.getName()
        );
        storage.configure(configs);
        final var metricName = "configured-timeout-errors";

        stubFor(any(anyUrl()).willReturn(aResponse().withFixedDelay(100)));

        assertThatThrownBy(() -> storage.fetch(new TestObjectKey("key"), ByteRange.maxRange()))
            .isExactlyInstanceOf(StorageBackendTimeoutException.class)
            .hasMessage("Failed to fetch key")
            .hasRootCauseExactlyInstanceOf(ApiCallAttemptTimeoutException.class)
            .hasRootCauseMessage(
                "HTTP request execution did not complete before the specified timeout configuration: 1 millis");

        // Comparing to 4 since the SDK makes 3 retries by default.
        assertThat(MBEAN_SERVER.getAttribute(s3MetricsObjectName, metricName + "-total"))
            .isEqualTo(4.0);
        assertThat(MBEAN_SERVER.getAttribute(s3MetricsObjectName, metricName + "-rate"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
    }

    @Test
    void ioErrors(final WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
        final Map<String, Object> configs = Map.of(
            "s3.bucket.name", BUCKET_NAME,
            "s3.region", Region.US_EAST_1.id(),
            "s3.endpoint.url", wmRuntimeInfo.getHttpBaseUrl(),
            "s3.path.style.access.enabled", "true",
            "aws.credentials.provider.class", AnonymousCredentialsProvider.class.getName()
        );
        storage.configure(configs);

        final var metricName = "io-errors";

        stubFor(any(anyUrl())
            .willReturn(aResponse()
                .withStatus(HttpStatusCode.OK)
                .withFault(Fault.RANDOM_DATA_THEN_CLOSE)));

        assertThatThrownBy(() -> storage.fetch(new TestObjectKey("key"), ByteRange.maxRange()))
            .isExactlyInstanceOf(StorageBackendException.class)
            .hasMessage("Failed to fetch key")
            .hasCauseExactlyInstanceOf(SdkClientException.class)
            .cause().hasMessage("Unable to execute HTTP request: null");

        // Comparing to 4 since the SDK makes 3 retries by default.
        assertThat(MBEAN_SERVER.getAttribute(s3MetricsObjectName, metricName + "-total"))
            .isEqualTo(4.0);
        assertThat(MBEAN_SERVER.getAttribute(s3MetricsObjectName, metricName + "-rate"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
    }
}
