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

package io.aiven.inkless.storage_backend.gcs;

import org.apache.kafka.common.metrics.Metrics;

import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpUnsuccessfulResponseHandler;
import com.google.auth.Credentials;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.StorageOptions;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MetricCollectorTest {
    private final Metrics metrics = new Metrics();

    @AfterEach
    void tearDown() {
        metrics.close();
    }

    @Test
    void countsResponsesRetriedByCredentialsHandler() throws IOException {
        final Credentials credentials = mock(Credentials.class);
        when(credentials.getUniverseDomain()).thenReturn(Credentials.GOOGLE_DEFAULT_UNIVERSE);

        final AtomicReference<HttpUnsuccessfulResponseHandler> unsuccessfulHandler = new AtomicReference<>();
        final HttpRequest request = mock(HttpRequest.class);
        when(request.getHeaders()).thenReturn(new HttpHeaders());
        when(request.setUnsuccessfulResponseHandler(any())).thenAnswer(invocation -> {
            unsuccessfulHandler.set(invocation.getArgument(0));
            return request;
        });
        when(request.getUnsuccessfulResponseHandler()).thenAnswer(ignored -> unsuccessfulHandler.get());

        final MetricCollector collector = new MetricCollector(metrics);
        final HttpTransportOptions transportOptions =
            collector.httpTransportOptions(HttpTransportOptions.newBuilder());
        final StorageOptions storageOptions = StorageOptions.newBuilder()
            .setProjectId("test-project")
            .setCredentials(credentials)
            .build();
        transportOptions.getHttpRequestInitializer(storageOptions).initialize(request);

        final HttpResponse unauthorized = mock(HttpResponse.class);
        when(unauthorized.getHeaders()).thenReturn(new HttpHeaders());
        when(unauthorized.getStatusCode()).thenReturn(401);

        assertThat(unsuccessfulHandler.get().handleResponse(request, unauthorized, true)).isTrue();
        assertThat(unsuccessfulHandler.get().handleResponse(request, unauthorized, true)).isTrue();

        assertThat(errorTotal("other-errors")).isEqualTo(2.0);
        verify(credentials, times(2)).refresh();
    }

    private double errorTotal(final String sensor) {
        return (double) metrics.metric(metrics.metricName(sensor + "-total", "gcs-client-metrics")).metricValue();
    }
}
