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

package io.aiven.inkless.storage_backend.gcs;

import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.CumulativeCount;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.utils.Time;

import com.google.api.client.http.*;
import com.google.cloud.ServiceOptions;
import com.google.cloud.http.HttpTransportOptions;
import com.groupcdg.pitest.annotations.CoverageIgnore;

import java.util.List;
import java.util.regex.Pattern;

import static io.aiven.inkless.storage_backend.gcs.MetricRegistry.METRIC_CONTEXT;
import static io.aiven.inkless.storage_backend.gcs.MetricRegistry.OBJECT_DELETE;
import static io.aiven.inkless.storage_backend.gcs.MetricRegistry.OBJECT_DELETE_RATE_METRIC_NAME;
import static io.aiven.inkless.storage_backend.gcs.MetricRegistry.OBJECT_DELETE_TOTAL_METRIC_NAME;
import static io.aiven.inkless.storage_backend.gcs.MetricRegistry.OBJECT_GET;
import static io.aiven.inkless.storage_backend.gcs.MetricRegistry.OBJECT_GET_RATE_METRIC_NAME;
import static io.aiven.inkless.storage_backend.gcs.MetricRegistry.OBJECT_GET_TOTAL_METRIC_NAME;
import static io.aiven.inkless.storage_backend.gcs.MetricRegistry.OBJECT_METADATA_GET;
import static io.aiven.inkless.storage_backend.gcs.MetricRegistry.OBJECT_METADATA_GET_RATE_METRIC_NAME;
import static io.aiven.inkless.storage_backend.gcs.MetricRegistry.OBJECT_METADATA_GET_TOTAL_METRIC_NAME;
import static io.aiven.inkless.storage_backend.gcs.MetricRegistry.RESUMABLE_CHUNK_UPLOAD;
import static io.aiven.inkless.storage_backend.gcs.MetricRegistry.RESUMABLE_CHUNK_UPLOAD_RATE_METRIC_NAME;
import static io.aiven.inkless.storage_backend.gcs.MetricRegistry.RESUMABLE_CHUNK_UPLOAD_TOTAL_METRIC_NAME;
import static io.aiven.inkless.storage_backend.gcs.MetricRegistry.RESUMABLE_UPLOAD_INITIATE;
import static io.aiven.inkless.storage_backend.gcs.MetricRegistry.RESUMABLE_UPLOAD_INITIATE_RATE_METRIC_NAME;
import static io.aiven.inkless.storage_backend.gcs.MetricRegistry.RESUMABLE_UPLOAD_INITIATE_TOTAL_METRIC_NAME;

@CoverageIgnore  // tested on integration level
public class MetricCollector {
    private final org.apache.kafka.common.metrics.Metrics metrics;

    /**
     * The pattern for object metadata paths.
     *
     * <p>That is, {@literal "/storage/v1/b/<bucket>/o/<object>"}.
     */
    static final Pattern OBJECT_METADATA_PATH_PATTERN =
        Pattern.compile("^/storage/v1/b/([^/]+)/o/([^/]+)/?$");

    /**
     * The pattern for object download paths.
     *
     * <p>That is, {@literal "/download/storage/v1/b/<bucket>/o/<object>"}.
     */
    static final Pattern OBJECT_DOWNLOAD_PATH_PATTERN =
        Pattern.compile("^/download/storage/v1/b/([^/]+)/o/([^/]+)/?$");

    /**
     * The pattern for object upload paths.
     *
     * <p>That is, {@literal "/upload/storage/v1/b/<bucket>/o"}.
     */
    static final Pattern OBJECT_UPLOAD_PATH_PATTERN =
        Pattern.compile("^/upload/storage/v1/b/([^/]+)/o/?$");

    private final Sensor getObjectMetadataRequests;
    private final Sensor deleteObjectRequests;
    private final Sensor resumableUploadInitiateRequests;
    private final Sensor resumableChunkUploadRequests;
    private final Sensor getObjectRequests;

    public MetricCollector() {
        final JmxReporter reporter = new JmxReporter();

        metrics = new org.apache.kafka.common.metrics.Metrics(
            new MetricConfig(), List.of(reporter), Time.SYSTEM,
            new KafkaMetricsContext(METRIC_CONTEXT)
        );

        getObjectMetadataRequests = createSensor(
            OBJECT_METADATA_GET,
            OBJECT_METADATA_GET_RATE_METRIC_NAME,
            OBJECT_METADATA_GET_TOTAL_METRIC_NAME
        );
        getObjectRequests = createSensor(
            OBJECT_GET,
            OBJECT_GET_RATE_METRIC_NAME,
            OBJECT_GET_TOTAL_METRIC_NAME
        );
        deleteObjectRequests = createSensor(
            OBJECT_DELETE,
            OBJECT_DELETE_RATE_METRIC_NAME,
            OBJECT_DELETE_TOTAL_METRIC_NAME
        );
        resumableUploadInitiateRequests = createSensor(
            RESUMABLE_UPLOAD_INITIATE,
            RESUMABLE_UPLOAD_INITIATE_RATE_METRIC_NAME,
            RESUMABLE_UPLOAD_INITIATE_TOTAL_METRIC_NAME
        );
        resumableChunkUploadRequests = createSensor(
            RESUMABLE_CHUNK_UPLOAD,
            RESUMABLE_CHUNK_UPLOAD_RATE_METRIC_NAME,
            RESUMABLE_CHUNK_UPLOAD_TOTAL_METRIC_NAME
        );
    }

    private Sensor createSensor(
        final String name,
        final MetricNameTemplate rateMetricName,
        final MetricNameTemplate totalMetricName
    ) {
        final Sensor sensor = metrics.sensor(name);
        sensor.add(metrics.metricInstance(rateMetricName), new Rate());
        sensor.add(metrics.metricInstance(totalMetricName), new CumulativeCount());
        return sensor;
    }

    private final MetricResponseInterceptor metricResponseInterceptor = new MetricResponseInterceptor();

    private class MetricResponseInterceptor implements HttpResponseInterceptor {

        @Override
        public void interceptResponse(final HttpResponse response) {
            final HttpRequest request = response.getRequest();
            final GenericUrl url = request.getUrl();

            if (OBJECT_METADATA_PATH_PATTERN.matcher(url.getRawPath()).matches()) {
                // Single object metadata operations: metadata gets and object deletions.
                if (request.getRequestMethod().equals(HttpMethods.GET)) {
                    getObjectMetadataRequests.record();
                } else if (request.getRequestMethod().equals(HttpMethods.DELETE)) {
                    deleteObjectRequests.record();
                }
            } else if (OBJECT_DOWNLOAD_PATH_PATTERN.matcher(url.getRawPath()).matches()) {
                // Object download operations.
                if (request.getRequestMethod().equals(HttpMethods.GET)) {
                    getObjectRequests.record();
                }
            } else if (OBJECT_UPLOAD_PATH_PATTERN.matcher(url.getRawPath()).matches()) {
                // Object upload operations.
                if (request.getRequestMethod().equals(HttpMethods.POST)
                    && url.getFirst("uploadType").equals("resumable")) {
                    resumableUploadInitiateRequests.record();
                } else if (request.getRequestMethod().equals(HttpMethods.PUT)
                    && url.getFirst("uploadType").equals("resumable")
                    && url.containsKey("upload_id")) {
                    resumableChunkUploadRequests.record();
                }
            }
        }
    }

    HttpTransportOptions httpTransportOptions(final HttpTransportOptions.Builder builder) {
        return new HttpTransportOptions(builder) {
            @Override
            public HttpRequestInitializer getHttpRequestInitializer(final ServiceOptions<?, ?> serviceOptions) {
                final var superInitializer = super.getHttpRequestInitializer(serviceOptions);
                return request -> {
                    superInitializer.initialize(request);
                    request.setResponseInterceptor(metricResponseInterceptor);
                };
            }
        };
    }
}
