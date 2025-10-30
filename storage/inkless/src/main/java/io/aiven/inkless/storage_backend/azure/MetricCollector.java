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

package io.aiven.inkless.storage_backend.azure;

import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.*;
import org.apache.kafka.common.metrics.stats.CumulativeCount;
import org.apache.kafka.common.metrics.stats.Rate;

import com.azure.core.http.HttpPipelineCallContext;
import com.azure.core.http.HttpPipelineNextPolicy;
import com.azure.core.http.HttpPipelineNextSyncPolicy;
import com.azure.core.http.HttpResponse;
import com.azure.core.http.policy.HttpPipelinePolicy;
import com.groupcdg.pitest.annotations.CoverageIgnore;

import java.util.regex.Pattern;

import io.aiven.inkless.common.metrics.SensorProvider;
import reactor.core.publisher.Mono;

import static io.aiven.inkless.storage_backend.azure.MetricRegistry.BLOB_DELETE;
import static io.aiven.inkless.storage_backend.azure.MetricRegistry.BLOB_DELETE_RATE_METRIC_NAME;
import static io.aiven.inkless.storage_backend.azure.MetricRegistry.BLOB_DELETE_TOTAL_METRIC_NAME;
import static io.aiven.inkless.storage_backend.azure.MetricRegistry.BLOB_GET;
import static io.aiven.inkless.storage_backend.azure.MetricRegistry.BLOB_GET_RATE_METRIC_NAME;
import static io.aiven.inkless.storage_backend.azure.MetricRegistry.BLOB_GET_TOTAL_METRIC_NAME;
import static io.aiven.inkless.storage_backend.azure.MetricRegistry.BLOB_UPLOAD;
import static io.aiven.inkless.storage_backend.azure.MetricRegistry.BLOB_UPLOAD_RATE_METRIC_NAME;
import static io.aiven.inkless.storage_backend.azure.MetricRegistry.BLOB_UPLOAD_TOTAL_METRIC_NAME;
import static io.aiven.inkless.storage_backend.azure.MetricRegistry.BLOCK_LIST_UPLOAD;
import static io.aiven.inkless.storage_backend.azure.MetricRegistry.BLOCK_LIST_UPLOAD_RATE_METRIC_NAME;
import static io.aiven.inkless.storage_backend.azure.MetricRegistry.BLOCK_LIST_UPLOAD_TOTAL_METRIC_NAME;
import static io.aiven.inkless.storage_backend.azure.MetricRegistry.BLOCK_UPLOAD;
import static io.aiven.inkless.storage_backend.azure.MetricRegistry.BLOCK_UPLOAD_RATE_METRIC_NAME;
import static io.aiven.inkless.storage_backend.azure.MetricRegistry.BLOCK_UPLOAD_TOTAL_METRIC_NAME;

@CoverageIgnore // tested on integration level
public class MetricCollector {
    final Metrics metrics;

    public MetricCollector(final Metrics metrics) {
        this.metrics = metrics;
    }

    final Pattern pathPattern(final AzureBlobStorageConfig config) {
        // account is in the hostname when on azure, but included on the path when testing on Azurite
        final var maybeAccountName = "(/" + config.accountName() + ")?";
        final var exp = "^" + maybeAccountName + "/" + config.containerName() + "/" + "([^/]+)";
        return Pattern.compile(exp);
    }

    MetricsPolicy policy(final AzureBlobStorageConfig config) {
        return new MetricsPolicy(metrics, pathPattern(config));
    }

    static class MetricsPolicy implements HttpPipelinePolicy {

        static final Pattern UPLOAD_QUERY_PATTERN = Pattern.compile("comp=(?<comp>[^&]+)");

        private final Sensor deleteBlobRequests;
        private final Sensor uploadBlobRequests;
        private final Sensor uploadBlockRequests;
        private final Sensor uploadBlockListRequests;
        private final Sensor getBlobRequests;

        private final Metrics metrics;
        private final Pattern pathPattern;

        MetricsPolicy(final Metrics metrics, final Pattern pathPattern) {
            this.metrics = metrics;
            this.pathPattern = pathPattern;
            this.deleteBlobRequests = createSensor(
                BLOB_DELETE,
                BLOB_DELETE_RATE_METRIC_NAME,
                BLOB_DELETE_TOTAL_METRIC_NAME
            );
            this.uploadBlobRequests = createSensor(
                BLOB_UPLOAD,
                BLOB_UPLOAD_RATE_METRIC_NAME,
                BLOB_UPLOAD_TOTAL_METRIC_NAME
            );
            this.uploadBlockRequests = createSensor(
                BLOCK_UPLOAD,
                BLOCK_UPLOAD_RATE_METRIC_NAME,
                BLOCK_UPLOAD_TOTAL_METRIC_NAME
            );
            this.uploadBlockListRequests = createSensor(
                BLOCK_LIST_UPLOAD,
                BLOCK_LIST_UPLOAD_RATE_METRIC_NAME,
                BLOCK_LIST_UPLOAD_TOTAL_METRIC_NAME
            );
            this.getBlobRequests = createSensor(
                BLOB_GET,
                BLOB_GET_RATE_METRIC_NAME,
                BLOB_GET_TOTAL_METRIC_NAME
            );
        }

        private Sensor createSensor(
            final String name,
            final MetricNameTemplate rateMetricName,
            final MetricNameTemplate totalMetricName
        ) {
            return new SensorProvider(metrics, name)
                .with(rateMetricName, new Rate())
                .with(totalMetricName, new CumulativeCount())
                .get();
        }

        @Override
        public Mono<HttpResponse> process(final HttpPipelineCallContext context, final HttpPipelineNextPolicy next) {
            processMetrics(context);
            return next.process();
        }

        @Override
        public HttpResponse processSync(final HttpPipelineCallContext context, final HttpPipelineNextSyncPolicy next) {
            processMetrics(context);
            return next.processSync();
        }

        void processMetrics(final HttpPipelineCallContext context) {
            final var httpRequest = context.getHttpRequest();
            final var path = httpRequest.getUrl().getPath();
            if (pathPattern.matcher(path).matches()) {
                switch (httpRequest.getHttpMethod()) {
                    case GET:
                        getBlobRequests.record();
                        break;
                    case PUT:
                        final var q = httpRequest.getUrl().getQuery();
                        if (q == null) {
                            uploadBlobRequests.record();
                            break;
                        }
                        final var matcher = UPLOAD_QUERY_PATTERN.matcher(q);
                        if (matcher.find()) {
                            final var comp = matcher.group("comp");
                            switch (comp) {
                                case "block":
                                    uploadBlockRequests.record();
                                    break;
                                case "blocklist":
                                    uploadBlockListRequests.record();
                                    break;
                                default:
                            }
                        }
                        break;
                    case DELETE:
                        deleteBlobRequests.record();
                        break;
                    default:
                }
            }
        }
    }
}
