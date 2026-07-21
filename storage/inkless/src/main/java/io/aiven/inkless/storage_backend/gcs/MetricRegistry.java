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

import com.groupcdg.pitest.annotations.CoverageIgnore;

import java.util.List;

@CoverageIgnore  // tested on integration level
public class MetricRegistry {
    static final String METRIC_GROUP = "gcs-client-metrics";
    static final String OBJECT_METADATA_GET = "object-metadata-get";
    static final String OBJECT_METADATA_GET_RATE = OBJECT_METADATA_GET + "-rate";
    static final String OBJECT_METADATA_GET_TOTAL = OBJECT_METADATA_GET + "-total";
    static final String OBJECT_METADATA_GET_DOC = "get object metadata operations";
    static final String OBJECT_GET = "object-get";
    static final String OBJECT_GET_RATE = OBJECT_GET + "-rate";
    static final String OBJECT_GET_TOTAL = OBJECT_GET + "-total";
    static final String OBJECT_GET_DOC = "get object operations";
    static final String OBJECT_DELETE = "object-delete";
    static final String OBJECT_DELETE_RATE = OBJECT_DELETE + "-rate";
    static final String OBJECT_DELETE_TOTAL = OBJECT_DELETE + "-total";
    static final String OBJECT_DELETE_DOC = "delete object operations";
    static final String RESUMABLE_UPLOAD_INITIATE = "resumable-upload-initiate";
    static final String RESUMABLE_UPLOAD_INITIATE_RATE = RESUMABLE_UPLOAD_INITIATE + "-rate";
    static final String RESUMABLE_UPLOAD_INITIATE_TOTAL = RESUMABLE_UPLOAD_INITIATE + "-total";
    static final String RESUMABLE_UPLOAD_INITIATE_DOC = "initiate resumable upload operations";
    static final String RESUMABLE_CHUNK_UPLOAD = "resumable-chunk-upload";
    static final String RESUMABLE_CHUNK_UPLOAD_RATE = RESUMABLE_CHUNK_UPLOAD + "-rate";
    static final String RESUMABLE_CHUNK_UPLOAD_TOTAL = RESUMABLE_CHUNK_UPLOAD + "-total";
    static final String RESUMABLE_CHUNK_UPLOAD_DOC = "upload chunk operations as part of resumable upload";

    private static final String RATE_DOC_PREFIX = "Rate of ";
    private static final String TOTAL_DOC_PREFIX = "Total number of ";

    static final MetricNameTemplate OBJECT_METADATA_GET_RATE_METRIC_NAME = new MetricNameTemplate(
        OBJECT_METADATA_GET_RATE,
        METRIC_GROUP,
        RATE_DOC_PREFIX + OBJECT_METADATA_GET_DOC
    );
    static final MetricNameTemplate OBJECT_METADATA_GET_TOTAL_METRIC_NAME = new MetricNameTemplate(
        OBJECT_METADATA_GET_TOTAL,
        METRIC_GROUP,
        TOTAL_DOC_PREFIX + OBJECT_METADATA_GET_DOC
    );
    static final MetricNameTemplate OBJECT_GET_RATE_METRIC_NAME = new MetricNameTemplate(
        OBJECT_GET_RATE,
        METRIC_GROUP,
        RATE_DOC_PREFIX + OBJECT_GET_DOC
    );
    static final MetricNameTemplate OBJECT_GET_TOTAL_METRIC_NAME = new MetricNameTemplate(
        OBJECT_GET_TOTAL,
        METRIC_GROUP,
        TOTAL_DOC_PREFIX + OBJECT_GET_DOC
    );
    static final MetricNameTemplate OBJECT_DELETE_RATE_METRIC_NAME = new MetricNameTemplate(
        OBJECT_DELETE_RATE,
        METRIC_GROUP,
        RATE_DOC_PREFIX + OBJECT_DELETE_DOC
    );
    static final MetricNameTemplate OBJECT_DELETE_TOTAL_METRIC_NAME = new MetricNameTemplate(
        OBJECT_DELETE_TOTAL,
        METRIC_GROUP,
        TOTAL_DOC_PREFIX + OBJECT_DELETE_DOC
    );
    static final MetricNameTemplate RESUMABLE_UPLOAD_INITIATE_RATE_METRIC_NAME = new MetricNameTemplate(
        RESUMABLE_UPLOAD_INITIATE_RATE,
        METRIC_GROUP,
        RATE_DOC_PREFIX + RESUMABLE_UPLOAD_INITIATE_DOC
    );
    static final MetricNameTemplate RESUMABLE_UPLOAD_INITIATE_TOTAL_METRIC_NAME = new MetricNameTemplate(
        RESUMABLE_UPLOAD_INITIATE_TOTAL,
        METRIC_GROUP,
        TOTAL_DOC_PREFIX + RESUMABLE_UPLOAD_INITIATE_DOC
    );
    static final MetricNameTemplate RESUMABLE_CHUNK_UPLOAD_RATE_METRIC_NAME = new MetricNameTemplate(
        RESUMABLE_CHUNK_UPLOAD_RATE,
        METRIC_GROUP,
        RATE_DOC_PREFIX + RESUMABLE_CHUNK_UPLOAD_DOC
    );
    static final MetricNameTemplate RESUMABLE_CHUNK_UPLOAD_TOTAL_METRIC_NAME = new MetricNameTemplate(
        RESUMABLE_CHUNK_UPLOAD_TOTAL,
        METRIC_GROUP,
        TOTAL_DOC_PREFIX + RESUMABLE_CHUNK_UPLOAD_DOC
    );

    public static List<MetricNameTemplate> all() {
        return List.of(
            OBJECT_METADATA_GET_RATE_METRIC_NAME,
            OBJECT_METADATA_GET_TOTAL_METRIC_NAME,
            OBJECT_GET_RATE_METRIC_NAME,
            OBJECT_GET_TOTAL_METRIC_NAME,
            OBJECT_DELETE_RATE_METRIC_NAME,
            OBJECT_DELETE_TOTAL_METRIC_NAME,
            RESUMABLE_UPLOAD_INITIATE_RATE_METRIC_NAME,
            RESUMABLE_UPLOAD_INITIATE_TOTAL_METRIC_NAME,
            RESUMABLE_CHUNK_UPLOAD_RATE_METRIC_NAME,
            RESUMABLE_CHUNK_UPLOAD_TOTAL_METRIC_NAME
        );
    }
}
