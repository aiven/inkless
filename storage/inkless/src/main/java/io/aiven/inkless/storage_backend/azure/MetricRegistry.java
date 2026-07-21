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

import com.groupcdg.pitest.annotations.CoverageIgnore;

import java.util.List;

@CoverageIgnore // tested on integration level
public class MetricRegistry {
    static final String METRIC_GROUP = "azure-blob-storage-client-metrics";
    static final String BLOB_DELETE = "blob-delete";
    static final String BLOB_DELETE_DOC = "object delete operations";
    static final String BLOB_DELETE_RATE = BLOB_DELETE + "-rate";
    static final String BLOB_DELETE_TOTAL = BLOB_DELETE + "-total";
    static final String BLOB_UPLOAD = "blob-upload";
    static final String BLOB_UPLOAD_DOC = "object upload operations";
    static final String BLOB_UPLOAD_RATE = BLOB_UPLOAD + "-rate";
    static final String BLOB_UPLOAD_TOTAL = BLOB_UPLOAD + "-total";
    static final String BLOCK_UPLOAD = "block-upload";
    static final String BLOCK_UPLOAD_RATE = BLOCK_UPLOAD + "-rate";
    static final String BLOCK_UPLOAD_TOTAL = BLOCK_UPLOAD + "-total";
    static final String BLOCK_UPLOAD_DOC = "block (blob part) upload operations";
    static final String BLOCK_LIST_UPLOAD = "block-list-upload";
    static final String BLOCK_LIST_UPLOAD_RATE = BLOCK_LIST_UPLOAD + "-rate";
    static final String BLOCK_LIST_UPLOAD_TOTAL = BLOCK_LIST_UPLOAD + "-total";
    static final String BLOCK_LIST_UPLOAD_DOC = "block list (making a blob) upload operations";
    static final String BLOB_GET = "blob-get";
    static final String BLOB_GET_RATE = BLOB_GET + "-rate";
    static final String BLOB_GET_TOTAL = BLOB_GET + "-total";
    static final String BLOB_GET_DOC = "get object operations";

    private static final String RATE_DOC_PREFIX = "Rate of ";
    private static final String TOTAL_DOC_PREFIX = "Total number of ";

    static final MetricNameTemplate BLOB_DELETE_RATE_METRIC_NAME = new MetricNameTemplate(
        BLOB_DELETE_RATE,
        METRIC_GROUP,
        RATE_DOC_PREFIX + BLOB_DELETE_DOC
    );
    static final MetricNameTemplate BLOB_DELETE_TOTAL_METRIC_NAME = new MetricNameTemplate(
        BLOB_DELETE_TOTAL,
        METRIC_GROUP,
        TOTAL_DOC_PREFIX + BLOB_DELETE_DOC
    );
    static final MetricNameTemplate BLOB_UPLOAD_RATE_METRIC_NAME = new MetricNameTemplate(
        BLOB_UPLOAD_RATE,
        METRIC_GROUP,
        RATE_DOC_PREFIX + BLOB_UPLOAD_DOC
    );
    static final MetricNameTemplate BLOB_UPLOAD_TOTAL_METRIC_NAME = new MetricNameTemplate(
        BLOB_UPLOAD_TOTAL,
        METRIC_GROUP,
        TOTAL_DOC_PREFIX + BLOB_UPLOAD_DOC
    );
    static final MetricNameTemplate BLOCK_UPLOAD_RATE_METRIC_NAME = new MetricNameTemplate(
        BLOCK_UPLOAD_RATE,
        METRIC_GROUP,
        RATE_DOC_PREFIX + BLOCK_UPLOAD_DOC
    );
    static final MetricNameTemplate BLOCK_UPLOAD_TOTAL_METRIC_NAME = new MetricNameTemplate(
        BLOCK_UPLOAD_TOTAL,
        METRIC_GROUP,
        TOTAL_DOC_PREFIX + BLOCK_UPLOAD_DOC
    );
    static final MetricNameTemplate BLOCK_LIST_UPLOAD_RATE_METRIC_NAME = new MetricNameTemplate(
        BLOCK_LIST_UPLOAD_RATE,
        METRIC_GROUP,
        RATE_DOC_PREFIX + BLOCK_LIST_UPLOAD_DOC
    );
    static final MetricNameTemplate BLOCK_LIST_UPLOAD_TOTAL_METRIC_NAME = new MetricNameTemplate(
        BLOCK_LIST_UPLOAD_TOTAL,
        METRIC_GROUP,
        TOTAL_DOC_PREFIX + BLOCK_LIST_UPLOAD_DOC
    );
    static final MetricNameTemplate BLOB_GET_RATE_METRIC_NAME = new MetricNameTemplate(
        BLOB_GET_RATE,
        METRIC_GROUP,
        RATE_DOC_PREFIX + BLOB_GET_DOC
    );
    static final MetricNameTemplate BLOB_GET_TOTAL_METRIC_NAME = new MetricNameTemplate(
        BLOB_GET_TOTAL,
        METRIC_GROUP,
        TOTAL_DOC_PREFIX + BLOB_GET_DOC
    );

    public static List<MetricNameTemplate> all() {
        return List.of(
            BLOB_DELETE_RATE_METRIC_NAME,
            BLOB_DELETE_TOTAL_METRIC_NAME,
            BLOB_UPLOAD_RATE_METRIC_NAME,
            BLOB_UPLOAD_TOTAL_METRIC_NAME,
            BLOCK_UPLOAD_RATE_METRIC_NAME,
            BLOCK_UPLOAD_TOTAL_METRIC_NAME,
            BLOCK_LIST_UPLOAD_RATE_METRIC_NAME,
            BLOCK_LIST_UPLOAD_TOTAL_METRIC_NAME,
            BLOB_GET_RATE_METRIC_NAME,
            BLOB_GET_TOTAL_METRIC_NAME
        );
    }
}
