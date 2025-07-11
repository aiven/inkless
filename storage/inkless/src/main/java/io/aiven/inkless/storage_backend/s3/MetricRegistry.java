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
package io.aiven.inkless.storage_backend.s3;

import org.apache.kafka.common.MetricNameTemplate;

import com.groupcdg.pitest.annotations.CoverageIgnore;

import java.util.List;

@CoverageIgnore  // tested on integration level
public class MetricRegistry {
    public static final String METRIC_CONTEXT = "aiven.inkless.server.s3";

    static final String METRIC_GROUP = "s3-client-metrics";
    static final String GET_OBJECT_REQUESTS = "get-object-requests";
    static final String GET_OBJECT_REQUESTS_RATE = GET_OBJECT_REQUESTS + "-rate";
    static final String GET_OBJECT_REQUESTS_TOTAL = GET_OBJECT_REQUESTS + "-total";
    static final String GET_OBJECT_REQUESTS_DOC = "get object request operations";
    static final String GET_OBJECT_TIME = "get-object-time";
    static final String GET_OBJECT_TIME_AVG = GET_OBJECT_TIME + "-avg";
    static final String GET_OBJECT_TIME_MAX = GET_OBJECT_TIME + "-max";
    static final String GET_OBJECT_TIME_DOC = "time spent getting a response from a get object request";
    static final String PUT_OBJECT_REQUESTS = "put-object-requests";
    static final String PUT_OBJECT_REQUESTS_RATE = PUT_OBJECT_REQUESTS + "-rate";
    static final String PUT_OBJECT_REQUESTS_TOTAL = PUT_OBJECT_REQUESTS + "-total";
    static final String PUT_OBJECT_REQUESTS_DOC = "put object request operations";
    static final String PUT_OBJECT_TIME = "put-object-time";
    static final String PUT_OBJECT_TIME_AVG = PUT_OBJECT_TIME + "-avg";
    static final String PUT_OBJECT_TIME_MAX = PUT_OBJECT_TIME + "-max";
    static final String PUT_OBJECT_TIME_DOC = "time spent uploading an object";
    static final String DELETE_OBJECT_REQUESTS = "delete-object-requests";
    static final String DELETE_OBJECT_REQUESTS_RATE = DELETE_OBJECT_REQUESTS + "-rate";
    static final String DELETE_OBJECT_REQUESTS_TOTAL = DELETE_OBJECT_REQUESTS + "-total";
    static final String DELETE_OBJECT_REQUESTS_DOC = "delete object request operations";
    static final String DELETE_OBJECT_TIME = "delete-object-time";
    static final String DELETE_OBJECT_TIME_AVG = DELETE_OBJECT_TIME + "-avg";
    static final String DELETE_OBJECT_TIME_MAX = DELETE_OBJECT_TIME + "-max";
    static final String DELETE_OBJECT_TIME_DOC = "time spent deleting an object";
    static final String DELETE_OBJECTS_REQUESTS = "delete-objects-requests";
    static final String DELETE_OBJECTS_REQUESTS_RATE = DELETE_OBJECTS_REQUESTS + "-rate";
    static final String DELETE_OBJECTS_REQUESTS_TOTAL = DELETE_OBJECTS_REQUESTS + "-total";
    static final String DELETE_OBJECTS_REQUESTS_DOC = "delete a set of objects request operations";
    static final String DELETE_OBJECTS_TIME = "delete-objects-time";
    static final String DELETE_OBJECTS_TIME_AVG = DELETE_OBJECTS_TIME + "-avg";
    static final String DELETE_OBJECTS_TIME_MAX = DELETE_OBJECTS_TIME + "-max";
    static final String DELETE_OBJECTS_TIME_DOC = "time spent deleting a set of objects";
    static final String UPLOAD_PART_REQUESTS = "upload-part-requests";
    static final String UPLOAD_PART_REQUESTS_RATE = UPLOAD_PART_REQUESTS + "-rate";
    static final String UPLOAD_PART_REQUESTS_TOTAL = UPLOAD_PART_REQUESTS + "-total";
    static final String UPLOAD_PART_REQUESTS_DOC = "upload part request operations "
        + "(as part of multi-part upload)";
    static final String UPLOAD_PART_TIME = "upload-part-time";
    static final String UPLOAD_PART_TIME_AVG = UPLOAD_PART_TIME + "-avg";
    static final String UPLOAD_PART_TIME_MAX = UPLOAD_PART_TIME + "-max";
    static final String UPLOAD_PART_TIME_DOC = "time spent uploading a single part";
    static final String CREATE_MULTIPART_UPLOAD_REQUESTS = "create-multipart-upload-requests";
    static final String CREATE_MULTIPART_UPLOAD_REQUESTS_RATE = CREATE_MULTIPART_UPLOAD_REQUESTS + "-rate";
    static final String CREATE_MULTIPART_UPLOAD_REQUESTS_TOTAL = CREATE_MULTIPART_UPLOAD_REQUESTS + "-total";
    static final String CREATE_MULTIPART_UPLOAD_REQUESTS_DOC = "create multi-part upload operations";
    static final String CREATE_MULTIPART_UPLOAD_TIME = "create-multipart-upload-time";
    static final String CREATE_MULTIPART_UPLOAD_TIME_AVG = CREATE_MULTIPART_UPLOAD_TIME + "-avg";
    static final String CREATE_MULTIPART_UPLOAD_TIME_MAX = CREATE_MULTIPART_UPLOAD_TIME + "-max";
    static final String CREATE_MULTIPART_UPLOAD_TIME_DOC = "time spent creating a new multi-part upload operation";
    static final String COMPLETE_MULTIPART_UPLOAD_REQUESTS = "complete-multipart-upload-requests";
    static final String COMPLETE_MULTIPART_UPLOAD_REQUESTS_RATE = COMPLETE_MULTIPART_UPLOAD_REQUESTS + "-rate";
    static final String COMPLETE_MULTIPART_UPLOAD_REQUESTS_TOTAL = COMPLETE_MULTIPART_UPLOAD_REQUESTS + "-total";
    static final String COMPLETE_MULTIPART_UPLOAD_REQUESTS_DOC = "complete multi-part upload operations";
    static final String COMPLETE_MULTIPART_UPLOAD_TIME = "complete-multipart-upload-time";
    static final String COMPLETE_MULTIPART_UPLOAD_TIME_AVG = COMPLETE_MULTIPART_UPLOAD_TIME + "-avg";
    static final String COMPLETE_MULTIPART_UPLOAD_TIME_MAX = COMPLETE_MULTIPART_UPLOAD_TIME + "-max";
    static final String COMPLETE_MULTIPART_UPLOAD_TIME_DOC = "time spent completing a new multi-part upload operation";
    static final String ABORT_MULTIPART_UPLOAD_REQUESTS = "abort-multipart-upload-requests";
    static final String ABORT_MULTIPART_UPLOAD_REQUESTS_RATE = ABORT_MULTIPART_UPLOAD_REQUESTS + "-rate";
    static final String ABORT_MULTIPART_UPLOAD_REQUESTS_TOTAL = ABORT_MULTIPART_UPLOAD_REQUESTS + "-total";
    static final String ABORT_MULTIPART_UPLOAD_REQUESTS_DOC = "abort multi-part upload operations";
    static final String ABORT_MULTIPART_UPLOAD_TIME = "abort-multipart-upload-time";
    static final String ABORT_MULTIPART_UPLOAD_TIME_AVG = ABORT_MULTIPART_UPLOAD_TIME + "-avg";
    static final String ABORT_MULTIPART_UPLOAD_TIME_MAX = ABORT_MULTIPART_UPLOAD_TIME + "-max";
    static final String ABORT_MULTIPART_UPLOAD_TIME_DOC = "time spent aborting a new multi-part upload operation";

    static final String THROTTLING_ERRORS = "throttling-errors";
    static final String THROTTLING_ERRORS_RATE = THROTTLING_ERRORS + "-rate";
    static final String THROTTLING_ERRORS_TOTAL = THROTTLING_ERRORS + "-total";
    static final String THROTTLING_ERRORS_DOC = "throttling errors";
    static final String SERVER_ERRORS = "server-errors";
    static final String SERVER_ERRORS_RATE = SERVER_ERRORS + "-rate";
    static final String SERVER_ERRORS_TOTAL = SERVER_ERRORS + "-total";
    static final String SERVER_ERRORS_DOC = "server errors";
    static final String CONFIGURED_TIMEOUT_ERRORS = "configured-timeout-errors";
    static final String CONFIGURED_TIMEOUT_ERRORS_RATE = CONFIGURED_TIMEOUT_ERRORS + "-rate";
    static final String CONFIGURED_TIMEOUT_ERRORS_TOTAL = CONFIGURED_TIMEOUT_ERRORS + "-total";
    static final String CONFIGURED_TIMEOUT_ERRORS_DOC = "configured timeout errors";
    static final String IO_ERRORS = "io-errors";
    static final String IO_ERRORS_RATE = IO_ERRORS + "-rate";
    static final String IO_ERRORS_TOTAL = IO_ERRORS + "-total";
    static final String IO_ERRORS_DOC = "IO errors";
    static final String OTHER_ERRORS = "other-errors";
    static final String OTHER_ERRORS_RATE = OTHER_ERRORS + "-rate";
    static final String OTHER_ERRORS_TOTAL = OTHER_ERRORS + "-total";
    static final String OTHER_ERRORS_DOC = "other errors";

    private static final String RATE_DOC_PREFIX = "Rate of ";
    private static final String TOTAL_DOC_PREFIX = "Total number of ";
    private static final String AVG_DOC_PREFIX = "Average ";
    private static final String MAX_DOC_PREFIX = "Maximum ";

    static final MetricNameTemplate GET_OBJECT_REQUESTS_RATE_METRIC_NAME = new MetricNameTemplate(
        GET_OBJECT_REQUESTS_RATE,
        METRIC_GROUP,
        RATE_DOC_PREFIX + GET_OBJECT_REQUESTS_DOC
    );
    static final MetricNameTemplate GET_OBJECT_REQUESTS_TOTAL_METRIC_NAME = new MetricNameTemplate(
        GET_OBJECT_REQUESTS_TOTAL,
        METRIC_GROUP,
        TOTAL_DOC_PREFIX + GET_OBJECT_REQUESTS_DOC
    );
    static final MetricNameTemplate GET_OBJECT_TIME_AVG_METRIC_NAME = new MetricNameTemplate(
        GET_OBJECT_TIME_AVG,
        METRIC_GROUP,
        AVG_DOC_PREFIX + GET_OBJECT_TIME_DOC
    );
    static final MetricNameTemplate GET_OBJECT_TIME_MAX_METRIC_NAME = new MetricNameTemplate(
        GET_OBJECT_TIME_MAX,
        METRIC_GROUP,
        MAX_DOC_PREFIX + GET_OBJECT_TIME_DOC
    );
    static final MetricNameTemplate PUT_OBJECT_REQUESTS_RATE_METRIC_NAME = new MetricNameTemplate(
        PUT_OBJECT_REQUESTS_RATE,
        METRIC_GROUP,
        RATE_DOC_PREFIX + PUT_OBJECT_REQUESTS_DOC
    );
    static final MetricNameTemplate PUT_OBJECT_REQUESTS_TOTAL_METRIC_NAME = new MetricNameTemplate(
        PUT_OBJECT_REQUESTS_TOTAL,
        METRIC_GROUP,
        TOTAL_DOC_PREFIX + PUT_OBJECT_REQUESTS_DOC
    );
    static final MetricNameTemplate PUT_OBJECT_TIME_AVG_METRIC_NAME = new MetricNameTemplate(
        PUT_OBJECT_TIME_AVG,
        METRIC_GROUP,
        AVG_DOC_PREFIX + PUT_OBJECT_TIME_DOC
    );
    static final MetricNameTemplate PUT_OBJECT_TIME_MAX_METRIC_NAME = new MetricNameTemplate(
        PUT_OBJECT_TIME_MAX,
        METRIC_GROUP,
        MAX_DOC_PREFIX + PUT_OBJECT_TIME_DOC
    );
    static final MetricNameTemplate DELETE_OBJECT_REQUESTS_RATE_METRIC_NAME = new MetricNameTemplate(
        DELETE_OBJECT_REQUESTS_RATE,
        METRIC_GROUP,
        RATE_DOC_PREFIX + DELETE_OBJECT_REQUESTS_DOC
    );
    static final MetricNameTemplate DELETE_OBJECT_REQUESTS_TOTAL_METRIC_NAME = new MetricNameTemplate(
        DELETE_OBJECT_REQUESTS_TOTAL,
        METRIC_GROUP,
        TOTAL_DOC_PREFIX + DELETE_OBJECT_REQUESTS_DOC
    );
    static final MetricNameTemplate DELETE_OBJECT_TIME_AVG_METRIC_NAME = new MetricNameTemplate(
        DELETE_OBJECT_TIME_AVG,
        METRIC_GROUP,
        AVG_DOC_PREFIX + DELETE_OBJECT_TIME_DOC
    );
    static final MetricNameTemplate DELETE_OBJECT_TIME_MAX_METRIC_NAME = new MetricNameTemplate(
        DELETE_OBJECT_TIME_MAX,
        METRIC_GROUP,
        MAX_DOC_PREFIX + DELETE_OBJECT_TIME_DOC
    );
    static final MetricNameTemplate DELETE_OBJECTS_REQUESTS_RATE_METRIC_NAME = new MetricNameTemplate(
        DELETE_OBJECTS_REQUESTS_RATE,
        METRIC_GROUP,
        RATE_DOC_PREFIX + DELETE_OBJECTS_REQUESTS_DOC
    );
    static final MetricNameTemplate DELETE_OBJECTS_REQUESTS_TOTAL_METRIC_NAME = new MetricNameTemplate(
        DELETE_OBJECTS_REQUESTS_TOTAL,
        METRIC_GROUP,
        TOTAL_DOC_PREFIX + DELETE_OBJECTS_REQUESTS_DOC
    );
    static final MetricNameTemplate DELETE_OBJECTS_TIME_AVG_METRIC_NAME = new MetricNameTemplate(
        DELETE_OBJECTS_TIME_AVG,
        METRIC_GROUP,
        AVG_DOC_PREFIX + DELETE_OBJECTS_TIME_DOC
    );
    static final MetricNameTemplate DELETE_OBJECTS_TIME_MAX_METRIC_NAME = new MetricNameTemplate(
        DELETE_OBJECTS_TIME_MAX,
        METRIC_GROUP,
        MAX_DOC_PREFIX + DELETE_OBJECTS_TIME_DOC
    );
    static final MetricNameTemplate UPLOAD_PART_REQUESTS_RATE_METRIC_NAME = new MetricNameTemplate(
        UPLOAD_PART_REQUESTS_RATE,
        METRIC_GROUP,
        RATE_DOC_PREFIX + UPLOAD_PART_REQUESTS_DOC
    );
    static final MetricNameTemplate UPLOAD_PART_REQUESTS_TOTAL_METRIC_NAME = new MetricNameTemplate(
        UPLOAD_PART_REQUESTS_TOTAL,
        METRIC_GROUP,
        TOTAL_DOC_PREFIX + UPLOAD_PART_REQUESTS_DOC
    );
    static final MetricNameTemplate UPLOAD_PART_TIME_AVG_METRIC_NAME = new MetricNameTemplate(
        UPLOAD_PART_TIME_AVG,
        METRIC_GROUP,
        AVG_DOC_PREFIX + UPLOAD_PART_TIME_DOC
    );
    static final MetricNameTemplate UPLOAD_PART_TIME_MAX_METRIC_NAME = new MetricNameTemplate(
        UPLOAD_PART_TIME_MAX,
        METRIC_GROUP,
        MAX_DOC_PREFIX + UPLOAD_PART_TIME_DOC
    );
    static final MetricNameTemplate CREATE_MULTIPART_UPLOAD_REQUESTS_RATE_METRIC_NAME = new MetricNameTemplate(
        CREATE_MULTIPART_UPLOAD_REQUESTS_RATE,
        METRIC_GROUP,
        RATE_DOC_PREFIX + CREATE_MULTIPART_UPLOAD_REQUESTS_DOC
    );
    static final MetricNameTemplate CREATE_MULTIPART_UPLOAD_REQUESTS_TOTAL_METRIC_NAME = new MetricNameTemplate(
        CREATE_MULTIPART_UPLOAD_REQUESTS_TOTAL,
        METRIC_GROUP,
        TOTAL_DOC_PREFIX + CREATE_MULTIPART_UPLOAD_REQUESTS_DOC
    );
    static final MetricNameTemplate CREATE_MULTIPART_UPLOAD_TIME_AVG_METRIC_NAME = new MetricNameTemplate(
        CREATE_MULTIPART_UPLOAD_TIME_AVG,
        METRIC_GROUP,
        AVG_DOC_PREFIX + CREATE_MULTIPART_UPLOAD_TIME_DOC
    );
    static final MetricNameTemplate CREATE_MULTIPART_UPLOAD_TIME_MAX_METRIC_NAME = new MetricNameTemplate(
        CREATE_MULTIPART_UPLOAD_TIME_MAX,
        METRIC_GROUP,
        MAX_DOC_PREFIX + CREATE_MULTIPART_UPLOAD_TIME_DOC
    );
    static final MetricNameTemplate COMPLETE_MULTIPART_UPLOAD_REQUESTS_RATE_METRIC_NAME = new MetricNameTemplate(
        COMPLETE_MULTIPART_UPLOAD_REQUESTS_RATE,
        METRIC_GROUP,
        RATE_DOC_PREFIX + COMPLETE_MULTIPART_UPLOAD_REQUESTS_DOC
    );
    static final MetricNameTemplate COMPLETE_MULTIPART_UPLOAD_REQUESTS_TOTAL_METRIC_NAME = new MetricNameTemplate(
        COMPLETE_MULTIPART_UPLOAD_REQUESTS_TOTAL,
        METRIC_GROUP,
        TOTAL_DOC_PREFIX + COMPLETE_MULTIPART_UPLOAD_REQUESTS_DOC
    );
    static final MetricNameTemplate COMPLETE_MULTIPART_UPLOAD_TIME_AVG_METRIC_NAME = new MetricNameTemplate(
        COMPLETE_MULTIPART_UPLOAD_TIME_AVG,
        METRIC_GROUP,
        AVG_DOC_PREFIX + COMPLETE_MULTIPART_UPLOAD_TIME_DOC
    );
    static final MetricNameTemplate COMPLETE_MULTIPART_UPLOAD_TIME_MAX_METRIC_NAME = new MetricNameTemplate(
        COMPLETE_MULTIPART_UPLOAD_TIME_MAX,
        METRIC_GROUP,
        MAX_DOC_PREFIX + COMPLETE_MULTIPART_UPLOAD_TIME_DOC
    );
    static final MetricNameTemplate ABORT_MULTIPART_UPLOAD_REQUESTS_RATE_METRIC_NAME = new MetricNameTemplate(
        ABORT_MULTIPART_UPLOAD_REQUESTS_RATE,
        METRIC_GROUP,
        RATE_DOC_PREFIX + ABORT_MULTIPART_UPLOAD_REQUESTS_DOC
    );
    static final MetricNameTemplate ABORT_MULTIPART_UPLOAD_REQUESTS_TOTAL_METRIC_NAME = new MetricNameTemplate(
        ABORT_MULTIPART_UPLOAD_REQUESTS_TOTAL,
        METRIC_GROUP,
        TOTAL_DOC_PREFIX + ABORT_MULTIPART_UPLOAD_REQUESTS_DOC
    );
    static final MetricNameTemplate ABORT_MULTIPART_UPLOAD_TIME_AVG_METRIC_NAME = new MetricNameTemplate(
        ABORT_MULTIPART_UPLOAD_TIME_AVG,
        METRIC_GROUP,
        AVG_DOC_PREFIX + ABORT_MULTIPART_UPLOAD_TIME_DOC
    );
    static final MetricNameTemplate ABORT_MULTIPART_UPLOAD_TIME_MAX_METRIC_NAME = new MetricNameTemplate(
        ABORT_MULTIPART_UPLOAD_TIME_MAX,
        METRIC_GROUP,
        MAX_DOC_PREFIX + ABORT_MULTIPART_UPLOAD_TIME_DOC
    );

    static final MetricNameTemplate THROTTLING_ERRORS_RATE_METRIC_NAME = new MetricNameTemplate(
        THROTTLING_ERRORS_RATE,
        METRIC_GROUP,
        RATE_DOC_PREFIX + THROTTLING_ERRORS_DOC
    );
    static final MetricNameTemplate THROTTLING_ERRORS_TOTAL_METRIC_NAME = new MetricNameTemplate(
        THROTTLING_ERRORS_TOTAL,
        METRIC_GROUP,
        TOTAL_DOC_PREFIX + THROTTLING_ERRORS_DOC
    );
    static final MetricNameTemplate SERVER_ERRORS_RATE_METRIC_NAME = new MetricNameTemplate(
        SERVER_ERRORS_RATE,
        METRIC_GROUP,
        RATE_DOC_PREFIX + SERVER_ERRORS_DOC
    );
    static final MetricNameTemplate SERVER_ERRORS_TOTAL_METRIC_NAME = new MetricNameTemplate(
        SERVER_ERRORS_TOTAL,
        METRIC_GROUP,
        TOTAL_DOC_PREFIX + SERVER_ERRORS_DOC
    );
    static final MetricNameTemplate CONFIGURED_TIMEOUT_ERRORS_RATE_METRIC_NAME = new MetricNameTemplate(
        CONFIGURED_TIMEOUT_ERRORS_RATE,
        METRIC_GROUP,
        RATE_DOC_PREFIX + CONFIGURED_TIMEOUT_ERRORS_DOC
    );
    static final MetricNameTemplate CONFIGURED_TIMEOUT_ERRORS_TOTAL_METRIC_NAME = new MetricNameTemplate(
        CONFIGURED_TIMEOUT_ERRORS_TOTAL,
        METRIC_GROUP,
        TOTAL_DOC_PREFIX + CONFIGURED_TIMEOUT_ERRORS_DOC
    );
    static final MetricNameTemplate IO_ERRORS_RATE_METRIC_NAME = new MetricNameTemplate(
        IO_ERRORS_RATE,
        METRIC_GROUP,
        RATE_DOC_PREFIX + IO_ERRORS_DOC
    );
    static final MetricNameTemplate IO_ERRORS_TOTAL_METRIC_NAME = new MetricNameTemplate(
        IO_ERRORS_TOTAL,
        METRIC_GROUP,
        TOTAL_DOC_PREFIX + IO_ERRORS_DOC
    );
    static final MetricNameTemplate OTHER_ERRORS_RATE_METRIC_NAME = new MetricNameTemplate(
        OTHER_ERRORS_RATE,
        METRIC_GROUP,
        RATE_DOC_PREFIX + OTHER_ERRORS_DOC
    );
    static final MetricNameTemplate OTHER_ERRORS_TOTAL_METRIC_NAME = new MetricNameTemplate(
        OTHER_ERRORS_TOTAL,
        METRIC_GROUP,
        TOTAL_DOC_PREFIX + OTHER_ERRORS_DOC
    );

    public static List<MetricNameTemplate> all() {
        return List.of(
            GET_OBJECT_REQUESTS_RATE_METRIC_NAME,
            GET_OBJECT_REQUESTS_TOTAL_METRIC_NAME,
            GET_OBJECT_TIME_AVG_METRIC_NAME,
            GET_OBJECT_TIME_MAX_METRIC_NAME,
            PUT_OBJECT_REQUESTS_RATE_METRIC_NAME,
            PUT_OBJECT_REQUESTS_TOTAL_METRIC_NAME,
            PUT_OBJECT_TIME_AVG_METRIC_NAME,
            PUT_OBJECT_TIME_MAX_METRIC_NAME,
            DELETE_OBJECT_REQUESTS_RATE_METRIC_NAME,
            DELETE_OBJECT_REQUESTS_TOTAL_METRIC_NAME,
            DELETE_OBJECT_TIME_AVG_METRIC_NAME,
            DELETE_OBJECT_TIME_MAX_METRIC_NAME,
            DELETE_OBJECTS_REQUESTS_RATE_METRIC_NAME,
            DELETE_OBJECTS_REQUESTS_TOTAL_METRIC_NAME,
            DELETE_OBJECTS_TIME_AVG_METRIC_NAME,
            DELETE_OBJECTS_TIME_MAX_METRIC_NAME,
            UPLOAD_PART_REQUESTS_RATE_METRIC_NAME,
            UPLOAD_PART_REQUESTS_TOTAL_METRIC_NAME,
            UPLOAD_PART_TIME_AVG_METRIC_NAME,
            UPLOAD_PART_TIME_MAX_METRIC_NAME,
            CREATE_MULTIPART_UPLOAD_REQUESTS_RATE_METRIC_NAME,
            CREATE_MULTIPART_UPLOAD_REQUESTS_TOTAL_METRIC_NAME,
            CREATE_MULTIPART_UPLOAD_TIME_AVG_METRIC_NAME,
            CREATE_MULTIPART_UPLOAD_TIME_MAX_METRIC_NAME,
            COMPLETE_MULTIPART_UPLOAD_REQUESTS_RATE_METRIC_NAME,
            COMPLETE_MULTIPART_UPLOAD_REQUESTS_TOTAL_METRIC_NAME,
            COMPLETE_MULTIPART_UPLOAD_TIME_AVG_METRIC_NAME,
            COMPLETE_MULTIPART_UPLOAD_TIME_MAX_METRIC_NAME,
            ABORT_MULTIPART_UPLOAD_REQUESTS_RATE_METRIC_NAME,
            ABORT_MULTIPART_UPLOAD_REQUESTS_TOTAL_METRIC_NAME,
            ABORT_MULTIPART_UPLOAD_TIME_AVG_METRIC_NAME,
            ABORT_MULTIPART_UPLOAD_TIME_MAX_METRIC_NAME,
            THROTTLING_ERRORS_RATE_METRIC_NAME,
            THROTTLING_ERRORS_TOTAL_METRIC_NAME,
            SERVER_ERRORS_RATE_METRIC_NAME,
            SERVER_ERRORS_TOTAL_METRIC_NAME,
            CONFIGURED_TIMEOUT_ERRORS_RATE_METRIC_NAME,
            CONFIGURED_TIMEOUT_ERRORS_TOTAL_METRIC_NAME,
            IO_ERRORS_RATE_METRIC_NAME,
            IO_ERRORS_TOTAL_METRIC_NAME,
            OTHER_ERRORS_RATE_METRIC_NAME,
            OTHER_ERRORS_TOTAL_METRIC_NAME
        );
    }
}
