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
package io.aiven.inkless.control_plane;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;

import java.io.Closeable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Set;

import io.aiven.inkless.common.ObjectFormat;
import io.aiven.inkless.config.InklessConfig;

public interface ControlPlane extends Closeable, Configurable {
    List<CommitBatchResponse> commitFile(
            String objectKey,
            ObjectFormat format,
            int uploaderBrokerId,
            long fileSize,
            List<CommitBatchRequest> batches);

    /**
     * Find batches for the given partition requests.
     *
     * @param findBatchRequests the list of partition requests
     * @param fetchMaxBytes maximum bytes to fetch across all partitions
     * @param maxBatchesPerPartition maximum batches per partition to return
     * @return list of responses, one per request, in the same order as the requests.
     *         The contract requires that responses[i] corresponds to findBatchRequests[i] for all i.
     */
    List<FindBatchResponse> findBatches(
        List<FindBatchRequest> findBatchRequests,
        int fetchMaxBytes,
        int maxBatchesPerPartition
    );

    void createTopicAndPartitions(Set<CreateTopicAndPartitionsRequest> requests);

    void initLogDisklessStartOffset(Set<InitLogDisklessStartOffsetRequest> requests);

    List<DeleteRecordsResponse> deleteRecords(List<DeleteRecordsRequest> requests);

    void deleteTopics(Set<Uuid> topicIds);

    List<EnforceRetentionResponse> enforceRetention(List<EnforceRetentionRequest> requests, int maxBatchesPerRequest);

    List<FileToDelete> getFilesToDelete();

    void deleteFiles(DeleteFilesRequest request);

    List<ListOffsetsResponse> listOffsets(List<ListOffsetsRequest> requests);

    FileMergeWorkItem getFileMergeWorkItem();

    void commitFileMergeWorkItem(
            long workItemId,
            String objectKey,
            ObjectFormat format,
            int uploaderBrokerId,
            long fileSize,
            List<MergedFileBatch> batches);

    void releaseFileMergeWorkItem(long workItemId);

    static ControlPlane create(final InklessConfig config, final Time time) {
        final Class<ControlPlane> controlPlaneClass = config.controlPlaneClass();
        try {
            final Constructor<ControlPlane> ctor = controlPlaneClass.getConstructor(Time.class);
            final ControlPlane result = ctor.newInstance(time);
            result.configure(config.controlPlaneConfig());
            return result;
        } catch (final NoSuchMethodException | InstantiationException | IllegalAccessException |
                       InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    boolean isSafeToDeleteFile(String objectKeyPath);

    List<GetDisklessLogResponse> getDisklessLog(List<GetDisklessLogRequest> requests);

    // used for testing purposes only
    List<GetLogInfoResponse> getLogInfo(List<GetLogInfoRequest> requests);
}
