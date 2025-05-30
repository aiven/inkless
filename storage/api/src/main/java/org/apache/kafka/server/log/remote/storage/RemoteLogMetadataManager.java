/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.server.log.remote.storage;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.TopicIdPartition;

import java.io.Closeable;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * This interface provides storing and fetching remote log segment metadata with strongly consistent semantics.
 * <p>
 * This class can be plugged in to Kafka cluster by adding the implementation class as
 * <code>remote.log.metadata.manager.class.name</code> property value. There is an inbuilt implementation backed by
 * topic storage in the local cluster. This is used as the default implementation if
 * remote.log.metadata.manager.class.name is not configured.
 * </p>
 * <p>
 * <code>remote.log.metadata.manager.class.path</code> property is about the class path of the RemoteLogMetadataManager
 * implementation. If specified, the RemoteLogMetadataManager implementation and its dependent libraries will be loaded
 * by a dedicated classloader which searches this class path before the Kafka broker class path. The syntax of this
 * parameter is same with the standard Java class path string.
 * </p>
 * <p>
 * <code>remote.log.metadata.manager.listener.name</code> property is about listener name of the local broker to which
 * it should get connected if needed by RemoteLogMetadataManager implementation.
 * </p>
 * "cluster.id", "broker.id" and all other properties prefixed with the config: "remote.log.metadata.manager.impl.prefix"
 * (default value is "rlmm.config.") are passed when {@link #configure(Map)} is invoked on this instance.
 * <p>
 */
public interface RemoteLogMetadataManager extends Configurable, Closeable {

    /**
     * This method is used to add {@link RemoteLogSegmentMetadata} asynchronously with the containing {@link RemoteLogSegmentId} into {@link RemoteLogMetadataManager}.
     * <p>
     * RemoteLogSegmentMetadata is identified by RemoteLogSegmentId and it should have the initial state which is {@link RemoteLogSegmentState#COPY_SEGMENT_STARTED}.
     * <p>
     * {@link #updateRemoteLogSegmentMetadata(RemoteLogSegmentMetadataUpdate)} should be used to update an existing RemoteLogSegmentMetadata.
     *
     * @param remoteLogSegmentMetadata metadata about the remote log segment.
     * @throws RemoteStorageException   if there are any storage related errors occurred.
     * @throws IllegalArgumentException if the given metadata instance does not have the state as {@link RemoteLogSegmentState#COPY_SEGMENT_STARTED}
     * @return a CompletableFuture which will complete once this operation is finished.
     */
    CompletableFuture<Void> addRemoteLogSegmentMetadata(RemoteLogSegmentMetadata remoteLogSegmentMetadata) throws RemoteStorageException;

    /**
     * This method is used to update the {@link RemoteLogSegmentMetadata} asynchronously. Currently, it allows to update with the new
     * state based on the life cycle of the segment. It can go through the below state transitions.
     * <p>
     * <pre>
     * +---------------------+            +----------------------+
     * |COPY_SEGMENT_STARTED |-----------&gt;|COPY_SEGMENT_FINISHED |
     * +-------------------+-+            +--+-------------------+
     *                     |                 |
     *                     |                 |
     *                     v                 v
     *                  +--+-----------------+-+
     *                  |DELETE_SEGMENT_STARTED|
     *                  +-----------+----------+
     *                              |
     *                              |
     *                              v
     *                  +-----------+-----------+
     *                  |DELETE_SEGMENT_FINISHED|
     *                  +-----------------------+
     * </pre>
     * <p>
     * {@link RemoteLogSegmentState#COPY_SEGMENT_STARTED} - This state indicates that the segment copying to remote storage is started but not yet finished.
     * {@link RemoteLogSegmentState#COPY_SEGMENT_FINISHED} - This state indicates that the segment copying to remote storage is finished.
     * <br>
     * The leader broker copies the log segments to the remote storage and puts the remote log segment metadata with the
     * state as “COPY_SEGMENT_STARTED” and updates the state as “COPY_SEGMENT_FINISHED” once the copy is successful.
     * <p></p>
     * {@link RemoteLogSegmentState#DELETE_SEGMENT_STARTED} - This state indicates that the segment deletion is started but not yet finished.
     * {@link RemoteLogSegmentState#DELETE_SEGMENT_FINISHED} - This state indicates that the segment is deleted successfully.
     * <br>
     * Leader partitions publish both the above delete segment events when remote log retention is reached for the
     * respective segments. Remote Partition Removers also publish these events when a segment is deleted as part of
     * the remote partition deletion.
     *
     * @param remoteLogSegmentMetadataUpdate update of the remote log segment metadata.
     * @throws RemoteStorageException          if there are any storage related errors occurred.
     * @throws RemoteResourceNotFoundException when there are no resources associated with the given remoteLogSegmentMetadataUpdate.
     * @throws IllegalArgumentException        if the given metadata instance has the state as {@link RemoteLogSegmentState#COPY_SEGMENT_STARTED}
     * @return a CompletableFuture which will complete once this operation is finished.
     */
    CompletableFuture<Void> updateRemoteLogSegmentMetadata(RemoteLogSegmentMetadataUpdate remoteLogSegmentMetadataUpdate)
            throws RemoteStorageException;

    /**
     * Returns {@link RemoteLogSegmentMetadata} if it exists for the given topic partition containing the offset with
     * the given leader-epoch for the offset, else returns {@link Optional#empty()}.
     *
     * @param topicIdPartition topic partition
     * @param epochForOffset   leader epoch for the given offset
     * @param offset           offset
     * @return the requested remote log segment metadata if it exists.
     * @throws RemoteStorageException if there are any storage related errors occurred.
     */
    Optional<RemoteLogSegmentMetadata> remoteLogSegmentMetadata(TopicIdPartition topicIdPartition,
                                                                int epochForOffset,
                                                                long offset)
            throws RemoteStorageException;

    /**
     * Returns the highest log offset of topic partition for the given leader epoch in remote storage. This is used by
     * remote log management subsystem to know up to which offset the segments have been copied to remote storage for
     * a given leader epoch.
     *
     * @param topicIdPartition topic partition
     * @param leaderEpoch      leader epoch
     * @return the requested highest log offset if exists.
     * @throws RemoteStorageException if there are any storage related errors occurred.
     */
    Optional<Long> highestOffsetForEpoch(TopicIdPartition topicIdPartition,
                                         int leaderEpoch) throws RemoteStorageException;

    /**
     * This method is used to update the metadata about remote partition delete event asynchronously. Currently, it allows updating the
     * state ({@link RemotePartitionDeleteState}) of a topic partition in remote metadata storage. Controller invokes
     * this method with {@link RemotePartitionDeleteMetadata} having state as {@link RemotePartitionDeleteState#DELETE_PARTITION_MARKED}.
     * So, remote partition removers can act on this event to clean the respective remote log segments of the partition.
     * <p><br>
     * In the case of default RLMM implementation, remote partition remover processes {@link RemotePartitionDeleteState#DELETE_PARTITION_MARKED}
     * <ul>
     * <li> sends an event with state as {@link RemotePartitionDeleteState#DELETE_PARTITION_STARTED}
     * <li> gets all the remote log segments and deletes them.
     * <li> sends an event with state as {@link RemotePartitionDeleteState#DELETE_PARTITION_FINISHED} once all the remote log segments are
     * deleted.
     * </ul>
     *
     * @param remotePartitionDeleteMetadata update on delete state of a partition.
     * @throws RemoteStorageException          if there are any storage related errors occurred.
     * @throws RemoteResourceNotFoundException when there are no resources associated with the given remotePartitionDeleteMetadata.
     * @return a CompletableFuture which will complete once this operation is finished.
     */
    CompletableFuture<Void> putRemotePartitionDeleteMetadata(RemotePartitionDeleteMetadata remotePartitionDeleteMetadata)
            throws RemoteStorageException;

    /**
     * Returns all the remote log segment metadata of the given topicIdPartition.
     * <p>
     * Remote Partition Removers uses this method to fetch all the segments for a given topic partition, so that they
     * can delete them.
     *
     * @return Iterator of all the remote log segment metadata for the given topic partition.
     */
    Iterator<RemoteLogSegmentMetadata> listRemoteLogSegments(TopicIdPartition topicIdPartition)
            throws RemoteStorageException;

    /**
     * Returns iterator of remote log segment metadata, sorted by {@link RemoteLogSegmentMetadata#startOffset()} in
     * ascending order which contains the given leader epoch. This is used by remote log retention management subsystem
     * to fetch the segment metadata for a given leader epoch.
     *
     * @param topicIdPartition topic partition
     * @param leaderEpoch      leader epoch
     * @return Iterator of remote segments, sorted by start offset in ascending order.
     */
    Iterator<RemoteLogSegmentMetadata> listRemoteLogSegments(TopicIdPartition topicIdPartition,
                                                             int leaderEpoch) throws RemoteStorageException;

    /**
     * This method is invoked only when there are changes in leadership of the topic partitions that this broker is
     * responsible for.
     *
     * @param leaderPartitions   partitions that have become leaders on this broker.
     * @param followerPartitions partitions that have become followers on this broker.
     */
    void onPartitionLeadershipChanges(Set<TopicIdPartition> leaderPartitions,
                                      Set<TopicIdPartition> followerPartitions);

    /**
     * This method is invoked only when the topic partitions are stopped on this broker. This can happen when a
     * partition is emigrated to other broker or a partition is deleted.
     *
     * @param partitions topic partitions that have been stopped.
     */
    void onStopPartitions(Set<TopicIdPartition> partitions);

    /**
     * Returns total size of the log for the given leader epoch in remote storage.
     *
     * @param topicIdPartition topic partition for which size needs to be calculated.
     * @param leaderEpoch Size will only include segments belonging to this epoch.
     * @return Total size of the log stored in remote storage in bytes.
     */
    long remoteLogSize(TopicIdPartition topicIdPartition, int leaderEpoch) throws RemoteStorageException;

    /**
     * Returns the next segment metadata that contains the aborted transaction entries for the given topic partition, epoch and offset.
     * <ul>
     *     <li>The default implementation returns the segment metadata that matches the given epoch and offset
     *     irrespective of the presence of the transaction index.</li>
     *     <li>The custom implementation can optimize by returning the next segment metadata that contains the txn index
     *     in the given epoch. If there are no segments with txn index in the given epoch, then return empty.</li>
     * </ul>
     * @param topicIdPartition topic partition to search for.
     * @param epoch leader epoch for the given offset.
     * @param offset offset
     * @return The next segment metadata. The transaction index may or may not exist in the returned segment metadata
     * which depends on the RLMM plugin implementation. The caller of this method handles for both the cases.
     * @throws RemoteStorageException if there are any storage related errors occurred.
     */
    default Optional<RemoteLogSegmentMetadata> nextSegmentWithTxnIndex(TopicIdPartition topicIdPartition,
                                                                       int epoch,
                                                                       long offset) throws RemoteStorageException {
        return remoteLogSegmentMetadata(topicIdPartition, epoch, offset);
    }

    /**
     * Denotes whether the partition metadata is ready to serve.
     *
     * @param topicIdPartition topic partition
     * @return True if the partition is ready to serve for remote storage operations.
     */
    default boolean isReady(TopicIdPartition topicIdPartition) {
        return true;
    }
}
