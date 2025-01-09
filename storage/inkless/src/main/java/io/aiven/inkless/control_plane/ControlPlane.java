// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;

import java.io.Closeable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Set;

import io.aiven.inkless.config.InklessConfig;

public interface ControlPlane extends Closeable, Configurable {
    List<CommitBatchResponse> commitFile(String objectKey,
                                         int uploaderBrokerId,
                                         long fileSize,
                                         List<CommitBatchRequest> batches);

    List<FindBatchResponse> findBatches(List<FindBatchRequest> findBatchRequests,
                                        boolean minOneMessage,
                                        int fetchMaxBytes);

    void createTopicAndPartitions(Set<CreateTopicAndPartitionsRequest> requests);

    void deleteTopics(Set<Uuid> topicIds);

    List<FileToDelete> getFilesToDelete();

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

    /**
     * There are scenarios where Control Plane needs to be shared across Broker and Controller.
     * This method provides a way to get the same instance of the control plane.
     *
     * @return Get a new or existing instance of the control plane.
     */
    static ControlPlane get(final InklessConfig config, final Time time) {
        return ControlPlaneProvider.get(config, time);
    }

    class ControlPlaneProvider {
        private static ControlPlane instance;

        synchronized static ControlPlane get(final InklessConfig config, final Time time) {
            if (instance == null) {
                instance = ControlPlane.create(config, time);
            }
            return instance;
        }
    }
}
