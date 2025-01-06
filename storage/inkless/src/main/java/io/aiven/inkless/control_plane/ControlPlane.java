// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.utils.Time;

import java.io.Closeable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

import io.aiven.inkless.config.InklessConfig;

public interface ControlPlane extends Closeable, Configurable, TopicMetadataChangesSubscriber {
    List<CommitBatchResponse> commitFile(String objectKey,
                                         int uploaderBrokerId,
                                         long fileSize,
                                         List<CommitBatchRequest> batches);

    List<FindBatchResponse> findBatches(List<FindBatchRequest> findBatchRequests,
                                        boolean minOneMessage,
                                        int fetchMaxBytes);

    static ControlPlane create(final InklessConfig config, final Time time, final MetadataView metadata) {
        final Class<ControlPlane> controlPlaneClass = config.controlPlaneClass();
        try {
            final Constructor<ControlPlane> ctor = controlPlaneClass.getConstructor(Time.class, MetadataView.class);
            final ControlPlane result = ctor.newInstance(time, metadata);
            result.configure(config.controlPlaneConfig());
            return result;
        } catch (final NoSuchMethodException | InstantiationException | IllegalAccessException |
                       InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }
}
