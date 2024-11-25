package io.aiven.inkless.control_plane;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.config.InklessConfig;

public interface ControlPlane {
    List<CommitBatchResponse> commitFile(ObjectKey objectKey,
                                         List<CommitBatchRequest> batches);

    List<FindBatchResponse> findBatches(List<FindBatchRequest> findBatchRequests,
                                        boolean minOneMessage,
                                        int fetchMaxBytes);

    static ControlPlane init(final InklessConfig config, final MetadataView metadata) {
        final Class<ControlPlane> controlPlaneClass = config.controlPlaneClass();
        try {
            final Constructor<ControlPlane> ctor = controlPlaneClass.getConstructor(MetadataView.class);
            return ctor.newInstance(metadata);
        } catch (final NoSuchMethodException | InstantiationException | IllegalAccessException |
                       InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }
}
