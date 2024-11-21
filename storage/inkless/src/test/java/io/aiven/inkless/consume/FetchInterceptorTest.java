// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.consume;

import io.aiven.inkless.common.SharedState;
import io.aiven.inkless.config.InklessConfig;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.MetadataView;
import io.aiven.inkless.storage_backend.common.StorageBackend;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class FetchInterceptorTest {
    Time time = new MockTime();
    @Mock
    InklessConfig inklessConfig;
    @Mock
    MetadataView metadataView;
    @Mock
    ControlPlane controlPlane;
    @Mock
    StorageBackend storageBackend;
    @Mock
    Consumer<Map<TopicIdPartition, FetchPartitionData>> responseCallback;

    @Captor
    ArgumentCaptor<Map<TopicIdPartition, FetchPartitionData>> resultCaptor;

    private final short fetchVersion = ApiMessageType.FETCH.highestSupportedVersion(true);
    private final Uuid inklessUuid = Uuid.randomUuid();
    private final Uuid inklessOtherUuid = Uuid.randomUuid();
    private final Uuid classicUuid = Uuid.randomUuid();

    @Test
    public void mixingInklessAndClassicTopicsIsNotAllowed() {
        when(metadataView.isInklessTopic(eq("inkless"))).thenReturn(true);
        when(metadataView.isInklessTopic(eq("non_inkless"))).thenReturn(false);
        final FetchInterceptor interceptor = new FetchInterceptor(new SharedState(time, inklessConfig, metadataView, controlPlane, storageBackend));

        final FetchParams params = new FetchParams(fetchVersion,
                -1, -1, -1, -1, -1,
                FetchIsolation.LOG_END, Optional.empty());

        final Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos = Map.of(
                new TopicIdPartition(inklessUuid, 0, "inkless"),
                new FetchRequest.PartitionData(inklessUuid, 0, 0, 1024, Optional.empty()),
                new TopicIdPartition(classicUuid, 0, "non_inkless"),
                new FetchRequest.PartitionData(classicUuid, 0, 0, 1024, Optional.empty())
        );

        final boolean result = interceptor.intercept(params, fetchInfos, responseCallback);
        assertThat(result).isTrue();

        FetchPartitionData error = new FetchPartitionData(Errors.INVALID_REQUEST, -1, -1,
                null, Optional.empty(), OptionalLong.empty(),
                Optional.empty(), OptionalInt.empty(), false);

        verify(responseCallback).accept(resultCaptor.capture());
        assertThat(resultCaptor.getValue())
                .isNotNull()
                .containsKeys(
                        new TopicIdPartition(inklessUuid, 0, "inkless"),
                        new TopicIdPartition(classicUuid, 0, "non_inkless")
                )
                .values()
                .allMatch(fetchPartitionData -> fetchPartitionData.error == Errors.INVALID_REQUEST);
    }

    @Test
    public void notInterceptProducingToClassicTopics() {
        when(metadataView.isInklessTopic(eq("non_inkless"))).thenReturn(false);
        final FetchInterceptor interceptor = new FetchInterceptor(new SharedState(time, inklessConfig, metadataView, controlPlane, storageBackend));

        final FetchParams params = new FetchParams(fetchVersion,
                -1, -1, -1, -1, -1,
                FetchIsolation.LOG_END, Optional.empty());

        final Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos = Map.of(
                new TopicIdPartition(classicUuid, 0, "non_inkless"),
                new FetchRequest.PartitionData(classicUuid, 0, 0, 1024, Optional.empty())
        );

        final boolean result = interceptor.intercept(params, fetchInfos, responseCallback);
        assertThat(result).isFalse();
        verify(responseCallback, never()).accept(any());
    }

}
