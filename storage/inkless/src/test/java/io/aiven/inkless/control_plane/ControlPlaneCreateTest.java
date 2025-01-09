package io.aiven.inkless.control_plane;

import org.apache.kafka.common.utils.MockTime;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.stream.Stream;

import io.aiven.inkless.config.InklessConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class ControlPlaneCreateTest {

    @Mock
    private InklessConfig inklessConfig;
    @Mock
    private MetadataView metadataView;

    public static Stream<Arguments> controlPlaneClasses() {
        return Stream.of(
            Arguments.of(InMemoryControlPlane.class)
        );
    }

    @ParameterizedTest
    @MethodSource("controlPlaneClasses")
    void testCreate(Class<ControlPlane> controlPlaneClass) {
        when(inklessConfig.controlPlaneClass()).thenReturn(controlPlaneClass);
        final var controlPlane = ControlPlane.create(inklessConfig, new MockTime());
        assertThat(controlPlane)
            .isNotNull()
            .isOfAnyClassIn(controlPlaneClass);
    }

    @Test
    void invalidControlPlaneClass() {
        when(inklessConfig.controlPlaneClass()).thenReturn(ControlPlane.class);
        assertThatThrownBy(() -> ControlPlane.create(inklessConfig, new MockTime()))
            .isInstanceOf(RuntimeException.class);
    }
}