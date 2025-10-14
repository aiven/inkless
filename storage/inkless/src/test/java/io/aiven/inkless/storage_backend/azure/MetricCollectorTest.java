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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class MetricCollectorTest {

    @Test
    void pathInDevWithAccountName() throws Exception {
        final var props = Map.of("azure.account.name", "test-account",
            "azure.container.name", "cont1");
        try (var metrics = new MetricCollector()) {
            var matcher = metrics.pathPattern(new AzureBlobStorageConfig(props)).matcher("/test-account/cont1/test-object");
            assertThat(matcher).matches();
        }
    }

    @Test
    void pathInProdWithoutAccountName() throws Exception {
        final var props = Map.of("azure.account.name", "test-account",
            "azure.container.name", "cont1");
        try (var metrics = new MetricCollector()) {
            var matcher = metrics.pathPattern(new AzureBlobStorageConfig(props)).matcher("/cont1/test-object");
            assertThat(matcher).matches();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"comp=test", "comp=test&post=val", "pre=val&comp=test", "pre=val&comp=test&post=val"})
    void uploadQueryWithComp(final String query) {
        final var matcher = MetricCollector.MetricsPolicy.UPLOAD_QUERY_PATTERN.matcher(query);
        assertThat(matcher.find()).isTrue();
        assertThat(matcher.group("comp")).isEqualTo("test");
    }
}
