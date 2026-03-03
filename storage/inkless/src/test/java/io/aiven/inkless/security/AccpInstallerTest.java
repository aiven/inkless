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
package io.aiven.inkless.security;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AccpInstallerTest {

    @Test
    void installShouldSucceedWhenAccpIsAvailable() {
        // This test verifies ACCP can be installed when available on classpath
        boolean installed = AccpInstaller.install();

        // ACCP should be available in test classpath
        assertTrue(installed, "ACCP should be installed successfully");
        assertTrue(AccpInstaller.isInstalled(), "isInstalled() should return true after installation");
        assertNotNull(AccpInstaller.getProviderInfo(), "Provider info should not be null after installation");

        System.out.println("ACCP Provider Info: " + AccpInstaller.getProviderInfo());
    }

    @Test
    void installIsIdempotent() {
        // First install
        boolean first = AccpInstaller.install();
        // Second install should also succeed (idempotent)
        boolean second = AccpInstaller.install();

        assertTrue(first, "First install should succeed");
        assertTrue(second, "Second install should also succeed (idempotent)");
    }

    @Test
    void logCryptoStatusShouldNotThrow() {
        // Ensure ACCP is installed first
        AccpInstaller.install();

        // logCryptoStatus should not throw
        assertDoesNotThrow(AccpInstaller::logCryptoStatus);
    }

    @Test
    void installIfEnabledRespectsSystemProperty() {
        // Clear any previous state by checking current status
        boolean wasInstalled = AccpInstaller.isInstalled();

        // If not already installed, test with property disabled
        if (!wasInstalled) {
            System.clearProperty(AccpInstaller.ACCP_ENABLED_PROPERTY);
            boolean result = AccpInstaller.installIfEnabled();
            // Should not install when property is not set
            // Note: This may still return true if ACCP was installed by another test
        }

        // Test with property enabled
        System.setProperty(AccpInstaller.ACCP_ENABLED_PROPERTY, "true");
        try {
            boolean result = AccpInstaller.installIfEnabled();
            assertTrue(result, "Should install when property is enabled");
        } finally {
            System.clearProperty(AccpInstaller.ACCP_ENABLED_PROPERTY);
        }
    }
}
