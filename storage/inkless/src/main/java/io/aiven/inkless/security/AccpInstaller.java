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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.Provider;
import java.security.Security;

/**
 * Utility to install Amazon Corretto Crypto Provider (ACCP) for faster SSL/TLS operations.
 *
 * <p>ACCP provides hardware-accelerated cryptographic operations using OpenSSL under the hood,
 * which can significantly improve SSL/TLS performance (2-3x faster for AES-GCM).
 *
 * <p>To enable ACCP, set the system property {@code inkless.accp.enabled=true} or call
 * {@link #install()} directly at application startup.
 *
 * <p>ACCP must be installed before any SSL connections are made to be effective.
 *
 * @see <a href="https://github.com/corretto/amazon-corretto-crypto-provider">ACCP GitHub</a>
 */
public final class AccpInstaller {

    private static final Logger LOGGER = LoggerFactory.getLogger(AccpInstaller.class);

    /**
     * System property to enable ACCP installation.
     */
    public static final String ACCP_ENABLED_PROPERTY = "inkless.accp.enabled";

    /**
     * ACCP provider class name.
     */
    private static final String ACCP_PROVIDER_CLASS = "com.amazon.corretto.crypto.provider.AmazonCorrettoCryptoProvider";

    /**
     * ACCP install method name.
     */
    private static final String ACCP_INSTALL_METHOD = "install";

    private static volatile boolean installed = false;
    private static volatile boolean installAttempted = false;

    private AccpInstaller() {
        // Utility class
    }

    /**
     * Install ACCP if enabled via system property and available on the classpath.
     *
     * <p>This method is idempotent - calling it multiple times has no additional effect.
     *
     * @return true if ACCP was successfully installed, false otherwise
     */
    public static boolean installIfEnabled() {
        final String enabled = System.getProperty(ACCP_ENABLED_PROPERTY, "false");
        if ("true".equalsIgnoreCase(enabled)) {
            LOGGER.info("ACCP is enabled via system property -D{}=true", ACCP_ENABLED_PROPERTY);
            return install();
        } else {
            LOGGER.info("ACCP is disabled. To enable hardware-accelerated SSL/TLS, set -D{}=true",
                ACCP_ENABLED_PROPERTY);
            return false;
        }
    }

    /**
     * Install ACCP as the highest priority security provider.
     *
     * <p>This method is idempotent - calling it multiple times has no additional effect.
     *
     * @return true if ACCP was successfully installed, false otherwise
     */
    public static synchronized boolean install() {
        if (installed) {
            LOGGER.debug("ACCP already installed");
            return true;
        }

        if (installAttempted) {
            LOGGER.debug("ACCP installation already attempted and failed");
            return false;
        }

        installAttempted = true;

        try {
            // Try to load and install ACCP using reflection to avoid compile-time dependency
            final Class<?> accpClass = Class.forName(ACCP_PROVIDER_CLASS);
            final java.lang.reflect.Method installMethod = accpClass.getMethod(ACCP_INSTALL_METHOD);
            installMethod.invoke(null);

            installed = true;
            LOGGER.info("Amazon Corretto Crypto Provider (ACCP) installed successfully. " +
                "SSL/TLS operations will use hardware-accelerated cryptography.");

            // Log provider info
            final Provider provider = Security.getProvider("AmazonCorrettoCryptoProvider");
            if (provider != null) {
                LOGGER.info("ACCP version: {}", provider.getVersionStr());
            }

            return true;

        } catch (ClassNotFoundException e) {
            LOGGER.warn("ACCP not available on classpath. SSL/TLS will use default Java crypto. " +
                "To enable ACCP, add the amazon-corretto-crypto-provider dependency.");
            return false;

        } catch (NoSuchMethodException e) {
            LOGGER.error("ACCP install method not found. This may indicate an incompatible ACCP version.", e);
            return false;

        } catch (Exception e) {
            LOGGER.error("Failed to install ACCP: {}. SSL/TLS will use default Java crypto.", e.getMessage());
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("ACCP installation error details", e);
            }
            return false;
        }
    }

    /**
     * Check if ACCP is currently installed as a security provider.
     *
     * @return true if ACCP is installed
     */
    public static boolean isInstalled() {
        return installed || Security.getProvider("AmazonCorrettoCryptoProvider") != null;
    }

    /**
     * Get information about the currently installed ACCP provider.
     *
     * @return provider info string, or null if not installed
     */
    public static String getProviderInfo() {
        final Provider provider = Security.getProvider("AmazonCorrettoCryptoProvider");
        if (provider != null) {
            return String.format("ACCP %s", provider.getVersionStr());
        }
        return null;
    }

    /**
     * Log the current crypto provider status at INFO level.
     * Call this after initialization to confirm the crypto configuration.
     */
    public static void logCryptoStatus() {
        if (isInstalled()) {
            final String providerInfo = getProviderInfo();
            LOGGER.info("============================================================");
            LOGGER.info("CRYPTO PROVIDER: Amazon Corretto Crypto Provider (ACCP)");
            LOGGER.info("  Status: ENABLED - using hardware-accelerated cryptography");
            LOGGER.info("  Version: {}", providerInfo != null ? providerInfo : "unknown");
            LOGGER.info("  Expected SSL/TLS performance improvement: 3-4x faster");
            LOGGER.info("============================================================");
        } else {
            LOGGER.info("============================================================");
            LOGGER.info("CRYPTO PROVIDER: Default Java Cryptography");
            LOGGER.info("  Status: ACCP is NOT enabled");
            LOGGER.info("  To enable ACCP for better SSL/TLS performance, start with:");
            LOGGER.info("    -D{}=true", ACCP_ENABLED_PROPERTY);
            LOGGER.info("============================================================");
        }
    }
}
