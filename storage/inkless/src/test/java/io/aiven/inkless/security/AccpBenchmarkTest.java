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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.security.Provider;
import java.security.SecureRandom;
import java.security.Security;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

/**
 * Benchmark test comparing AES-GCM performance between default Java crypto and ACCP.
 *
 * <p>Run with: ./gradlew :storage:inkless:test --tests "io.aiven.inkless.security.AccpBenchmarkTest"
 *
 * <p>This test measures encryption/decryption throughput for AES-256-GCM, which is the
 * cipher used by TLS 1.3 and is the main CPU hotspot identified in profiling.
 */
@Tag("benchmark")
class AccpBenchmarkTest {

    private static final int WARMUP_ITERATIONS = 1000;
    private static final int BENCHMARK_ITERATIONS = 10000;
    private static final int DATA_SIZE_KB = 16; // Typical Kafka batch size
    private static final int GCM_TAG_LENGTH = 128;
    private static final int GCM_IV_LENGTH = 12;

    private static SecretKey secretKey;
    private static byte[] testData;
    private static byte[] iv;

    @BeforeAll
    static void setUp() throws Exception {
        // Generate test key
        KeyGenerator keyGen = KeyGenerator.getInstance("AES");
        keyGen.init(256);
        secretKey = keyGen.generateKey();

        // Generate test data
        testData = new byte[DATA_SIZE_KB * 1024];
        new SecureRandom().nextBytes(testData);

        // Generate IV
        iv = new byte[GCM_IV_LENGTH];
        new SecureRandom().nextBytes(iv);
    }

    @Test
    void benchmarkAesGcmEncryption() throws Exception {
        System.out.println("\n========================================");
        System.out.println("AES-256-GCM Encryption Benchmark");
        System.out.println("Data size: " + DATA_SIZE_KB + " KB");
        System.out.println("Iterations: " + BENCHMARK_ITERATIONS);
        System.out.println("========================================\n");

        // Benchmark with default provider
        double defaultThroughput = benchmarkEncryption("Default Java Crypto", null);

        // Try to install ACCP
        boolean accpInstalled = AccpInstaller.install();

        if (accpInstalled) {
            // Benchmark with ACCP
            double accpThroughput = benchmarkEncryption("ACCP", "AmazonCorrettoCryptoProvider");

            // Calculate improvement
            double improvement = (accpThroughput / defaultThroughput - 1) * 100;
            System.out.println("\n----------------------------------------");
            System.out.printf("ACCP improvement: %.1f%% faster%n", improvement);
            System.out.println("----------------------------------------\n");
        } else {
            System.out.println("\nACCP not available - skipping ACCP benchmark");
            System.out.println("To enable ACCP, ensure amazon-corretto-crypto-provider is on the classpath\n");
        }
    }

    @Test
    void benchmarkAesGcmDecryption() throws Exception {
        System.out.println("\n========================================");
        System.out.println("AES-256-GCM Decryption Benchmark");
        System.out.println("Data size: " + DATA_SIZE_KB + " KB");
        System.out.println("Iterations: " + BENCHMARK_ITERATIONS);
        System.out.println("========================================\n");

        // First encrypt the data
        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        GCMParameterSpec gcmSpec = new GCMParameterSpec(GCM_TAG_LENGTH, iv);
        cipher.init(Cipher.ENCRYPT_MODE, secretKey, gcmSpec);
        byte[] encryptedData = cipher.doFinal(testData);

        // Benchmark decryption with default provider
        double defaultThroughput = benchmarkDecryption("Default Java Crypto", null, encryptedData);

        // Try to install ACCP
        boolean accpInstalled = AccpInstaller.install();

        if (accpInstalled) {
            // Benchmark decryption with ACCP
            double accpThroughput = benchmarkDecryption("ACCP", "AmazonCorrettoCryptoProvider", encryptedData);

            // Calculate improvement
            double improvement = (accpThroughput / defaultThroughput - 1) * 100;
            System.out.println("\n----------------------------------------");
            System.out.printf("ACCP improvement: %.1f%% faster%n", improvement);
            System.out.println("----------------------------------------\n");
        } else {
            System.out.println("\nACCP not available - skipping ACCP benchmark");
        }
    }

    @Test
    void verifyAccpCorrectness() throws Exception {
        // Skip if ACCP not available
        if (!AccpInstaller.install()) {
            System.out.println("ACCP not available - skipping correctness test");
            return;
        }

        System.out.println("\nVerifying ACCP produces correct results...");

        // Encrypt with default provider
        Cipher defaultCipher = Cipher.getInstance("AES/GCM/NoPadding");
        GCMParameterSpec gcmSpec = new GCMParameterSpec(GCM_TAG_LENGTH, iv);
        defaultCipher.init(Cipher.ENCRYPT_MODE, secretKey, gcmSpec);
        byte[] defaultEncrypted = defaultCipher.doFinal(testData);

        // Decrypt with ACCP
        Provider accpProvider = Security.getProvider("AmazonCorrettoCryptoProvider");
        Cipher accpCipher = Cipher.getInstance("AES/GCM/NoPadding", accpProvider);
        accpCipher.init(Cipher.DECRYPT_MODE, secretKey, gcmSpec);
        byte[] accpDecrypted = accpCipher.doFinal(defaultEncrypted);

        // Verify correctness
        assertArrayEquals(testData, accpDecrypted, "ACCP decryption should produce original data");

        // Also test ACCP encryption -> default decryption
        accpCipher.init(Cipher.ENCRYPT_MODE, secretKey, gcmSpec);
        byte[] accpEncrypted = accpCipher.doFinal(testData);

        defaultCipher.init(Cipher.DECRYPT_MODE, secretKey, gcmSpec);
        byte[] defaultDecrypted = defaultCipher.doFinal(accpEncrypted);

        assertArrayEquals(testData, defaultDecrypted, "Default decryption of ACCP encrypted data should work");

        System.out.println("ACCP correctness verified!\n");
    }

    private double benchmarkEncryption(String name, String providerName) throws Exception {
        Cipher cipher;
        if (providerName != null) {
            Provider provider = Security.getProvider(providerName);
            if (provider == null) {
                throw new IllegalStateException("Provider not found: " + providerName);
            }
            cipher = Cipher.getInstance("AES/GCM/NoPadding", provider);
        } else {
            cipher = Cipher.getInstance("AES/GCM/NoPadding");
        }

        SecureRandom random = new SecureRandom();
        byte[] iterIv = new byte[GCM_IV_LENGTH];

        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            random.nextBytes(iterIv);
            GCMParameterSpec gcmSpec = new GCMParameterSpec(GCM_TAG_LENGTH, iterIv);
            cipher.init(Cipher.ENCRYPT_MODE, secretKey, gcmSpec);
            cipher.doFinal(testData);
        }

        // Benchmark
        long startTime = System.nanoTime();
        for (int i = 0; i < BENCHMARK_ITERATIONS; i++) {
            random.nextBytes(iterIv);
            GCMParameterSpec gcmSpec = new GCMParameterSpec(GCM_TAG_LENGTH, iterIv);
            cipher.init(Cipher.ENCRYPT_MODE, secretKey, gcmSpec);
            cipher.doFinal(testData);
        }
        long endTime = System.nanoTime();

        double durationMs = (endTime - startTime) / 1_000_000.0;
        double throughputMBps = (BENCHMARK_ITERATIONS * DATA_SIZE_KB / 1024.0) / (durationMs / 1000.0);
        double opsPerSec = BENCHMARK_ITERATIONS / (durationMs / 1000.0);

        System.out.printf("%s:%n", name);
        System.out.printf("  Duration: %.2f ms%n", durationMs);
        System.out.printf("  Throughput: %.2f MB/s%n", throughputMBps);
        System.out.printf("  Operations: %.0f ops/sec%n", opsPerSec);
        System.out.println();

        return throughputMBps;
    }

    private double benchmarkDecryption(String name, String providerName, byte[] encryptedData) throws Exception {
        Cipher cipher;
        if (providerName != null) {
            Provider provider = Security.getProvider(providerName);
            if (provider == null) {
                throw new IllegalStateException("Provider not found: " + providerName);
            }
            cipher = Cipher.getInstance("AES/GCM/NoPadding", provider);
        } else {
            cipher = Cipher.getInstance("AES/GCM/NoPadding");
        }

        GCMParameterSpec gcmSpec = new GCMParameterSpec(GCM_TAG_LENGTH, iv);

        // Warmup
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            cipher.init(Cipher.DECRYPT_MODE, secretKey, gcmSpec);
            cipher.doFinal(encryptedData);
        }

        // Benchmark
        long startTime = System.nanoTime();
        for (int i = 0; i < BENCHMARK_ITERATIONS; i++) {
            cipher.init(Cipher.DECRYPT_MODE, secretKey, gcmSpec);
            cipher.doFinal(encryptedData);
        }
        long endTime = System.nanoTime();

        double durationMs = (endTime - startTime) / 1_000_000.0;
        double throughputMBps = (BENCHMARK_ITERATIONS * DATA_SIZE_KB / 1024.0) / (durationMs / 1000.0);
        double opsPerSec = BENCHMARK_ITERATIONS / (durationMs / 1000.0);

        System.out.printf("%s:%n", name);
        System.out.printf("  Duration: %.2f ms%n", durationMs);
        System.out.printf("  Throughput: %.2f MB/s%n", throughputMBps);
        System.out.printf("  Operations: %.0f ops/sec%n", opsPerSec);
        System.out.println();

        return throughputMBps;
    }
}
