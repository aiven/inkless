package io.aiven.inkless.cache;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;

import static org.junit.jupiter.api.Assertions.*;

class InfinispanCacheTest {

    final Time time = new MockTime();

    @TempDir
    Path baseDir;

    @Test
    void testCacheCreationWithBasicConfig() throws IOException {
        InfinispanCache cache = new InfinispanCache(
            time,
            "test-cluster",
            "rack1",
            1000L,
            baseDir,
            false,
            300L,
            60
        );

        assertNotNull(cache);
        assertEquals(0, cache.size());
        cache.close();
    }

    @Test
    void testCacheCreationWithPersistentConfig() throws IOException {
        InfinispanCache cache = new InfinispanCache(
            time,
            "test-cluster",
            "rack1",
            1000L,
            baseDir,
            true,
            300L,
            60
        );

        assertNotNull(cache);
        assertEquals(0, cache.size());

        // Verify cache directories are created
        Path cacheDir = baseDir.resolve("inkless-cache");
        assertTrue(Files.exists(cacheDir));
        assertTrue(Files.isDirectory(cacheDir));

        cache.close();
    }

    @Test
    void testCachePersistenceDirCreation() {
        Path result = InfinispanCache.cachePersistenceDir(baseDir);

        assertNotNull(result);
        assertTrue(Files.exists(result));
        assertTrue(Files.isDirectory(result));
        assertEquals(baseDir.resolve("inkless-cache"), result);
    }

    @Test
    void testCachePersistenceDirAlreadyExists() throws IOException {
        Path cacheDir = baseDir.resolve("inkless-cache");
        Files.createDirectories(cacheDir);

        Path result = InfinispanCache.cachePersistenceDir(baseDir);

        assertEquals(cacheDir, result);
        assertTrue(Files.exists(result));
        assertTrue(Files.isDirectory(result));
    }

    @Test
    void testCachePersistenceDirNotDirectory() throws IOException {
        Path notADir = baseDir.resolve("inkless-cache");
        Files.createFile(notADir);

        ConfigException exception = assertThrows(ConfigException.class, () -> InfinispanCache.cachePersistenceDir(baseDir));

        assertTrue(exception.getMessage().contains("Cache persistence directory is not a directory"));
    }

    @Test
    void testCachePersistenceDirNotWritable() throws IOException {
        Path cacheDir = baseDir.resolve("inkless-cache");
        Files.createDirectories(cacheDir);

        // Make directory non-writable (Unix/Linux only)
        try {
            Files.setPosixFilePermissions(cacheDir, PosixFilePermissions.fromString("r-xr-xr-x"));

            ConfigException exception = assertThrows(ConfigException.class, () -> {
                InfinispanCache.cachePersistenceDir(baseDir);
            });

            assertTrue(exception.getMessage().contains("Cache persistence directory is not writable"));
        } catch (UnsupportedOperationException e) {
            // Skip this test on non-POSIX systems (like Windows)
            System.out.println("Skipping writable test on non-POSIX system");
        }
    }

    @Test
    void testClusterNameGeneration() {
        // Test with rack
        assertEquals("inkless-cluster1-rack1", InfinispanCache.clusterName("cluster1", "rack1"));

        // Test without rack
        assertEquals("inkless-cluster1", InfinispanCache.clusterName("cluster1", null));
    }

    @Test
    void testCacheMetricsInitialization() throws IOException {
        InfinispanCache cache = new InfinispanCache(
            time,
            "test-cluster",
            "rack1",
            1000L,
            baseDir,
            false,
            300L,
            60
        );

        assertNotNull(cache.metrics());
        cache.close();
    }
}