package org.apache.kafka.tools;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.coordinator.common.runtime.HdrHistogram;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class TestCaller {
    private static final HdrHistogram latencyHistogram = new HdrHistogram(60000, 3);
    private static volatile boolean running = true;
    
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        if (args.length != 1) {
            System.err.println("Usage: java TestCaller <number_of_threads>");
            System.exit(1);
        }
        
        int numThreads = Integer.parseInt(args[0]);
        
        final Properties props = new Properties();
        props.put("bootstrap.controllers", "127.0.0.1:29090,127.0.0.1:29091");
        
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        
        Thread statsReporter = new Thread(() -> {
            while (running) {
                try {
                    Thread.sleep(30000);
                    printStatistics();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
        statsReporter.setDaemon(true);
        statsReporter.start();
        
        for (int threadId = 0; threadId < numThreads; threadId++) {
            final int currentThreadId = threadId;
            executor.submit(() -> {
                try {
                    final AdminClient adminClient = KafkaAdminClient.create(props);
                    
                    for (int i = 0; i < 1000000; i++) {
                        long startTime = System.nanoTime();
                        final var f = adminClient.inklessCommit();
                        f.get();
                        long endTime = System.nanoTime();
                        long latencyMs = (endTime - startTime) / 1_000_000; // Convert to milliseconds
                        
                        // Record latency in histogram
                        latencyHistogram.record(latencyMs);

//                        if (i % 100 == 0) {
//                            System.out.println("Thread " + currentThreadId + ": " + i + " (latency: " + latencyMs + "ms)");
//                        }
                    }
                    
                    adminClient.close();
                } catch (Exception e) {
                    System.err.println("Thread " + currentThreadId + " failed: " + e.getMessage());
                    e.printStackTrace();
                }
            });
        }
        
        executor.shutdown();
        while (!executor.isTerminated()) {
            Thread.sleep(100);
        }
        
        running = false;
        statsReporter.interrupt();
        System.out.println("\n=== Final Statistics ===");
        printStatistics();
    }
    
    private static void printStatistics() {
        long now = System.currentTimeMillis();
        long count = latencyHistogram.count(now);
        
        if (count == 0) {
            System.out.println("No latency data available.");
            return;
        }
        
        // Get percentiles from histogram
        double p50 = latencyHistogram.measurePercentile(now, 50.0);
        double p90 = latencyHistogram.measurePercentile(now, 90.0);
        double p99 = latencyHistogram.measurePercentile(now, 99.0);
        long maxLatency = latencyHistogram.max(now);
        
        System.out.println(String.format(
            "=== Latency Statistics (Total calls: %d) ===\n" +
            "P50: %.2fms\n" +
            "P90: %.2fms\n" +
            "P99: %.2fms\n" +
            "Max: %dms\n",
            count,
            p50, p90, p99, maxLatency
        ));
    }
}
