package com.blazecache;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class Benchmark {
    public static void main(String[] args) {
        String serverAddr = "127.0.0.1:6792";
        int numOps = 10_000;
        int numWorkers = 10;
        
        if (args.length > 0) {
            serverAddr = args[0];
        }
        if (args.length > 1) {
            numOps = Integer.parseInt(args[1]);
        }
        if (args.length > 2) {
            numWorkers = Integer.parseInt(args[2]);
        }
        
        System.out.printf("=== Java Client Benchmark: %d operations with %d workers ===%n", numOps, numWorkers);
        
        // Verify server connection
        CacheClient testClient = new CacheClient(Arrays.asList(serverAddr));
        try {
            testClient.ping();
            System.out.println("✓ Server connection verified\n");
        } catch (IOException e) {
            System.out.println("✗ Server connection failed: " + e.getMessage());
            return;
        }
        
        // Create ONE shared client (like Go/Rust do) - all threads share the same connection pool
        CacheClient client = new CacheClient(Arrays.asList(serverAddr));
        
        long start = System.nanoTime();
        ExecutorService executor = Executors.newFixedThreadPool(numWorkers);
        CountDownLatch latch = new CountDownLatch(numWorkers);
        AtomicLong success = new AtomicLong(0);
        AtomicLong errors = new AtomicLong(0);
        
        int opsPerWorker = numOps / numWorkers;
        
        for (int workerId = 0; workerId < numWorkers; workerId++) {
            final int wid = workerId;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < opsPerWorker; i++) {
                        String key = String.format("key-%d-%d", wid, i);
                        byte[] value = String.format("value-%d-%d", wid, i).getBytes();
                        
                        // SET operation
                        try {
                            client.set(key, value);
                            success.incrementAndGet();
                        } catch (IOException e) {
                            errors.incrementAndGet();
                            continue;
                        }
                        
                        // GET operation
                        try {
                            Optional<byte[]> result = client.get(key);
                            if (result.isPresent() && Arrays.equals(result.get(), value)) {
                                success.incrementAndGet();
                            } else {
                                errors.incrementAndGet();
                            }
                        } catch (IOException e) {
                            errors.incrementAndGet();
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }
        
        try {
            latch.await();
            executor.shutdown();
            executor.awaitTermination(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            System.err.println("Interrupted: " + e.getMessage());
        }
        
        long elapsed = System.nanoTime() - start;
        double durationSeconds = elapsed / 1_000_000_000.0;
        long totalSuccess = success.get();
        long totalErrors = errors.get();
        double throughput = totalSuccess / durationSeconds;
        double avgLatency = durationSeconds / totalSuccess * 1_000_000.0; // microseconds
        
        System.out.println("=== Results ===");
        System.out.printf("Total operations: %d%n", totalSuccess + totalErrors);
        System.out.printf("Successful: %d (%.2f%%)%n", totalSuccess, 
            (totalSuccess * 100.0 / (totalSuccess + totalErrors)));
        System.out.printf("Errors: %d (%.2f%%)%n", totalErrors,
            (totalErrors * 100.0 / (totalSuccess + totalErrors)));
        System.out.printf("Time elapsed: %.3fs%n", durationSeconds);
        System.out.printf("Throughput: %.2f ops/sec%n", throughput);
        System.out.printf("Avg latency: %.2f µs/op%n", avgLatency);
    }
}

