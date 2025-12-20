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
        // Use virtual threads for blocking I/O - perfect for network operations
        // Virtual threads allow many concurrent blocking operations without OS thread overhead
        // Each operation blocks on I/O (network read/write), which virtual threads handle efficiently
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        // Semaphore limits concurrent operations to avoid overwhelming the connection pool
        // This prevents contention on ConcurrentHashMap/BlockingQueue in the pool
        // Virtual threads still provide high concurrency - allow more for better throughput
        int maxConcurrency = Math.max(numWorkers * 20, 200); // Increased for better parallelism
        Semaphore semaphore = new Semaphore(maxConcurrency);
        CountDownLatch latch = new CountDownLatch(numOps);
        AtomicLong success = new AtomicLong(0);
        AtomicLong errors = new AtomicLong(0);
        
        // Create one task per operation (each does SET then GET sequentially)
        // Virtual threads handle the blocking I/O efficiently, allowing high concurrency
        for (int i = 0; i < numOps; i++) {
            final int opId = i;
            executor.submit(() -> {
                try {
                    semaphore.acquire(); // Limit concurrent operations to avoid pool contention
                    try {
                        // Optimize: avoid String.format() - use StringBuilder for better performance
                        StringBuilder keyBuilder = new StringBuilder(16);
                        keyBuilder.append("key-").append(opId);
                        String key = keyBuilder.toString();
                        
                        StringBuilder valueBuilder = new StringBuilder(16);
                        valueBuilder.append("value-").append(opId);
                        byte[] value = valueBuilder.toString().getBytes();
                        
                        // SET operation (blocks on I/O - virtual thread handles this efficiently)
                        try {
                            client.set(key, value);
                            success.incrementAndGet();
                        } catch (IOException e) {
                            errors.incrementAndGet();
                            return; // Skip GET if SET failed
                        }
                        
                        // GET operation (blocks on I/O - virtual thread handles this efficiently)
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
                    } finally {
                        semaphore.release();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    errors.incrementAndGet();
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

