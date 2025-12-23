package com.blazecache;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * UDP Load Test Client for BlazeCache
 * Similar to Go perf_client.go, performs PUT/GET operations with configurable concurrency.
 */
public class UDPLoadTest {
    private static final int DEFAULT_CONCURRENCY = 100;
    private static final int DEFAULT_VALUE_SIZE = 1024;
    private static final int DEFAULT_DURATION = 60;
    private static final int DEFAULT_INTERVAL = 1;

    public static void main(String[] args) {
        // Parse arguments
        String serverAddr = "127.0.0.1:6793";
        int concurrency = DEFAULT_CONCURRENCY;
        int valueSize = DEFAULT_VALUE_SIZE;
        int duration = DEFAULT_DURATION;
        int interval = DEFAULT_INTERVAL;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--server":
                    if (i + 1 < args.length) serverAddr = args[++i];
                    break;
                case "--concurrency":
                    if (i + 1 < args.length) concurrency = Integer.parseInt(args[++i]);
                    break;
                case "--value-size":
                    if (i + 1 < args.length) valueSize = Integer.parseInt(args[++i]);
                    break;
                case "--duration":
                    if (i + 1 < args.length) duration = Integer.parseInt(args[++i]);
                    break;
                case "--interval":
                    if (i + 1 < args.length) interval = Integer.parseInt(args[++i]);
                    break;
                case "--help":
                    printUsage();
                    return;
            }
        }

        // Also support environment variables (for Docker)
        if (System.getenv("SERVER_ADDR") != null) {
            serverAddr = System.getenv("SERVER_ADDR");
        }
        if (System.getenv("CONCURRENCY") != null) {
            concurrency = Integer.parseInt(System.getenv("CONCURRENCY"));
        }
        if (System.getenv("VALUE_SIZE") != null) {
            valueSize = Integer.parseInt(System.getenv("VALUE_SIZE"));
        }
        if (System.getenv("DURATION") != null) {
            duration = Integer.parseInt(System.getenv("DURATION"));
        }
        if (System.getenv("INTERVAL") != null) {
            interval = Integer.parseInt(System.getenv("INTERVAL"));
        }

        // Make final copies for use in lambdas
        final String finalServerAddr = serverAddr;
        final int finalConcurrency = concurrency;
        final int finalValueSize = valueSize;
        final int finalDuration = duration;
        final int finalInterval = interval;

        System.out.println("=== Java UDP Client Performance Test ===");
        System.out.println("Server: " + finalServerAddr);
        System.out.println("Concurrency: " + finalConcurrency);
        System.out.println("Value size: " + finalValueSize + " bytes");
        System.out.println("Duration: " + finalDuration + " seconds");
        System.out.println("Interval: " + finalInterval + " seconds");
        System.out.println();

        // Wait for server to be ready
        // In Docker, rely on healthcheck - just wait a bit for server to be fully ready
        System.out.println("Waiting for server to be ready...");
        try {
            Thread.sleep(3000); // Wait for Docker healthcheck to pass
            System.out.println("✓ Assuming server is ready (Docker healthcheck passed)");
            System.out.println();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.exit(1);
        }

        // Prepare test data
        final byte[] value = new byte[finalValueSize];
        for (int i = 0; i < finalValueSize; i++) {
            value[i] = (byte) (i % 256);
        }

        // Statistics
        AtomicLong totalOps = new AtomicLong(0);
        AtomicLong totalErrors = new AtomicLong(0);
        AtomicLong requestID = new AtomicLong(0);
        List<Long> putLatencies = Collections.synchronizedList(new ArrayList<>());
        List<Long> getLatencies = Collections.synchronizedList(new ArrayList<>());

        long startTime = System.currentTimeMillis();
        long endTime = startTime + (finalDuration * 1000L);

        // Stats reporting thread
        ScheduledExecutorService statsExecutor = Executors.newScheduledThreadPool(1);
        statsExecutor.scheduleAtFixedRate(() -> {
            long ops = totalOps.get();
            long errors = totalErrors.get();
            long elapsed = (System.currentTimeMillis() - startTime) / 1000;
            double rps = elapsed > 0 ? (double) ops / elapsed : 0.0;
            System.out.printf("[Stats] Ops: %d, Errors: %d, RPS: %.2f, Elapsed: %ds%n",
                    ops, errors, rps, elapsed);
        }, finalInterval, finalInterval, TimeUnit.SECONDS);

        // Worker threads
        ExecutorService executor = Executors.newFixedThreadPool(finalConcurrency);
        CountDownLatch latch = new CountDownLatch(finalConcurrency);

        for (int i = 0; i < finalConcurrency; i++) {
            executor.submit(() -> {
                try {
                    UDPClient client = new UDPClient(finalServerAddr);
                    try {
                        while (System.currentTimeMillis() < endTime) {
                            long id = requestID.incrementAndGet();
                            String key = "perf-key-" + id;

                            // PUT operation
                            long putStart = System.nanoTime();
                            try {
                                client.set(key, value);
                            } catch (IOException e) {
                                totalErrors.incrementAndGet();
                                continue;
                            }
                            long putLatency = System.nanoTime() - putStart;
                            putLatencies.add(putLatency);

                            // GET operation
                            long getStart = System.nanoTime();
                            try {
                                Optional<byte[]> result = client.get(key);
                                if (!result.isPresent()) {
                                    totalErrors.incrementAndGet();
                                    continue;
                                }
                            } catch (IOException e) {
                                totalErrors.incrementAndGet();
                                continue;
                            }
                            long getLatency = System.nanoTime() - getStart;
                            getLatencies.add(getLatency);

                            totalOps.incrementAndGet();

                            // Limit latency list size
                            if (putLatencies.size() > 10000) {
                                synchronized (putLatencies) {
                                    if (putLatencies.size() > 10000) {
                                        putLatencies.subList(0, putLatencies.size() - 1000).clear();
                                        getLatencies.subList(0, getLatencies.size() - 1000).clear();
                                    }
                                }
                            }
                        }
                    } finally {
                        client.close();
                    }
                } catch (IOException e) {
                    System.err.println("Failed to create client: " + e.getMessage());
                    totalErrors.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }

        // Wait for completion
        try {
            latch.await(duration + 10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        statsExecutor.shutdown();
        executor.shutdown();
        try {
            executor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Final stats
        long elapsed = System.currentTimeMillis() - startTime;
        long ops = totalOps.get();
        long errors = totalErrors.get();
        double rps = (elapsed / 1000.0) > 0 ? (double) ops / (elapsed / 1000.0) : 0.0;

        System.out.println();
        System.out.println("=== Final Results ===");
        System.out.println("Total operations: " + ops);
        System.out.println("Errors: " + errors);
        System.out.println("Time elapsed: " + (elapsed / 1000.0) + "s");
        System.out.println("Throughput: " + String.format("%.2f", rps) + " ops/sec");

        if (!putLatencies.isEmpty()) {
            double avgPut = putLatencies.stream().mapToLong(Long::longValue).average().orElse(0.0) / 1_000_000.0;
            double avgGet = getLatencies.stream().mapToLong(Long::longValue).average().orElse(0.0) / 1_000_000.0;
            System.out.println("Avg PUT latency: " + String.format("%.2f", avgPut) + " ms");
            System.out.println("Avg GET latency: " + String.format("%.2f", avgGet) + " ms");
        }
    }

    private static void printUsage() {
        System.out.println("Usage: UDPLoadTest [options]");
        System.out.println("Options:");
        System.out.println("  --server <addr>        Server address (default: 127.0.0.1:6793)");
        System.out.println("  --concurrency <n>      Number of concurrent operations (default: 100)");
        System.out.println("  --value-size <bytes>   Value size in bytes (default: 1024)");
        System.out.println("  --duration <seconds>   Test duration in seconds (default: 60)");
        System.out.println("  --interval <seconds>   Stats reporting interval (default: 1)");
        System.out.println();
        System.out.println("Environment variables:");
        System.out.println("  SERVER_ADDR, CONCURRENCY, VALUE_SIZE, DURATION, INTERVAL");
    }
}

