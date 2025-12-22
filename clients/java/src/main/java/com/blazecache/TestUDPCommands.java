package com.blazecache;

import java.io.IOException;
import java.util.Optional;

/**
 * Individual command tests for UDP client.
 */
public class TestUDPCommands {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: TestUDPCommands <server-address>");
            System.err.println("Example: TestUDPCommands 127.0.0.1:6793");
            System.exit(1);
        }
        
        String serverAddr = args[0];
        
        try (UDPClient client = new UDPClient(serverAddr)) {
            System.out.println("╔════════════════════════════════════════════════════════════╗");
            System.out.println("║     Testing Java UDP Client - Individual Commands         ║");
            System.out.println("╚════════════════════════════════════════════════════════════╝");
            System.out.println("Server: " + serverAddr);
            System.out.println();
            
            int passed = 0;
            int failed = 0;
            
            // Test 1: PING
            System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
            System.out.println("Test 1: PING");
            System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
            try {
                client.ping();
                System.out.println("✓ PASS: PING successful");
                passed++;
            } catch (IOException e) {
                System.err.println("✗ FAIL: PING failed: " + e.getMessage());
                failed++;
            }
            System.out.println();
            
            // Test 2: PUT (small value)
            System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
            System.out.println("Test 2: PUT (small value)");
            System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
            try {
                String key = "test-key-small";
                String value = "Hello, BlazeCache UDP!";
                client.set(key, value.getBytes());
                System.out.println("✓ PASS: PUT successful");
                System.out.println("  Key: " + key);
                System.out.println("  Value: " + value);
                passed++;
            } catch (IOException e) {
                System.err.println("✗ FAIL: PUT failed: " + e.getMessage());
                failed++;
            }
            System.out.println();
            
            // Test 3: GET (small value)
            System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
            System.out.println("Test 3: GET (small value)");
            System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
            try {
                String key = "test-key-small";
                Optional<byte[]> result = client.get(key);
                if (result.isPresent()) {
                    String value = new String(result.get());
                    System.out.println("✓ PASS: GET successful");
                    System.out.println("  Key: " + key);
                    System.out.println("  Value: " + value);
                    if (value.equals("Hello, BlazeCache UDP!")) {
                        System.out.println("  ✓ Value matches expected");
                    } else {
                        System.out.println("  ✗ Value mismatch!");
                        failed++;
                    }
                    passed++;
                } else {
                    System.err.println("✗ FAIL: GET returned empty (key not found)");
                    failed++;
                }
            } catch (IOException e) {
                System.err.println("✗ FAIL: GET failed: " + e.getMessage());
                failed++;
            }
            System.out.println();
            
            // Test 4: GET (non-existent key)
            System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
            System.out.println("Test 4: GET (non-existent key)");
            System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
            try {
                String key = "non-existent-key-12345";
                Optional<byte[]> result = client.get(key);
                if (result.isEmpty()) {
                    System.out.println("✓ PASS: GET correctly returned empty for non-existent key");
                    passed++;
                } else {
                    System.err.println("✗ FAIL: GET should have returned empty");
                    failed++;
                }
            } catch (IOException e) {
                System.err.println("✗ FAIL: GET failed: " + e.getMessage());
                failed++;
            }
            System.out.println();
            
            // Test 5: PUT (large value - fragmentation)
            System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
            System.out.println("Test 5: PUT (large value - fragmentation)");
            System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
            try {
                String key = "test-key-large";
                byte[] largeValue = new byte[5000]; // Will be fragmented
                for (int i = 0; i < largeValue.length; i++) {
                    largeValue[i] = (byte) (i % 256);
                }
                client.set(key, largeValue);
                System.out.println("✓ PASS: PUT large message successful");
                System.out.println("  Key: " + key);
                System.out.println("  Size: " + largeValue.length + " bytes (fragmented)");
                passed++;
            } catch (IOException e) {
                System.err.println("✗ FAIL: PUT large message failed: " + e.getMessage());
                failed++;
            }
            System.out.println();
            
            // Test 6: GET (large value - reassembly)
            System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
            System.out.println("Test 6: GET (large value - reassembly)");
            System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
            try {
                String key = "test-key-large";
                Optional<byte[]> result = client.get(key);
                if (result.isPresent()) {
                    byte[] received = result.get();
                    System.out.println("✓ PASS: GET large message successful");
                    System.out.println("  Key: " + key);
                    System.out.println("  Size: " + received.length + " bytes (reassembled)");
                    if (received.length == 5000) {
                        System.out.println("  ✓ Size matches expected (5000 bytes)");
                        // Verify content
                        boolean matches = true;
                        for (int i = 0; i < received.length; i++) {
                            if (received[i] != (byte) (i % 256)) {
                                matches = false;
                                break;
                            }
                        }
                        if (matches) {
                            System.out.println("  ✓ Content matches expected");
                        } else {
                            System.out.println("  ✗ Content mismatch!");
                            failed++;
                        }
                    } else {
                        System.out.println("  ✗ Size mismatch! Expected 5000, got " + received.length);
                        failed++;
                    }
                    passed++;
                } else {
                    System.err.println("✗ FAIL: GET large message returned empty");
                    failed++;
                }
            } catch (IOException e) {
                System.err.println("✗ FAIL: GET large message failed: " + e.getMessage());
                failed++;
            }
            System.out.println();
            
            // Test 7: DELETE
            System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
            System.out.println("Test 7: DELETE");
            System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
            try {
                String key = "test-key-small";
                boolean deleted = client.delete(key);
                if (deleted) {
                    System.out.println("✓ PASS: DELETE successful");
                    System.out.println("  Key: " + key);
                    passed++;
                } else {
                    System.err.println("✗ FAIL: DELETE returned false (key not found)");
                    failed++;
                }
            } catch (IOException e) {
                System.err.println("✗ FAIL: DELETE failed: " + e.getMessage());
                failed++;
            }
            System.out.println();
            
            // Test 8: GET (after DELETE - should not exist)
            System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
            System.out.println("Test 8: GET (after DELETE - should not exist)");
            System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
            try {
                String key = "test-key-small";
                Optional<byte[]> result = client.get(key);
                if (result.isEmpty()) {
                    System.out.println("✓ PASS: GET correctly returned empty after DELETE");
                    passed++;
                } else {
                    System.err.println("✗ FAIL: GET should have returned empty after DELETE");
                    failed++;
                }
            } catch (IOException e) {
                System.err.println("✗ FAIL: GET failed: " + e.getMessage());
                failed++;
            }
            System.out.println();
            
            // Test 9: PUT with TTL
            System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
            System.out.println("Test 9: PUT with TTL");
            System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
            try {
                String key = "test-key-ttl";
                String value = "TTL test value";
                client.set(key, value.getBytes(), 3600); // 1 hour TTL
                System.out.println("✓ PASS: PUT with TTL successful");
                System.out.println("  Key: " + key);
                System.out.println("  Value: " + value);
                System.out.println("  TTL: 3600 seconds");
                passed++;
            } catch (IOException e) {
                System.err.println("✗ FAIL: PUT with TTL failed: " + e.getMessage());
                failed++;
            }
            System.out.println();
            
            // Summary
            System.out.println("╔════════════════════════════════════════════════════════════╗");
            System.out.println("║                      Test Summary                         ║");
            System.out.println("╚════════════════════════════════════════════════════════════╝");
            System.out.println("Total tests: " + (passed + failed));
            System.out.println("Passed: " + passed);
            System.out.println("Failed: " + failed);
            System.out.println();
            
            if (failed == 0) {
                System.out.println("✅ All tests passed!");
                System.exit(0);
            } else {
                System.out.println("❌ Some tests failed!");
                System.exit(1);
            }
            
        } catch (IOException e) {
            System.err.println("Failed to create UDP client: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}

