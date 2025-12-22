package com.blazecache;

import java.io.IOException;
import java.util.Optional;

/**
 * Simple test client demonstrating UDP client usage with QUIC-like features.
 */
public class TestUDPClient {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: TestUDPClient <server-address>");
            System.err.println("Example: TestUDPClient 127.0.0.1:6793");
            System.exit(1);
        }
        
        String serverAddr = args[0];
        
        try (UDPClient client = new UDPClient(serverAddr)) {
            System.out.println("Testing UDP client with QUIC-like features...");
            System.out.println("Server: " + serverAddr);
            System.out.println();
            
            // Test PING
            System.out.println("1. Testing PING...");
            try {
                client.ping();
                System.out.println("   ✓ PING successful");
            } catch (IOException e) {
                System.err.println("   ✗ PING failed: " + e.getMessage());
                return;
            }
            
            // Test PUT
            System.out.println("2. Testing PUT...");
            try {
                String key = "test-key";
                String value = "Hello, BlazeCache UDP!";
                client.set(key, value.getBytes());
                System.out.println("   ✓ PUT successful: " + key + " = " + value);
            } catch (IOException e) {
                System.err.println("   ✗ PUT failed: " + e.getMessage());
                return;
            }
            
            // Test GET
            System.out.println("3. Testing GET...");
            try {
                String key = "test-key";
                Optional<byte[]> result = client.get(key);
                if (result.isPresent()) {
                    String value = new String(result.get());
                    System.out.println("   ✓ GET successful: " + key + " = " + value);
                } else {
                    System.err.println("   ✗ GET failed: key not found");
                }
            } catch (IOException e) {
                System.err.println("   ✗ GET failed: " + e.getMessage());
                return;
            }
            
            // Test DELETE
            System.out.println("4. Testing DELETE...");
            try {
                String key = "test-key";
                boolean deleted = client.delete(key);
                if (deleted) {
                    System.out.println("   ✓ DELETE successful: " + key);
                } else {
                    System.err.println("   ✗ DELETE failed: key not found");
                }
            } catch (IOException e) {
                System.err.println("   ✗ DELETE failed: " + e.getMessage());
                return;
            }
            
            // Test large message (fragmentation)
            System.out.println("5. Testing large message (fragmentation)...");
            try {
                String key = "large-key";
                byte[] largeValue = new byte[5000]; // Will be fragmented
                for (int i = 0; i < largeValue.length; i++) {
                    largeValue[i] = (byte) (i % 256);
                }
                client.set(key, largeValue);
                System.out.println("   ✓ PUT large message successful: " + key + " (" + largeValue.length + " bytes)");
                
                Optional<byte[]> result = client.get(key);
                if (result.isPresent() && result.get().length == largeValue.length) {
                    System.out.println("   ✓ GET large message successful: " + key + " (" + result.get().length + " bytes)");
                } else {
                    System.err.println("   ✗ GET large message failed: size mismatch");
                }
            } catch (IOException e) {
                System.err.println("   ✗ Large message test failed: " + e.getMessage());
            }
            
            System.out.println();
            System.out.println("All tests completed!");
            
        } catch (IOException e) {
            System.err.println("Failed to create UDP client: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}

