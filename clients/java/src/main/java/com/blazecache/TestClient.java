package com.blazecache;

import java.io.IOException;
import java.util.*;

public class TestClient {
    
    public static void main(String[] args) {
        System.out.println("Testing BlazeCache Java client...");
        
        try {
            // Create client
            List<String> servers = Arrays.asList("127.0.0.1:6784");
            CacheClient client = new CacheClient(servers);
            
            // Test ping
            try {
                client.ping();
                System.out.println("✓ Ping successful");
            } catch (IOException e) {
                System.out.println("✗ Ping failed: " + e.getMessage());
            }
            
            // Test set
            try {
                client.set("java-key", "java-value".getBytes());
                System.out.println("✓ Set successful");
            } catch (IOException e) {
                System.out.println("✗ Set failed: " + e.getMessage());
            }
            
            // Test get
            try {
                Optional<byte[]> result = client.get("java-key");
                if (result.isPresent()) {
                    System.out.println("✓ Get successful: " + new String(result.get()));
                } else {
                    System.out.println("✗ Key not found");
                }
            } catch (IOException e) {
                System.out.println("✗ Get failed: " + e.getMessage());
            }
            
            // Test get non-existent key
            try {
                Optional<byte[]> result = client.get("nonexistent");
                if (result.isEmpty()) {
                    System.out.println("✓ Correctly returned empty for missing key");
                } else {
                    System.out.println("✗ Should not have found key");
                }
            } catch (IOException e) {
                System.out.println("✗ Get failed: " + e.getMessage());
            }
            
            // Test delete
            try {
                boolean deleted = client.delete("java-key");
                if (deleted) {
                    System.out.println("✓ Delete successful");
                } else {
                    System.out.println("✗ Key not found for delete");
                }
            } catch (IOException e) {
                System.out.println("✗ Delete failed: " + e.getMessage());
            }
            
            // Test multi-get
            try {
                client.set("key1", "value1".getBytes());
                client.set("key2", "value2".getBytes());
                
                List<String> keys = Arrays.asList("key1", "key2", "key3");
                Map<String, byte[]> results = client.getMulti(keys);
                
                System.out.println("✓ Multi-get successful: " + results.size() + " keys found");
                for (Map.Entry<String, byte[]> entry : results.entrySet()) {
                    System.out.println("  " + entry.getKey() + ": " + new String(entry.getValue()));
                }
            } catch (IOException e) {
                System.out.println("✗ Multi-get failed: " + e.getMessage());
            }
            
            System.out.println("\n✅ Java client test completed!");
            
        } catch (Exception e) {
            System.err.println("Test failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
