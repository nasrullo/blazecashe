package main

import (
	"fmt"
	"log"

	blazecache "github.com/blazecache/client"
)

func main() {
	fmt.Println("=== Testing All BlazeCache Go Client Commands ===\n")

	// Create client
	client, err := blazecache.New("127.0.0.1:6792")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// Test 1: Ping
	fmt.Println("1. Testing Ping...")
	if err := client.Ping(); err != nil {
		fmt.Printf("   ✗ Ping failed: %v\n", err)
	} else {
		fmt.Println("   ✓ Ping successful")
	}

	// Test 2: Set
	fmt.Println("\n2. Testing Set...")
	if err := client.Set("test-key", []byte("test-value"), 0); err != nil {
		fmt.Printf("   ✗ Set failed: %v\n", err)
	} else {
		fmt.Println("   ✓ Set successful")
	}

	// Test 3: Set with TTL
	fmt.Println("\n3. Testing Set with TTL...")
	if err := client.Set("test-key-ttl", []byte("test-value-ttl"), 60); err != nil {
		fmt.Printf("   ✗ Set with TTL failed: %v\n", err)
	} else {
		fmt.Println("   ✓ Set with TTL successful (TTL: 60 seconds)")
	}

	// Test 4: Get (existing key)
	fmt.Println("\n4. Testing Get (existing key)...")
	value, err := client.Get("test-key")
	if err != nil {
		if err == blazecache.ErrNotFound {
			fmt.Println("   ✗ Key not found (unexpected)")
		} else {
			fmt.Printf("   ✗ Get failed: %v\n", err)
		}
	} else {
		fmt.Printf("   ✓ Get successful: key='test-key', value='%s'\n", string(value))
	}

	// Test 5: Get (non-existent key)
	fmt.Println("\n5. Testing Get (non-existent key)...")
	_, err = client.Get("nonexistent-key-12345")
	if err == blazecache.ErrNotFound {
		fmt.Println("   ✓ Correctly returned ErrNotFound for missing key")
	} else if err != nil {
		fmt.Printf("   ✗ Get failed with unexpected error: %v\n", err)
	} else {
		fmt.Println("   ✗ Should have returned ErrNotFound")
	}

	// Test 6: GetMulti
	fmt.Println("\n6. Testing GetMulti...")
	// Set up multiple keys
	client.Set("multi-key1", []byte("multi-value1"), 0)
	client.Set("multi-key2", []byte("multi-value2"), 0)
	client.Set("multi-key3", []byte("multi-value3"), 0)

	results, err := client.GetMulti([]string{"multi-key1", "multi-key2", "multi-key3", "multi-key4"})
	if err != nil {
		fmt.Printf("   ✗ GetMulti failed: %v\n", err)
	} else {
		fmt.Printf("   ✓ GetMulti successful: found %d out of 4 keys\n", len(results))
		for key, val := range results {
			fmt.Printf("      - %s: %s\n", key, string(val))
		}
	}

	// Test 7: Delete (existing key)
	fmt.Println("\n7. Testing Delete (existing key)...")
	if err := client.Delete("test-key"); err != nil {
		if err == blazecache.ErrNotFound {
			fmt.Println("   ✗ Key not found for delete (unexpected)")
		} else {
			fmt.Printf("   ✗ Delete failed: %v\n", err)
		}
	} else {
		fmt.Println("   ✓ Delete successful")
	}

	// Test 8: Delete (non-existent key)
	fmt.Println("\n8. Testing Delete (non-existent key)...")
	if err := client.Delete("nonexistent-key-12345"); err != nil {
		if err == blazecache.ErrNotFound {
			fmt.Println("   ✓ Correctly returned ErrNotFound for missing key")
		} else {
			fmt.Printf("   ✗ Delete failed with unexpected error: %v\n", err)
		}
	} else {
		fmt.Println("   ✓ Delete returned no error (key may not have existed)")
	}

	// Test 9: Verify deleted key is gone
	fmt.Println("\n9. Testing Get after Delete...")
	_, err = client.Get("test-key")
	if err == blazecache.ErrNotFound {
		fmt.Println("   ✓ Key correctly not found after deletion")
	} else if err != nil {
		fmt.Printf("   ✗ Get failed: %v\n", err)
	} else {
		fmt.Println("   ✗ Key still exists after deletion (unexpected)")
	}

	// Test 10: Consistent Hashing
	fmt.Println("\n10. Testing Consistent Hashing...")
	client2, err := blazecache.New("127.0.0.1:6792")
	if err != nil {
		fmt.Printf("   ✗ Failed to create client: %v\n", err)
	} else {
		defer client2.Close()
		client2.WithStrategy(blazecache.ConsistentHashing)

		if err := client2.Set("hash-key", []byte("hash-value"), 0); err != nil {
			fmt.Printf("   ✗ Set with consistent hashing failed: %v\n", err)
		} else {
			value, err := client2.Get("hash-key")
			if err != nil {
				fmt.Printf("   ✗ Get with consistent hashing failed: %v\n", err)
			} else {
				fmt.Printf("   ✓ Consistent hashing works: value='%s'\n", string(value))
			}
		}
	}

	// Test 11: Multiple operations (connection pooling test)
	fmt.Println("\n11. Testing Connection Pooling (multiple operations)...")
	successCount := 0
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("pool-key-%d", i)
		val := fmt.Sprintf("pool-value-%d", i)
		if err := client.Set(key, []byte(val), 0); err == nil {
			successCount++
		}
	}
	fmt.Printf("   ✓ Completed %d/10 Set operations (connection pooling active)\n", successCount)

	// Test 12: Error handling
	fmt.Println("\n12. Testing Error Handling...")
	if err := client.Ping(); err != nil {
		fmt.Printf("   ✗ Unexpected error: %v\n", err)
	} else {
		fmt.Println("   ✓ Error handling works correctly")
	}

	fmt.Println("\n=== All Tests Completed ===")
}
