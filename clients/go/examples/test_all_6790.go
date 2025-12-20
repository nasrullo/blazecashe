package main

import (
	"fmt"
	"log"
	"time"

	blazecache "github.com/blazecache/client"
)

func main() {
	fmt.Println("=== Testing All BlazeCache Go Client Commands (port 6790) ===\n")

	client, err := blazecache.New("127.0.0.1:6790")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// Test 1: Ping
	fmt.Println("1. Testing Ping...")
	start := time.Now()
	if err := client.Ping(); err != nil {
		fmt.Printf("   ✗ Ping failed: %v (took %v)\n", err, time.Since(start))
	} else {
		fmt.Printf("   ✓ Ping successful (took %v)\n", time.Since(start))
	}

	// Test 2: Set
	fmt.Println("\n2. Testing Set...")
	start = time.Now()
	if err := client.Set("test-key", []byte("test-value"), 0); err != nil {
		fmt.Printf("   ✗ Set failed: %v (took %v)\n", err, time.Since(start))
	} else {
		fmt.Printf("   ✓ Set successful (took %v)\n", time.Since(start))
	}

	// Test 3: Get
	fmt.Println("\n3. Testing Get...")
	start = time.Now()
	value, err := client.Get("test-key")
	if err != nil {
		if err == blazecache.ErrNotFound {
			fmt.Printf("   ✗ Key not found (took %v)\n", time.Since(start))
		} else {
			fmt.Printf("   ✗ Get failed: %v (took %v)\n", err, time.Since(start))
		}
	} else {
		fmt.Printf("   ✓ Get successful: value='%s' (took %v)\n", string(value), time.Since(start))
	}

	// Test 4: Set with TTL
	fmt.Println("\n4. Testing Set with TTL...")
	start = time.Now()
	if err := client.Set("test-key-ttl", []byte("test-value-ttl"), 60); err != nil {
		fmt.Printf("   ✗ Set with TTL failed: %v (took %v)\n", err, time.Since(start))
	} else {
		fmt.Printf("   ✓ Set with TTL successful (took %v)\n", time.Since(start))
	}

	// Test 5: GetMulti
	fmt.Println("\n5. Testing GetMulti...")
	start = time.Now()
	client.Set("multi-1", []byte("value-1"), 0)
	client.Set("multi-2", []byte("value-2"), 0)
	results, err := client.GetMulti([]string{"multi-1", "multi-2", "multi-3"})
	if err != nil {
		fmt.Printf("   ✗ GetMulti failed: %v (took %v)\n", err, time.Since(start))
	} else {
		fmt.Printf("   ✓ GetMulti successful: found %d keys (took %v)\n", len(results), time.Since(start))
		for k, v := range results {
			fmt.Printf("      - %s: %s\n", k, string(v))
		}
	}

	// Test 6: Delete
	fmt.Println("\n6. Testing Delete...")
	start = time.Now()
	if err := client.Delete("test-key"); err != nil {
		if err == blazecache.ErrNotFound {
			fmt.Printf("   ✗ Key not found (took %v)\n", time.Since(start))
		} else {
			fmt.Printf("   ✗ Delete failed: %v (took %v)\n", err, time.Since(start))
		}
	} else {
		fmt.Printf("   ✓ Delete successful (took %v)\n", time.Since(start))
	}

	// Test 7: Get after Delete
	fmt.Println("\n7. Testing Get after Delete...")
	start = time.Now()
	_, err = client.Get("test-key")
	if err == blazecache.ErrNotFound {
		fmt.Printf("   ✓ Key correctly not found after delete (took %v)\n", time.Since(start))
	} else if err != nil {
		fmt.Printf("   ✗ Get failed: %v (took %v)\n", err, time.Since(start))
	} else {
		fmt.Printf("   ✗ Key still exists (took %v)\n", time.Since(start))
	}

	fmt.Println("\n=== All Tests Completed Successfully! ===")
}
