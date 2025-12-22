package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/blazecache/client"
)

func main() {
	serverAddr := flag.String("server", "127.0.0.1:6793", "UDP server address")
	flag.Parse()

	fmt.Printf("=== Testing Go UDP Client ===\n")
	fmt.Printf("Server: %s\n\n", *serverAddr)

	// Create UDP client
	udpClient, err := blazecache.NewUDPClient(*serverAddr)
	if err != nil {
		log.Fatalf("Failed to create UDP client: %v", err)
	}
	defer udpClient.Close()

	// Test 1: PING
	fmt.Println("1. Testing PING...")
	start := time.Now()
	err = udpClient.Ping()
	elapsed := time.Since(start)
	if err != nil {
		fmt.Printf("   ✗ PING failed: %v\n", err)
	} else {
		fmt.Printf("   ✓ PING successful (took %v)\n", elapsed)
	}
	fmt.Println()

	// Test 2: SET (small value)
	fmt.Println("2. Testing SET (small value)...")
	testKey := "test-key-go"
	testValue := []byte("test-value-go")
	start = time.Now()
	err = udpClient.Set(testKey, testValue, 3600)
	elapsed = time.Since(start)
	if err != nil {
		fmt.Printf("   ✗ SET failed: %v\n", err)
	} else {
		fmt.Printf("   ✓ SET successful (took %v)\n", elapsed)
	}
	fmt.Println()

	// Test 3: GET
	fmt.Println("3. Testing GET...")
	start = time.Now()
	retrievedValue, err := udpClient.Get(testKey)
	elapsed = time.Since(start)
	if err != nil {
		fmt.Printf("   ✗ GET failed: %v\n", err)
	} else {
		if string(retrievedValue) == string(testValue) {
			fmt.Printf("   ✓ GET successful (took %v)\n", elapsed)
			fmt.Printf("   ✓ Value matches: %s\n", string(retrievedValue))
		} else {
			fmt.Printf("   ✗ GET value mismatch!\n")
			fmt.Printf("   Expected: %s\n", string(testValue))
			fmt.Printf("   Got:      %s\n", string(retrievedValue))
		}
	}
	fmt.Println()

	// Test 4: SET (large value - should trigger fragmentation)
	fmt.Println("4. Testing SET (large value - fragmentation)...")
	largeKey := "test-large-key-go"
	largeValue := make([]byte, 5000) // Larger than MAX_DATAGRAM
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}
	start = time.Now()
	err = udpClient.Set(largeKey, largeValue, 3600)
	elapsed = time.Since(start)
	if err != nil {
		fmt.Printf("   ✗ SET (large) failed: %v\n", err)
	} else {
		fmt.Printf("   ✓ SET (large) successful (took %v)\n", elapsed)
		fmt.Printf("   ✓ Sent %d bytes in fragments\n", len(largeValue))
	}
	fmt.Println()

	// Test 5: GET (large value - should trigger reassembly)
	fmt.Println("5. Testing GET (large value - reassembly)...")
	start = time.Now()
	retrievedLargeValue, err := udpClient.Get(largeKey)
	elapsed = time.Since(start)
	if err != nil {
		fmt.Printf("   ✗ GET (large) failed: %v\n", err)
	} else {
		if len(retrievedLargeValue) == len(largeValue) {
			match := true
			for i := range largeValue {
				if retrievedLargeValue[i] != largeValue[i] {
					match = false
					break
				}
			}
			if match {
				fmt.Printf("   ✓ GET (large) successful (took %v)\n", elapsed)
				fmt.Printf("   ✓ Reassembled %d bytes correctly\n", len(retrievedLargeValue))
			} else {
				fmt.Printf("   ✗ GET (large) value mismatch!\n")
			}
		} else {
			fmt.Printf("   ✗ GET (large) size mismatch!\n")
			fmt.Printf("   Expected: %d bytes\n", len(largeValue))
			fmt.Printf("   Got:      %d bytes\n", len(retrievedLargeValue))
		}
	}
	fmt.Println()

	// Test 6: GET (non-existent key)
	fmt.Println("6. Testing GET (non-existent key)...")
	start = time.Now()
	_, err = udpClient.Get("non-existent-key-go")
	elapsed = time.Since(start)
	if err != nil {
		if err.Error() == "key not found" {
			fmt.Printf("   ✓ GET (non-existent) correctly returned error (took %v)\n", elapsed)
			fmt.Printf("   ✓ Error: %v\n", err)
		} else {
			fmt.Printf("   ✗ GET (non-existent) returned unexpected error: %v\n", err)
		}
	} else {
		fmt.Printf("   ✗ GET (non-existent) should have failed but didn't!\n")
	}
	fmt.Println()

	// Test 7: Multiple concurrent SET/GET operations
	fmt.Println("7. Testing concurrent operations...")
	concurrentCount := 10
	successCount := 0
	errorCount := 0
	
	start = time.Now()
	done := make(chan bool, concurrentCount)
	for i := 0; i < concurrentCount; i++ {
		go func(id int) {
			key := fmt.Sprintf("concurrent-key-%d", id)
			value := []byte(fmt.Sprintf("concurrent-value-%d", id))
			
			if err := udpClient.Set(key, value, 3600); err != nil {
				errorCount++
				done <- false
				return
			}
			
			retrieved, err := udpClient.Get(key)
			if err != nil {
				errorCount++
				done <- false
				return
			}
			
			if string(retrieved) == string(value) {
				successCount++
				done <- true
			} else {
				errorCount++
				done <- false
			}
		}(i)
	}
	
	// Wait for all goroutines
	for i := 0; i < concurrentCount; i++ {
		<-done
	}
	elapsed = time.Since(start)
	
	fmt.Printf("   ✓ Concurrent operations completed (took %v)\n", elapsed)
	fmt.Printf("   ✓ Successful: %d/%d\n", successCount, concurrentCount)
	if errorCount > 0 {
		fmt.Printf("   ✗ Errors: %d\n", errorCount)
	}
	fmt.Println()

	fmt.Println("=== All Tests Complete ===")
	if successCount == concurrentCount && errorCount == 0 {
		fmt.Println("✓ All tests passed!")
		os.Exit(0)
	} else {
		fmt.Println("✗ Some tests failed")
		os.Exit(1)
	}
}

