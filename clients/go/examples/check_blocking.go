package main

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
	blazecache "github.com/blazecache/client"
)

func main() {
	serverAddr := "127.0.0.1:6792"
	
	fmt.Printf("=== Synchronous Blocking Analysis ===\n")
	fmt.Printf("Server: %s\n\n", serverAddr)
	
	// Create client
	client, err := blazecache.New(serverAddr)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	
	// Test ping first
	if err := client.Ping(); err != nil {
		log.Fatalf("Ping failed: %v", err)
	}
	fmt.Println("✓ Server connection verified\n")
	
	// Test 1: Sequential operations (should be slow)
	fmt.Println("=== Test 1: Sequential Operations ===")
	start1 := time.Now()
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("seq-key-%d", i)
		value := []byte(fmt.Sprintf("value-%d", i))
		item := &blazecache.Item{Key: key, Value: value}
		if err := client.Set(item); err != nil {
			log.Fatalf("Set failed: %v", err)
		}
	}
	seqTime := time.Since(start1)
	fmt.Printf("100 sequential SET operations: %s\n", seqTime)
	fmt.Printf("Throughput: %.2f ops/sec\n", 100.0/seqTime.Seconds())
	
	// Test 2: Concurrent operations (should be faster)
	fmt.Println("\n=== Test 2: Concurrent Operations (100 goroutines) ===")
	start2 := time.Now()
	var wg sync.WaitGroup
	var successCount int64
	
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			key := fmt.Sprintf("concurrent-key-%d", idx)
			value := []byte(fmt.Sprintf("value-%d", idx))
			item := &blazecache.Item{Key: key, Value: value}
			if err := client.Set(item); err != nil {
				return
			}
			atomic.AddInt64(&successCount, 1)
		}(i)
	}
	
	wg.Wait()
	concurrentTime := time.Since(start2)
	success := atomic.LoadInt64(&successCount)
	
	fmt.Printf("100 concurrent SET operations: %s\n", concurrentTime)
	fmt.Printf("Successful: %d\n", success)
	fmt.Printf("Throughput: %.2f ops/sec\n", float64(success)/concurrentTime.Seconds())
	
	// Analysis
	fmt.Println("\n=== Analysis ===")
	speedup := seqTime.Seconds() / concurrentTime.Seconds()
	fmt.Printf("Concurrent speedup: %.2fx\n", speedup)
	
	if speedup < 1.5 {
		fmt.Println("⚠️  WARNING: Concurrent operations are not much faster!")
		fmt.Println("   This suggests operations are blocking each other.")
		fmt.Println("   Possible causes:")
		fmt.Println("   - Lock contention in connection pool")
		fmt.Println("   - Synchronous operations")
		fmt.Println("   - Connection pool too small")
	} else {
		fmt.Println("✓ Concurrent operations show good speedup.")
		fmt.Println("  Operations appear to be truly concurrent.")
	}
	
	// Test 3: Measure operation overlap
	fmt.Println("\n=== Test 3: Operation Overlap Analysis ===")
	fmt.Println("Measuring if operations can overlap...")
	
	start3 := time.Now()
	var wg2 sync.WaitGroup
	operationTimes := make([]time.Duration, 10)
	
	for i := 0; i < 10; i++ {
		wg2.Add(1)
		go func(idx int) {
			defer wg2.Done()
			opStart := time.Now()
			key := fmt.Sprintf("overlap-key-%d", idx)
			value := []byte(fmt.Sprintf("value-%d", idx))
			item := &blazecache.Item{Key: key, Value: value}
			client.Set(item)
			operationTimes[idx] = time.Since(opStart)
		}(i)
	}
	
	wg2.Wait()
	totalTime := time.Since(start3)
	
	var maxOpTime time.Duration
	for _, t := range operationTimes {
		if t > maxOpTime {
			maxOpTime = t
		}
	}
	
	fmt.Printf("Total time for 10 operations: %s\n", totalTime)
	fmt.Printf("Longest single operation: %s\n", maxOpTime)
	fmt.Printf("Overlap ratio: %.2f\n", float64(maxOpTime)/float64(totalTime))
	
	if float64(maxOpTime)/float64(totalTime) > 0.5 {
		fmt.Println("⚠️  WARNING: Operations are not overlapping well!")
		fmt.Println("   They appear to be serialized.")
	} else {
		fmt.Println("✓ Operations are overlapping well.")
	}
}

