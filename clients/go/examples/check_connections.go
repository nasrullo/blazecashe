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
	
	fmt.Printf("=== Connection Reuse Analysis ===\n")
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
	
	// Monitor connections during load test
	fmt.Println("=== Monitoring Connections During Load Test ===")
	fmt.Println("Running 1000 operations with 10 workers...\n")
	
	var (
		successCount int64
		errorCount   int64
		wg           sync.WaitGroup
		numOps       = 1000
		numWorkers   = 10
		opsPerWorker = numOps / numWorkers
	)
	
	start := time.Now()
	
	// Spawn workers
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			localSuccess := int64(0)
			localErrors := int64(0)
			
			for i := 0; i < opsPerWorker; i++ {
				key := fmt.Sprintf("conn-test-key-%d-%d", workerID, i)
				value := []byte(fmt.Sprintf("value-%d-%d", workerID, i))
				
				// Set operation
				item := &blazecache.Item{Key: key, Value: value}
				if err := client.Set(item); err != nil {
					localErrors++
					continue
				}
				
				// Get operation
				result, err := client.Get(key)
				if err != nil || result == nil {
					localErrors++
					continue
				}
				
				localSuccess++
			}
			
			atomic.AddInt64(&successCount, localSuccess)
			atomic.AddInt64(&errorCount, localErrors)
		}(w)
	}
	
	wg.Wait()
	elapsed := time.Since(start)
	
	totalOps := atomic.LoadInt64(&successCount) + atomic.LoadInt64(&errorCount)
	success := atomic.LoadInt64(&successCount)
	errors := atomic.LoadInt64(&errorCount)
	
	fmt.Printf("\n=== Results ===\n")
	fmt.Printf("Total operations: %d\n", totalOps)
	fmt.Printf("Successful: %d (%.2f%%)\n", success, float64(success)/float64(totalOps)*100)
	fmt.Printf("Errors: %d (%.2f%%)\n", errors, float64(errors)/float64(totalOps)*100)
	fmt.Printf("Time elapsed: %v\n", elapsed)
	fmt.Printf("Throughput: %.2f ops/sec\n", float64(success)/elapsed.Seconds())
	fmt.Printf("Avg latency: %.2f µs/op\n", float64(elapsed.Nanoseconds())/float64(success)/1000)
	
	fmt.Println("\n=== Connection Analysis ===")
	fmt.Println("Note: Check active connections with: netstat -an | grep 6792 | grep ESTABLISHED | wc -l")
	fmt.Println("Or: ss -an | grep 6792 | grep ESTAB | wc -l")
}

