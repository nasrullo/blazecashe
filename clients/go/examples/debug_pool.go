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
	
	client, err := blazecache.New(serverAddr)
	if err != nil {
		log.Fatalf("Failed: %v", err)
	}
	
	// Warm up
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("warmup-%d", i)
		value := []byte("warmup")
		item := &blazecache.Item{Key: key, Value: value}
		client.Set(item)
	}
	
	fmt.Println("=== Testing Connection Pool Behavior ===")
	fmt.Println("Running 1000 operations with 50 workers...\n")
	
	var (
		wg           sync.WaitGroup
		successCount int64
		errorCount   int64
		numWorkers   = 50
		opsPerWorker = 20
	)
	
	start := time.Now()
	
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				key := fmt.Sprintf("test-%d-%d", workerID, i)
				value := []byte(fmt.Sprintf("value-%d", i))
				item := &blazecache.Item{Key: key, Value: value}
				
				if err := client.Set(item); err != nil {
					atomic.AddInt64(&errorCount, 1)
					continue
				}
				atomic.AddInt64(&successCount, 1)
			}
		}(w)
	}
	
	wg.Wait()
	elapsed := time.Since(start)
	
	totalOps := atomic.LoadInt64(&successCount) + atomic.LoadInt64(&errorCount)
	
	fmt.Printf("Results:\n")
	fmt.Printf("  Total operations: %d\n", totalOps)
	fmt.Printf("  Successful: %d\n", atomic.LoadInt64(&successCount))
	fmt.Printf("  Errors: %d\n", atomic.LoadInt64(&errorCount))
	fmt.Printf("  Time: %s\n", elapsed)
	fmt.Printf("  Throughput: %.2f ops/sec\n", float64(atomic.LoadInt64(&successCount))/elapsed.Seconds())
	fmt.Printf("  Avg latency: %.2f µs/op\n", float64(elapsed.Nanoseconds())/float64(atomic.LoadInt64(&successCount))/1000)
}

