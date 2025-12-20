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
	const (
		serverAddr = "127.0.0.1:6792"
		numOps     = 100000
		numWorkers = 50  // Reduced from 100
	)

	fmt.Printf("=== Go Client Load Test: %d operations with %d workers ===\n\n", numOps, numWorkers)

	client, err := blazecache.New(serverAddr)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	if err := client.Ping(); err != nil {
		log.Fatalf("Ping failed: %v", err)
	}
	fmt.Println("✓ Server connection verified")

	var (
		successCount int64
		errorCount   int64
		wg           sync.WaitGroup
		opsPerWorker = numOps / numWorkers
	)

	start := time.Now()

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			localSuccess := int64(0)
			localErrors := int64(0)

			for i := 0; i < opsPerWorker; i++ {
				key := fmt.Sprintf("key-%d-%d", workerID, i)
				value := []byte(fmt.Sprintf("value-%d-%d", workerID, i))

				if err := client.Set(key, value, 0); err != nil {
					localErrors++
					continue
				}

				result, err := client.Get(key)
				if err != nil || string(result) != string(value) {
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
}


