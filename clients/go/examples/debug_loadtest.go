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
		numOps     = 1000
		numWorkers = 10
	)

	fmt.Printf("=== Go Client Debug Load Test: %d operations with %d workers ===\n\n", numOps, numWorkers)

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
		setErrors     int64
		getErrors     int64
		valueMismatch int64
		successCount  int64
		errorTypes    = make(map[string]int64)
		mu            sync.Mutex
		wg            sync.WaitGroup
		opsPerWorker  = numOps / numWorkers
	)

	start := time.Now()

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for i := 0; i < opsPerWorker; i++ {
				key := fmt.Sprintf("key-%d-%d", workerID, i)
				value := []byte(fmt.Sprintf("value-%d-%d", workerID, i))

				// Set operation
				if err := client.Set(key, value, 0); err != nil {
					atomic.AddInt64(&setErrors, 1)
					mu.Lock()
					errorTypes[err.Error()]++
					mu.Unlock()
					continue
				}

				// Get operation
				result, err := client.Get(key)
				if err != nil {
					atomic.AddInt64(&getErrors, 1)
					mu.Lock()
					errorTypes[err.Error()]++
					mu.Unlock()
					continue
				}

				if string(result) != string(value) {
					atomic.AddInt64(&valueMismatch, 1)
					continue
				}

				atomic.AddInt64(&successCount, 1)
			}
		}(w)
	}

	wg.Wait()
	elapsed := time.Since(start)

	total := atomic.LoadInt64(&successCount) + atomic.LoadInt64(&setErrors) + atomic.LoadInt64(&getErrors) + atomic.LoadInt64(&valueMismatch)

	fmt.Printf("\n=== Results ===\n")
	fmt.Printf("Total operations: %d\n", total)
	fmt.Printf("Successful: %d (%.2f%%)\n", atomic.LoadInt64(&successCount), float64(atomic.LoadInt64(&successCount))/float64(total)*100)
	fmt.Printf("Set errors: %d\n", atomic.LoadInt64(&setErrors))
	fmt.Printf("Get errors: %d\n", atomic.LoadInt64(&getErrors))
	fmt.Printf("Value mismatches: %d\n", atomic.LoadInt64(&valueMismatch))
	fmt.Printf("Time elapsed: %v\n", elapsed)
	fmt.Printf("Throughput: %.2f ops/sec\n", float64(atomic.LoadInt64(&successCount))/elapsed.Seconds())

	fmt.Printf("\n=== Error Breakdown ===\n")
	mu.Lock()
	for errType, count := range errorTypes {
		fmt.Printf("  %s: %d\n", errType, count)
	}
	mu.Unlock()
}



