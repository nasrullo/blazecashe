package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blazecache/client"
)

func main() {
	serverAddr := flag.String("server", "127.0.0.1:6793", "UDP server address")
	numOps := flag.Int("ops", 1000, "Number of operations per worker")
	numWorkers := flag.Int("workers", 10, "Number of concurrent workers")
	flag.Parse()

	fmt.Printf("=== Go UDP Client Load Test ===\n")
	fmt.Printf("Server: %s\n", *serverAddr)
	fmt.Printf("Operations: %d per worker\n", *numOps)
	fmt.Printf("Workers: %d\n\n", *numWorkers)

	// Verify server connection
	testClient, err := blazecache.NewUDPClient(*serverAddr)
	if err != nil {
		log.Fatalf("Failed to create UDP client: %v", err)
	}
	defer testClient.Close()

	if err := testClient.Ping(); err != nil {
		log.Fatalf("Server connection failed: %v", err)
	}
	fmt.Println("✓ Server connection verified\n")

	start := time.Now()
	var wg sync.WaitGroup
	var totalSuccess int64
	var totalErrors int64

	opsPerWorker := *numOps / *numWorkers
	remainder := *numOps % *numWorkers

	for workerID := 0; workerID < *numWorkers; workerID++ {
		ops := opsPerWorker
		if workerID < remainder {
			ops++
		}

		wg.Add(1)
		go func(id int, numOps int) {
			defer wg.Done()

			client, err := blazecache.NewUDPClient(*serverAddr)
			if err != nil {
				atomic.AddInt64(&totalErrors, int64(numOps*2)) // SET + GET
				return
			}
			defer client.Close()

			var success int64
			var errors int64

			for i := 0; i < numOps; i++ {
				key := fmt.Sprintf("key-%d-%d", id, i)
				value := []byte(fmt.Sprintf("value-%d-%d", id, i))

				// SET operation
				if err := client.Set(key, value, 3600); err != nil {
					errors++
				} else {
					success++
				}

				// GET operation
				retrieved, err := client.Get(key)
				if err != nil {
					errors++
				} else if string(retrieved) == string(value) {
					success++
				} else {
					errors++
				}
			}

			atomic.AddInt64(&totalSuccess, success)
			atomic.AddInt64(&totalErrors, errors)
		}(workerID, ops)
	}

	wg.Wait()
	elapsed := time.Since(start)

	totalOps := totalSuccess + totalErrors
	throughput := float64(totalSuccess) / elapsed.Seconds()
	avgLatency := elapsed.Seconds() / float64(totalSuccess) * 1_000_000 // microseconds

	fmt.Println("=== Results ===")
	fmt.Printf("Total operations: %d (%d SET + %d GET)\n", totalOps, totalOps/2, totalOps/2)
	fmt.Printf("Successful: %d (%.2f%%)\n", totalSuccess, float64(totalSuccess)/float64(totalOps)*100.0)
	fmt.Printf("Errors: %d (%.2f%%)\n", totalErrors, float64(totalErrors)/float64(totalOps)*100.0)
	fmt.Printf("Time elapsed: %v\n", elapsed)
	fmt.Printf("Throughput: %.2f ops/sec\n", throughput)
	fmt.Printf("Avg latency: %.2f µs/op\n", avgLatency)

	if totalErrors > 0 {
		os.Exit(1)
	}
}

