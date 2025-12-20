package main

import (
	"fmt"
	"log"
	"time"
	blazecache "github.com/blazecache/client"
)

func main() {
	serverAddr := "127.0.0.1:6792"
	numOps := 10000
	numWorkers := 10

	fmt.Printf("=== Go Client Benchmark: %d operations with %d workers ===\n", numOps, numWorkers)

	client, err := blazecache.New(serverAddr)
	if err != nil {
		log.Fatalf("Failed: %v", err)
	}

	if err := client.Ping(); err != nil {
		log.Fatalf("Server connection failed: %v", err)
	}
	fmt.Println("✓ Server connection verified\n")

	start := time.Now()
	results := make(chan int, numWorkers)

	for w := 0; w < numWorkers; w++ {
		go func(workerID int) {
			opsPerWorker := numOps / numWorkers
			success := 0
			for i := 0; i < opsPerWorker; i++ {
				key := fmt.Sprintf("key-%d-%d", workerID, i)
				value := []byte(fmt.Sprintf("value-%d-%d", workerID, i))
				item := &blazecache.Item{Key: key, Value: value}

				if err := client.Set(item); err == nil {
					success++
				}
				if _, err := client.Get(key); err == nil {
					success++
				}
			}
			results <- success
		}(w)
	}

	totalSuccess := 0
	for i := 0; i < numWorkers; i++ {
		totalSuccess += <-results
	}

	elapsed := time.Since(start)
	throughput := float64(totalSuccess) / elapsed.Seconds()
	avgLatency := elapsed.Seconds() / float64(totalSuccess) * 1_000_000

	fmt.Println("=== Results ===")
	fmt.Printf("Total operations: %d\n", totalSuccess)
	fmt.Printf("Time elapsed: %v\n", elapsed)
	fmt.Printf("Throughput: %.2f ops/sec\n", throughput)
	fmt.Printf("Avg latency: %.2f µs/op\n", avgLatency)
}
