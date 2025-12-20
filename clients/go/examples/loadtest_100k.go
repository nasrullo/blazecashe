package main

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"time"
	blazecache "github.com/blazecache/client"
)

func main() {
	const (
		serverAddr = "127.0.0.1:6792"
		numOps     = 100000
		numWorkers = 100
	)

	fmt.Printf("=== Go Client Load Test: %d operations with %d workers ===\n\n", numOps, numWorkers)

	// Create client
	client, err := blazecache.New(serverAddr)
	if err != nil {
		log.Fatal(err)
	}

	// Test ping first
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

	// Enable CPU profiling
	cpuProfile, err := os.Create("cpu.prof")
	if err != nil {
		log.Fatalf("Failed to create CPU profile: %v", err)
	}
	defer cpuProfile.Close()
	if err := pprof.StartCPUProfile(cpuProfile); err != nil {
		log.Fatalf("Failed to start CPU profile: %v", err)
	}
	defer pprof.StopCPUProfile()

	// Enable memory profiling
	memProfile, err := os.Create("mem.prof")
	if err != nil {
		log.Fatalf("Failed to create memory profile: %v", err)
	}
	defer memProfile.Close()
	defer func() {
		runtime.GC()
		if err := pprof.WriteHeapProfile(memProfile); err != nil {
			log.Fatalf("Failed to write memory profile: %v", err)
		}
	}()

	start := time.Now()

	// Spawn workers
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			localSuccess := int64(0)
			localErrors := int64(0)

			for i := 0; i < opsPerWorker; i++ {
				key := fmt.Sprintf("key-%d-%d", workerID, i)
				value := []byte(fmt.Sprintf("value-%d-%d", workerID, i))

				// Set operation
				item := &blazecache.Item{Key: key, Value: value}
				if err := client.Set(item); err != nil {
					localErrors++
					continue
				}

				// Get operation
				result, err := client.Get(key)
				if err != nil || result == nil || string(result.Value) != string(value) {
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

