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
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("warmup-%d", i)
		value := []byte("warmup")
		item := &blazecache.Item{Key: key, Value: value}
		client.Set(item)
	}
	
	fmt.Println("=== Testing Different Worker Counts ===")
	
	workerCounts := []int{10, 25, 50, 75, 100, 150, 200}
	totalOps := 10000
	
	for _, numWorkers := range workerCounts {
		var wg sync.WaitGroup
		var successCount int64
		opsPerWorker := totalOps / numWorkers
		
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
						continue
					}
					atomic.AddInt64(&successCount, 1)
				}
			}(w)
		}
		
		wg.Wait()
		elapsed := time.Since(start)
		
		throughput := float64(atomic.LoadInt64(&successCount)) / elapsed.Seconds()
		fmt.Printf("%3d workers: %8.2f ops/sec (%.2f ms)\n", numWorkers, throughput, elapsed.Seconds()*1000)
	}
}
