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
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("warmup-%d", i)
		value := []byte("warmup")
		item := &blazecache.Item{Key: key, Value: value}
		client.Set(item)
	}
	
	fmt.Println("=== SET-only Performance ===")
	var wg1 sync.WaitGroup
	var success1 int64
	numOps := 10000
	numWorkers := 50
	
	start1 := time.Now()
	for w := 0; w < numWorkers; w++ {
		wg1.Add(1)
		go func(workerID int) {
			defer wg1.Done()
			for i := 0; i < numOps/numWorkers; i++ {
				key := fmt.Sprintf("set-%d-%d", workerID, i)
				value := []byte(fmt.Sprintf("value-%d", i))
				item := &blazecache.Item{Key: key, Value: value}
				if err := client.Set(item); err != nil {
					return
				}
				atomic.AddInt64(&success1, 1)
			}
		}(w)
	}
	wg1.Wait()
	elapsed1 := time.Since(start1)
	fmt.Printf("Throughput: %.2f ops/sec\n", float64(atomic.LoadInt64(&success1))/elapsed1.Seconds())
	
	fmt.Println("\n=== GET-only Performance (cache hits) ===")
	var wg2 sync.WaitGroup
	var success2 int64
	
	start2 := time.Now()
	for w := 0; w < numWorkers; w++ {
		wg2.Add(1)
		go func(workerID int) {
			defer wg2.Done()
			for i := 0; i < numOps/numWorkers; i++ {
				key := fmt.Sprintf("warmup-%d", (workerID*numOps/numWorkers+i)%100)
				if _, err := client.Get(key); err != nil {
					return
				}
				atomic.AddInt64(&success2, 1)
			}
		}(w)
	}
	wg2.Wait()
	elapsed2 := time.Since(start2)
	fmt.Printf("Throughput: %.2f ops/sec\n", float64(atomic.LoadInt64(&success2))/elapsed2.Seconds())
	
	fmt.Println("\n=== SET+GET Performance ===")
	var wg3 sync.WaitGroup
	var success3 int64
	
	start3 := time.Now()
	for w := 0; w < numWorkers; w++ {
		wg3.Add(1)
		go func(workerID int) {
			defer wg3.Done()
			for i := 0; i < numOps/numWorkers; i++ {
				key := fmt.Sprintf("both-%d-%d", workerID, i)
				value := []byte(fmt.Sprintf("value-%d", i))
				item := &blazecache.Item{Key: key, Value: value}
				if err := client.Set(item); err != nil {
					return
				}
				if _, err := client.Get(key); err != nil {
					return
				}
				atomic.AddInt64(&success3, 1)
			}
		}(w)
	}
	wg3.Wait()
	elapsed3 := time.Since(start3)
	fmt.Printf("Throughput: %.2f ops/sec\n", float64(atomic.LoadInt64(&success3))/elapsed3.Seconds())
}
