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
	
	fmt.Println("=== SET Performance: Round-Robin vs Consistent Hashing ===")
	fmt.Printf("Server: %s\n\n", serverAddr)
	
	// Test 1: Round-Robin (default)
	fmt.Println("=== Test 1: Round-Robin Strategy ===")
	client1, err := blazecache.New(serverAddr)
	if err != nil {
		log.Fatalf("Failed: %v", err)
	}
	
	// Warm up
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("warmup-rr-%d", i)
		value := []byte("warmup")
		item := &blazecache.Item{Key: key, Value: value}
		client1.Set(item)
	}
	
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
				key := fmt.Sprintf("rr-%d-%d", workerID, i)
				value := []byte(fmt.Sprintf("value-%d", i))
				item := &blazecache.Item{Key: key, Value: value}
				if err := client1.Set(item); err != nil {
					continue
				}
				atomic.AddInt64(&success1, 1)
			}
		}(w)
	}
	wg1.Wait()
	elapsed1 := time.Since(start1)
	throughput1 := float64(atomic.LoadInt64(&success1)) / elapsed1.Seconds()
	fmt.Printf("Throughput: %.2f ops/sec\n", throughput1)
	fmt.Printf("Time: %s\n", elapsed1)
	
	// Test 2: Consistent Hashing
	fmt.Println("\n=== Test 2: Consistent Hashing Strategy ===")
	client2, err := blazecache.New(serverAddr)
	if err != nil {
		log.Fatalf("Failed: %v", err)
	}
	client2.WithStrategy(blazecache.ConsistentHashing)
	
	// Warm up
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("warmup-ch-%d", i)
		value := []byte("warmup")
		item := &blazecache.Item{Key: key, Value: value}
		client2.Set(item)
	}
	
	var wg2 sync.WaitGroup
	var success2 int64
	
	start2 := time.Now()
	for w := 0; w < numWorkers; w++ {
		wg2.Add(1)
		go func(workerID int) {
			defer wg2.Done()
			for i := 0; i < numOps/numWorkers; i++ {
				key := fmt.Sprintf("ch-%d-%d", workerID, i)
				value := []byte(fmt.Sprintf("value-%d", i))
				item := &blazecache.Item{Key: key, Value: value}
				if err := client2.Set(item); err != nil {
					continue
				}
				atomic.AddInt64(&success2, 1)
			}
		}(w)
	}
	wg2.Wait()
	elapsed2 := time.Since(start2)
	throughput2 := float64(atomic.LoadInt64(&success2)) / elapsed2.Seconds()
	fmt.Printf("Throughput: %.2f ops/sec\n", throughput2)
	fmt.Printf("Time: %s\n", elapsed2)
	
	// Comparison
	fmt.Println("\n=== Comparison ===")
	fmt.Printf("Round-Robin:      %.2f ops/sec\n", throughput1)
	fmt.Printf("Consistent Hash:  %.2f ops/sec\n", throughput2)
	if throughput2 > throughput1 {
		improvement := ((throughput2 - throughput1) / throughput1) * 100
		fmt.Printf("Improvement: +%.2f%%\n", improvement)
	} else {
		decline := ((throughput1 - throughput2) / throughput1) * 100
		fmt.Printf("Decline: -%.2f%%\n", decline)
	}
	
	// Test 3: Individual SET timing with consistent hashing
	fmt.Println("\n=== Test 3: Individual SET Timing (Consistent Hashing) ===")
	var times []time.Duration
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("timing-ch-%d", i)
		value := []byte(fmt.Sprintf("value-%d", i))
		item := &blazecache.Item{Key: key, Value: value}
		
		start := time.Now()
		if err := client2.Set(item); err != nil {
			log.Fatalf("Set failed: %v", err)
		}
		times = append(times, time.Since(start))
	}
	
	var sum time.Duration
	var min, max time.Duration = times[0], times[0]
	for _, t := range times {
		sum += t
		if t < min {
			min = t
		}
		if t > max {
			max = t
		}
	}
	avg := sum / time.Duration(len(times))
	
	fmt.Printf("Min: %s\n", min)
	fmt.Printf("Max: %s\n", max)
	fmt.Printf("Avg: %s\n", avg)
	fmt.Printf("Throughput: %.2f ops/sec\n", 1000.0/sum.Seconds())
}

