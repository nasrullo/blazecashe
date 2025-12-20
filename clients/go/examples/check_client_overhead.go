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
	
	fmt.Printf("=== Client-Side Overhead Analysis ===\n")
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
	
	// Measure individual operation times
	fmt.Println("=== Individual Operation Timing ===")
	
	// Warm up
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("warmup-%d", i)
		value := []byte("warmup")
		item := &blazecache.Item{Key: key, Value: value}
		client.Set(item)
	}
	
	// Measure SET operations
	setTimes := make([]time.Duration, 1000)
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("timing-key-%d", i)
		value := []byte(fmt.Sprintf("value-%d", i))
		item := &blazecache.Item{Key: key, Value: value}
		
		start := time.Now()
		if err := client.Set(item); err != nil {
			log.Fatalf("Set failed: %v", err)
		}
		setTimes[i] = time.Since(start)
	}
	
	// Calculate statistics
	var sum time.Duration
	var min, max time.Duration = setTimes[0], setTimes[0]
	for _, t := range setTimes {
		sum += t
		if t < min {
			min = t
		}
		if t > max {
			max = t
		}
	}
	avg := sum / time.Duration(len(setTimes))
	
	fmt.Printf("SET operations (1000 samples):\n")
	fmt.Printf("  Min:    %s\n", min)
	fmt.Printf("  Max:    %s\n", max)
	fmt.Printf("  Avg:    %s\n", avg)
	fmt.Printf("  Total:  %s\n", sum)
	
	// Compare with latency measurement (should be similar)
	fmt.Println("\n=== Comparison with Latency Measurement ===")
	fmt.Println("From latency measurement:")
	fmt.Println("  SET RTT: ~49µs (median)")
	fmt.Printf("  Current avg: %s\n", avg)
	
	if avg > 100*time.Microsecond {
		fmt.Println("\n⚠️  WARNING: Average operation time is much higher than network RTT!")
		fmt.Printf("   Overhead: %s per operation\n", avg-49*time.Microsecond)
		fmt.Println("   Possible causes:")
		fmt.Println("   - Lock contention in connection pool")
		fmt.Println("   - Serialization overhead")
		fmt.Println("   - Connection pool lookup overhead")
	} else {
		fmt.Println("\n✓ Operation time is close to network RTT.")
		fmt.Println("  Client-side overhead is minimal.")
	}
	
	// Measure under load
	fmt.Println("\n=== Operation Time Under Load ===")
	fmt.Println("Running 1000 operations with 100 concurrent workers...")
	
	var (
		successCount int64
		wg           sync.WaitGroup
		loadTimes    = make([]time.Duration, 1000)
		timeIndex    int64
	)
	
	start := time.Now()
	for w := 0; w < 100; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				key := fmt.Sprintf("load-key-%d-%d", workerID, i)
				value := []byte(fmt.Sprintf("value-%d-%d", workerID, i))
				item := &blazecache.Item{Key: key, Value: value}
				
				opStart := time.Now()
				if err := client.Set(item); err != nil {
					continue
				}
				opTime := time.Since(opStart)
				
				idx := atomic.AddInt64(&timeIndex, 1) - 1
				if idx < int64(len(loadTimes)) {
					loadTimes[idx] = opTime
				}
				atomic.AddInt64(&successCount, 1)
			}
		}(w)
	}
	
	wg.Wait()
	totalTime := time.Since(start)
	
	// Calculate statistics under load
	var loadSum time.Duration
	var loadMin, loadMax time.Duration = loadTimes[0], loadTimes[0]
	validTimes := 0
	for _, t := range loadTimes {
		if t > 0 {
			loadSum += t
			validTimes++
			if t < loadMin {
				loadMin = t
			}
			if t > loadMax {
				loadMax = t
			}
		}
	}
	loadAvg := loadSum / time.Duration(validTimes)
	
	fmt.Printf("Operations under load (%d samples):\n", validTimes)
	fmt.Printf("  Min:    %s\n", loadMin)
	fmt.Printf("  Max:    %s\n", loadMax)
	fmt.Printf("  Avg:    %s\n", loadAvg)
	fmt.Printf("  Total time: %s\n", totalTime)
	fmt.Printf("  Throughput: %.2f ops/sec\n", float64(successCount)/totalTime.Seconds())
	
	// Compare idle vs load
	fmt.Println("\n=== Idle vs Load Comparison ===")
	fmt.Printf("Idle avg:    %s\n", avg)
	fmt.Printf("Load avg:    %s\n", loadAvg)
	slowdown := loadAvg.Seconds() / avg.Seconds()
	fmt.Printf("Slowdown:    %.2fx\n", slowdown)
	
	if slowdown > 2.0 {
		fmt.Println("\n⚠️  WARNING: Operations are significantly slower under load!")
		fmt.Println("   This suggests:")
		fmt.Println("   - Lock contention")
		fmt.Println("   - Connection pool exhaustion")
		fmt.Println("   - Resource competition")
	} else {
		fmt.Println("\n✓ Operations maintain similar performance under load.")
	}
}

