package main

import (
	"fmt"
	"log"
	"sort"
	"time"
	blazecache "github.com/blazecache/client"
)

func main() {
	serverAddr := "127.0.0.1:6792"
	
	fmt.Printf("=== Network Latency Measurement ===\n")
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
	
	// Measure PING latency
	fmt.Println("=== PING Latency ===")
	pingLatencies := make([]time.Duration, 100)
	for i := 0; i < 100; i++ {
		start := time.Now()
		if err := client.Ping(); err != nil {
			log.Fatalf("Ping failed: %v", err)
		}
		pingLatencies[i] = time.Since(start)
	}
	printStats("PING", pingLatencies)
	
	// Measure SET latency
	fmt.Println("\n=== SET Latency ===")
	setLatencies := make([]time.Duration, 100)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("latency-test-key-%d", i)
		value := []byte(fmt.Sprintf("value-%d", i))
		item := &blazecache.Item{Key: key, Value: value}
		
		start := time.Now()
		if err := client.Set(item); err != nil {
			log.Fatalf("Set failed: %v", err)
		}
		setLatencies[i] = time.Since(start)
	}
	printStats("SET", setLatencies)
	
	// Measure GET latency (warm cache)
	fmt.Println("\n=== GET Latency (cache hit) ===")
	getLatencies := make([]time.Duration, 100)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("latency-test-key-%d", i)
		
		start := time.Now()
		result, err := client.Get(key)
		if err != nil {
			log.Fatalf("Get failed: %v", err)
		}
		if result == nil {
			log.Fatalf("Get returned nil")
		}
		getLatencies[i] = time.Since(start)
		_ = result
	}
	printStats("GET (hit)", getLatencies)
	
	// Measure GET latency (cache miss)
	fmt.Println("\n=== GET Latency (cache miss) ===")
	getMissLatencies := make([]time.Duration, 100)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("latency-test-miss-%d", i)
		
		start := time.Now()
		_, err := client.Get(key)
		if err == nil {
			log.Fatalf("Expected error for missing key")
		}
		getMissLatencies[i] = time.Since(start)
	}
	printStats("GET (miss)", getMissLatencies)
	
	// Measure SET+GET combined latency
	fmt.Println("\n=== SET+GET Combined Latency ===")
	combinedLatencies := make([]time.Duration, 100)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("latency-test-combined-%d", i)
		value := []byte(fmt.Sprintf("value-%d", i))
		item := &blazecache.Item{Key: key, Value: value}
		
		start := time.Now()
		if err := client.Set(item); err != nil {
			log.Fatalf("Set failed: %v", err)
		}
		result, err := client.Get(key)
		if err != nil {
			log.Fatalf("Get failed: %v", err)
		}
		if result == nil {
			log.Fatalf("Get returned nil")
		}
		combinedLatencies[i] = time.Since(start)
		_ = result
	}
	printStats("SET+GET", combinedLatencies)
	
	fmt.Println("\n=== Summary ===")
	fmt.Printf("PING RTT:     %s\n", formatDuration(median(pingLatencies)))
	fmt.Printf("SET RTT:      %s\n", formatDuration(median(setLatencies)))
	fmt.Printf("GET (hit) RTT: %s\n", formatDuration(median(getLatencies)))
	fmt.Printf("GET (miss) RTT: %s\n", formatDuration(median(getMissLatencies)))
	fmt.Printf("SET+GET RTT:  %s\n", formatDuration(median(combinedLatencies)))
}

func printStats(name string, latencies []time.Duration) {
	sorted := make([]time.Duration, len(latencies))
	copy(sorted, latencies)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})
	
	min := sorted[0]
	max := sorted[len(sorted)-1]
	median := sorted[len(sorted)/2]
	p95 := sorted[int(float64(len(sorted))*0.95)]
	p99 := sorted[int(float64(len(sorted))*0.99)]
	
	var sum time.Duration
	for _, l := range latencies {
		sum += l
	}
	avg := sum / time.Duration(len(latencies))
	
	fmt.Printf("  Min:    %s\n", formatDuration(min))
	fmt.Printf("  Max:    %s\n", formatDuration(max))
	fmt.Printf("  Avg:    %s\n", formatDuration(avg))
	fmt.Printf("  Median: %s\n", formatDuration(median))
	fmt.Printf("  P95:    %s\n", formatDuration(p95))
	fmt.Printf("  P99:    %s\n", formatDuration(p99))
}

func median(durations []time.Duration) time.Duration {
	sorted := make([]time.Duration, len(durations))
	copy(sorted, durations)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})
	return sorted[len(sorted)/2]
}

func formatDuration(d time.Duration) string {
	if d < time.Microsecond {
		return fmt.Sprintf("%.2fns", float64(d.Nanoseconds()))
	} else if d < time.Millisecond {
		return fmt.Sprintf("%.2fµs", float64(d.Nanoseconds())/1000.0)
	} else {
		return fmt.Sprintf("%.2fms", float64(d.Nanoseconds())/1000000.0)
	}
}

