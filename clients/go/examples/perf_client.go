package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	blazecache "github.com/blazecache/client"
)

func main() {
	serverAddr := flag.String("server", "127.0.0.1:6793", "UDP server address")
	concurrency := flag.Int("concurrency", 100, "Number of concurrent operations")
	valueSize := flag.Int64("value-size", 1024, "Value size in bytes")
	duration := flag.Int("duration", 60, "Test duration in seconds")
	interval := flag.Int("interval", 1, "Stats reporting interval in seconds")
	flag.Parse()

	fmt.Printf("=== Go UDP Client Performance Test ===\n")
	fmt.Printf("Server: %s\n", *serverAddr)
	fmt.Printf("Concurrency: %d\n", *concurrency)
	fmt.Printf("Value size: %d bytes\n", *valueSize)
	fmt.Printf("Duration: %d seconds\n", *duration)
	fmt.Printf("Interval: %d seconds\n\n", *interval)

	// Verify server connection with retries (similar to Rust client fix)
	fmt.Println("Waiting for server to be ready...")
	var testClient *blazecache.UDPClient
	var err error
	serverReady := false
	for attempt := 1; attempt <= 10; attempt++ {
		time.Sleep(500 * time.Millisecond)
		testClient, err = blazecache.NewUDPClient(*serverAddr)
		if err != nil {
			fmt.Printf("Connection failed on attempt %d: %v (retrying...)\n", attempt, err)
			continue
		}
		
		// Try ping with timeout
		pingDone := make(chan error, 1)
		go func() {
			pingDone <- testClient.Ping()
		}()
		
		select {
		case err := <-pingDone:
			if err == nil {
				fmt.Printf("✓ Server is ready (attempt %d)\n\n", attempt)
				serverReady = true
				break
			}
			fmt.Printf("Ping failed on attempt %d: %v (retrying...)\n", attempt, err)
			testClient.Close()
		case <-time.After(2 * time.Second):
			fmt.Printf("Ping timeout on attempt %d (retrying...)\n", attempt)
			testClient.Close()
		}
		
		if serverReady {
			break
		}
	}
	
	if !serverReady {
		log.Fatalf("Server not ready after 10 attempts. Please ensure the server is running on %s", *serverAddr)
	}
	defer testClient.Close()

	start := time.Now()
	var wg sync.WaitGroup
	var totalOps int64
	var totalErrors int64
	var putLatencies []time.Duration
	var getLatencies []time.Duration
	var mu sync.Mutex

	// Stats reporting goroutine
	statsTicker := time.NewTicker(time.Duration(*interval) * time.Second)
	done := make(chan bool)
	go func() {
		for {
			select {
			case <-done:
				return
			case <-statsTicker.C:
				mu.Lock()
				ops := atomic.LoadInt64(&totalOps)
				errors := atomic.LoadInt64(&totalErrors)
				elapsed := time.Since(start).Seconds()
				rps := float64(ops) / elapsed
				mu.Unlock()

				fmt.Printf("[Stats] Ops: %d, Errors: %d, RPS: %.2f, Elapsed: %.2fs\n",
					ops, errors, rps, elapsed)
			}
		}
	}()

	// Signal handler
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Run operations
	value := make([]byte, *valueSize)
	for i := range value {
		value[i] = byte(i % 256)
	}

	requestID := int64(0)
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			client, err := blazecache.NewUDPClient(*serverAddr)
			if err != nil {
				atomic.AddInt64(&totalErrors, 1)
				return
			}
			defer client.Close()

			for {
				select {
				case <-sigChan:
					return
				default:
					id := atomic.AddInt64(&requestID, 1)
					key := fmt.Sprintf("perf-key-%d", id)

					// PUT operation
					putStart := time.Now()
					if err := client.Set(key, value, 3600); err != nil {
						atomic.AddInt64(&totalErrors, 1)
						continue
					}
					putLatency := time.Since(putStart)

					// GET operation
					getStart := time.Now()
					_, err := client.Get(key)
					getLatency := time.Since(getStart)
					if err != nil {
						atomic.AddInt64(&totalErrors, 1)
						continue
					}

					atomic.AddInt64(&totalOps, 1)

					mu.Lock()
					putLatencies = append(putLatencies, putLatency)
					getLatencies = append(getLatencies, getLatency)
					if len(putLatencies) > 10000 {
						putLatencies = putLatencies[len(putLatencies)-1000:]
						getLatencies = getLatencies[len(getLatencies)-1000:]
					}
					mu.Unlock()

					// Check duration
					if time.Since(start) > time.Duration(*duration)*time.Second {
						return
					}
				}
			}
		}()
	}

	// Wait for duration or signal
	select {
	case <-sigChan:
		fmt.Println("\nInterrupted, shutting down...")
	case <-time.After(time.Duration(*duration) * time.Second):
		fmt.Println("\nTest duration completed")
	}

	statsTicker.Stop()
	done <- true
	wg.Wait()

	// Final stats
	elapsed := time.Since(start)
	ops := atomic.LoadInt64(&totalOps)
	errors := atomic.LoadInt64(&totalErrors)
	rps := float64(ops) / elapsed.Seconds()

	fmt.Printf("\n=== Final Results ===\n")
	fmt.Printf("Total operations: %d\n", ops)
	fmt.Printf("Errors: %d\n", errors)
	fmt.Printf("Time elapsed: %v\n", elapsed)
	fmt.Printf("Throughput: %.2f ops/sec\n", rps)

	if len(putLatencies) > 0 {
		mu.Lock()
		avgPut := averageDuration(putLatencies)
		avgGet := averageDuration(getLatencies)
		mu.Unlock()
		fmt.Printf("Avg PUT latency: %v\n", avgPut)
		fmt.Printf("Avg GET latency: %v\n", avgGet)
	}
}

func averageDuration(durations []time.Duration) time.Duration {
	if len(durations) == 0 {
		return 0
	}
	var sum time.Duration
	for _, d := range durations {
		sum += d
	}
	return sum / time.Duration(len(durations))
}

