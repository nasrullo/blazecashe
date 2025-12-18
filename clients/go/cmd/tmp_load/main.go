package main

import (
	"fmt"
	"log"
	"time"

	bc "github.com/blazecache/client"
)

func main() {
	client, err := bc.New(
		"127.0.0.1:6784",
		"127.0.0.1:6786",
		"127.0.0.1:6788",
	)
	if err != nil {
		log.Fatalf("client init failed: %v", err)
	}

	start := time.Now()
	success := 0

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("load_put_%d", i)
		if err := client.Set(&bc.Item{Key: key, Value: []byte("value")}); err != nil {
			log.Printf("put %d failed: %v", i, err)
		} else {
			success++
		}
	}

	fmt.Printf("puts ok: %d/10, duration: %v\n", success, time.Since(start))
}
