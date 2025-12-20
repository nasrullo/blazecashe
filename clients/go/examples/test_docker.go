package main

import (
	"fmt"
	"log"
	"time"

	blazecache "github.com/blazecache/client"
)

func main() {
	fmt.Println("=== Testing Go Client against Docker (port 6791) ===\n")

	client, err := blazecache.New("127.0.0.1:6791")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	tests := []struct {
		name string
		test func() error
	}{
		{"Ping", func() error { return client.Ping() }},
		{"Set", func() error { return client.Set("docker-key", []byte("docker-value"), 0) }},
		{"Get", func() error {
			val, err := client.Get("docker-key")
			if err != nil {
				return err
			}
			if string(val) != "docker-value" {
				return fmt.Errorf("value mismatch")
			}
			return nil
		}},
		{"Set with TTL", func() error { return client.Set("docker-ttl", []byte("ttl-value"), 60) }},
		{"Delete", func() error { return client.Delete("docker-key") }},
	}

	for i, t := range tests {
		start := time.Now()
		err := t.test()
		elapsed := time.Since(start)
		if err != nil {
			fmt.Printf("%d. %s: ✗ Failed: %v (took %v)\n", i+1, t.name, err, elapsed)
		} else {
			fmt.Printf("%d. %s: ✓ Success (took %v)\n", i+1, t.name, elapsed)
		}
	}

	fmt.Println("\n=== All Docker Tests Completed ===")
}
