package main

import (
	"fmt"
	"log"
	blazecache "github.com/blazecache/client"
)

func main() {
	client, err := blazecache.New("127.0.0.1:6784")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	
	fmt.Println("Testing PUT...")
	err = client.Set("test", []byte("value"), 0)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		log.Fatal(err)
	}
	fmt.Println("PUT successful!")
}
