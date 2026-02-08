package main

import (
	"context"
	"log"
	"time"

	"github.com/pawan-87/MyKVStore/client"
)

func main() {

	myKVStoreClient, err := client.New("127.0.0.1:2379")
	if err != nil {
		log.Fatalf("clouldn't start the client: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	key := "myfirstKey"
	value := "value"

	resp, err := myKVStoreClient.KV.Put(ctx, key, value)
	if err != nil {
		log.Fatalf("couldn't add key-value: %v", err)
	}
	log.Printf("Aded key-value: response:%v", resp.String())

	time.Sleep(15 * time.Second)
}
