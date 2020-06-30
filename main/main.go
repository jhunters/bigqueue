package main

import (
	"fmt"
	"strconv"

	"github.com/jhunters/bigqueue"
)

func main() {
	var queue = new(bigqueue.FileQueue)

	var DefaultOptions = &bigqueue.Options{
		DataPageSize:      bigqueue.DefaultDataPageSize,
		GcLock:            false,
		IndexItemsPerPage: bigqueue.DefaultIndexItemsPerPage,
	}

	err := queue.Open(".", "testqueue", DefaultOptions)

	if err != nil {
		fmt.Println(err)
	}
	for i := 1; i < 2; i++ {
		data := []byte("hello xiemalin" + strconv.Itoa(i))
		i, err := queue.Enqueue(data)
		if err != nil {
			fmt.Println(err)
		} else {
			fmt.Println("Enqueued index=", i, string(data))
		}
	}
	//
	for i := 1; i < 2; i++ {
		index, bb, err := queue.Dequeue()
		if err != nil {
			fmt.Println(err)
		}
		if index != -1 {
			fmt.Println(index, string(bb))
		}

	}

	queue.Gc()

	queue.Close()

}
