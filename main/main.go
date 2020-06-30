package main

import (
	"fmt"
	"strconv"

	"github.com/jhunters/bigqueue"
)

// a demo to show how to enqueue and dequeue data
func main() {
	var queue = new(bigqueue.FileQueue)

	// use custom options
	var DefaultOptions = &bigqueue.Options{
		DataPageSize:      bigqueue.DefaultDataPageSize,
		GcLock:            false,
		IndexItemsPerPage: bigqueue.DefaultIndexItemsPerPage,
	}

	// open queue files
	err := queue.Open("./bin", "testqueue", DefaultOptions)

	if err != nil {
		fmt.Println(err)
	}
	defer queue.Close()

	// do enqueue
	for i := 1; i < 10; i++ {
		data := []byte("hello jhunters" + strconv.Itoa(i))
		i, err := queue.Enqueue(data)
		if err != nil {
			fmt.Println(err)
		} else {
			fmt.Println("Enqueued index=", i, string(data))
		}
	}
	// do dequeue
	for i := 1; i < 10; i++ {
		index, bb, err := queue.Dequeue()
		if err != nil {
			fmt.Println(err)
		}
		if index != -1 {
			fmt.Println(index, string(bb))
		}

	}

	// do gc action to free old data
	queue.Gc()

}
