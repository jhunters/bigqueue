package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/jhunters/bigqueue"
)

func main2() {
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
	for i := 1; i < 100; i++ {
		data := []byte("hello jhunters" + strconv.Itoa(i))
		queue.EnqueueAsync(data, func(index int64, err error) {
			if err != nil {
				fmt.Println(err)
			} else {
				fmt.Println("Enqueued index=", index, string(data))
			}
			idx, bb, err := queue.Dequeue()
			if err != nil {
				fmt.Println(err)
			}
			fmt.Println(idx, string(bb))
		})
	}

	time.Sleep(time.Duration(2) * time.Second)

	queue.Gc()

	queue.Close()

}
