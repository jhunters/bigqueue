<h1 align="center">BigQueue-go tutorial</h1>

<p align="center">
This is a tutorial to show the basic API usage of big queue.
</p>


## Quick  tutorial:  
**Initialize bigqueue**: <br>
You can create(initialize) a new big queue in just two statements: 
```go
    // new queue struct
	var queue = new(bigqueue.FileQueue)
    // open file with target directory and queue name
	err := queue.Open(".", "testqueue", nil)

```
Initialize with customized options
```go
    // new queue struct
	var queue = new(bigqueue.FileQueue)
	// create customized options
	var options = &bigqueue.Options{
		DataPageSize:      bigqueue.DefaultDataPageSize,
		GcLock:            false,
		IndexItemsPerPage: bigqueue.DefaultIndexItemsPerPage,
	}

	// open file with target directory and queue name
	err := queue.Open(".", "testqueue", options)
```
#### 参数说明:
参数名 |默认值 |  说明 
-|-|-
DataPageSize | 128 * 1024 * 1024 | Number of bytes size in one data page file |
IndexItemsPerPage | 17 |  Number of index item size in one index page file. default is 1 << 17 |

**Enqueue**: <br> 
To add or produce item into the queue, you just call the enqueue method on the queue reference, here we enqueue 10 numbers into the queue:
```go
	for i := 0; i < 10; i++ {
		content := strconv.Itoa(i)
		idx, err := queue.Enqueue([]byte(content))
		if err != nil {
			t.Error("Enqueue failed with err:", err)
		}
	}

```

**Size**: <br>
Now there are 10 items in the queue, and it’s not empty anymore, to find out the total number of items in the queue, call the size method:
```go
	size := queue.Size() // get size 10

```

**IsEmpty**: <br>
Check current queue is empty.
```go
	isEmpty := queue.IsEmpty() 

```

**Peek and Dequeue**: <br>
The peek method just let you peek item at the front of the queue without removing the item from the queue:
```go
	index, data, err := queue.Peek() 
	if err != nil {
		// print err
	}
```

To remove or consume item from the queue, just call the dequeue method, here we dequeue 1 items from the queue:
```go
	index, data, err := queue.Dequeue() 
	if err != nil {
		// print err
	}
```

**Skip**: <br>
The Skip method is to ignore the specified items count from current index.
```go
    count := int64(10)
	err := queue.Skip(count)
	if err != nil {
		// print err
	}
```


**Gc**: <br>
The GC method is to delete old items from index and data page file(s) to free disk usage.
```go
	err := queue.Gc()
	if err != nil {
		// print err
	}
```

**Subscribe and FreeSbuscribe**: <br>
The Subscribe method is dequeue item from queue in asynchouse way. like listener pattern.
```go
	queue.Subscribe(func(index int64, bb []byte, err error) {
		if err != nil {
			//  we met some error
		}
		// on item dequeued with item index and item data
	})

	// free subscribe action
	queue.FreeSubscribe()
```


**Close**: <br>
Finally, when you finish with the queue, just call Close method to release resource used by the queue, this is not mandatory, just a best practice, call close will release part of used memory immediately. Usually, you initialize big queue in a try block and close it in the finally block, here is the usage paradigm:
```go
	err := queue.Close()
	if err != nil {
		// print err
	}
```



## License
BigQueue-Go is [Apache 2.0 licensed](./LICENSE).
