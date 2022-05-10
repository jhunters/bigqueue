/*
 * @Author: Malin Xie
 * @Description:
 * @Date: 2020-08-28 20:06:46
 */
package bigqueue

// Queue inteface to define the all necessary functions
type Queue interface {
	// Determines whether a queue is empty
	// return ture if empty, false otherwise
	IsEmpty() bool

	// return avaiable queue size
	Size() int64

	// Append an item to the queue and return index no
	// if any error ocurres a non-nil error returned
	Enqueue(data []byte) (int64, error)

	EnqueueAsync(data []byte, fn func(int64, error))

	Dequeue() (int64, []byte, error)

	Peek() (int64, []byte, error)

	// To skip deqeue target number of items
	Skip(count int64) error

	Close() error

	// Delete all used data files to free disk space.
	Gc() error

	// Set to asynchous subscribe
	Subscribe(fn func(int64, []byte, error)) error

	// to free asynchous subscribe
	FreeSubscribe()
}

// I/O queue inteface to define the all necessary functions
type IOQueue interface {
	Queue

	// Open queue from file io info
	Open(dir string, queueName string, options *Options) error
}

// RemoteQueue remote server queue inteface to define  the all necessary functions
type RemoteQueue interface {
	Queue

	// Open queue from remote server
	Open(serverUrl string, queueName string)
}
