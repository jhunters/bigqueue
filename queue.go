package bigqueue

type Queue interface {
	Open(dir string, queueName string, options *Options) error

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
}
