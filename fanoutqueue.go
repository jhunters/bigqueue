package bigqueue

// FanOutQueue queue supports with pub-sub feature
type FanOutQueue interface {
	// Open to open target file if failed error returns
	Open(dir string, queueName string, options *Options) error

	// IsEmpty Determines whether a queue is empty
	//fanoutId queue index
	// return ture if empty, false otherwise
	IsEmpty(fanoutID int64) bool

	// Size return avaiable queue size
	Size(fanoutID int64) int64

	// Enqueue Append an item to the queue and return index no
	// if any error ocurres a non-nil error returned
	Enqueue(data []byte) (int64, error)

	// EnqueueAsync Append an item to the queue async way
	EnqueueAsync(data []byte, fn func(int64, error))

	Dequeue(fanoutID int64) (int64, []byte, error)

	Peek(fanoutID int64) (int64, []byte, error)

	// To skip deqeue target number of items
	Skip(fanoutID int64, count int64) error

	Close() error

	// Set to asynchous subscribe
	Subscribe(fanoutID int64, fn func(int64, []byte, error)) error

	// to free asynchous subscribe
	FreeSubscribe(fanoutID int64)

	// FreeAllSubscribe to free all asynchous subscribe
	FreeAllSubscribe()
}
