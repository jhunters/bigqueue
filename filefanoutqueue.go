package bigqueue

import (
	"os"
	"strconv"
	"sync"
)

const (
	// FanoutFrontFileName Fanout FrontFileName file name
	FanoutFrontFileName = "front_index_"
)

// FileFanoutQueue file fanout queue implements
type FileFanoutQueue struct {
	fileQueue *FileQueue

	// queue options
	options *Options

	// front index by fanout id
	frontIndexMap map[int64]*QueueFront

	queueGetLock sync.Mutex

	path string
}

// QueueFront queue front struct
type QueueFront struct {
	// fanout id
	fanoutID int64

	// front index of the big queue,
	frontIndex int64

	fanoutDatafile *DB

	// locks for queue front write management
	queueFrontWriteLock sync.Mutex
}

// Open the queue files
func (q *FileFanoutQueue) Open(dir string, queueName string, options *Options) error {

	q.fileQueue = &FileQueue{}

	if options == nil {
		options = DefaultOptions
	}
	err := q.fileQueue.Open(dir, queueName, options)
	if err != nil {
		return err
	}
	q.options = options

	q.path = dir + "/" + queueName

	q.frontIndexMap = make(map[int64]*QueueFront)

	return nil
}

// IsEmpty test if target fanoutID is empty
func (q *FileFanoutQueue) IsEmpty(fanoutID int64) bool {
	qf, err := q.getQueueFront(fanoutID)
	if err != nil {
		return true
	}

	return q.fileQueue.isEmpty(qf.frontIndex)
}

// Size return item size with target fanoutID
func (q *FileFanoutQueue) Size(fanoutID int64) int64 {
	qf, err := q.getQueueFront(fanoutID)
	if err != nil {
		return -1
	}

	return q.fileQueue.size(qf.frontIndex)
}

// Close free the resource
func (q *FileFanoutQueue) Close() {
	q.fileQueue.Close()

	for _, v := range q.frontIndexMap {
		v.fanoutDatafile.Close()
	}
}

// Enqueue Append an item to the queue and return index no
func (q *FileFanoutQueue) Enqueue(data []byte) (int64, error) {
	return q.fileQueue.Enqueue(data)
}

// Dequeue dequeue data from target fanoutID
func (q *FileFanoutQueue) Dequeue(fanoutID int64) (int64, []byte, error) {
	qf, err := q.getQueueFront(fanoutID)
	if err != nil {
		return -1, nil, err
	}

	index, err := qf.updateQueueFrontIndex(1)
	if err != nil {
		return -1, nil, err
	}

	data, err := q.fileQueue.peek(index)
	if err != nil {
		return -1, nil, err
	}

	return index, data, nil
}

// Peek peek the head item from target fanoutID
func (q *FileFanoutQueue) Peek(fanoutID int64) (int64, []byte, error) {
	qf, err := q.getQueueFront(fanoutID)
	if err != nil {
		return -1, nil, err
	}

	index := qf.frontIndex

	data, err := q.fileQueue.peek(index)
	if err != nil {
		return -1, nil, err
	}

	return index, data, nil
}

// Skip To skip deqeue target number of items
func (q *FileFanoutQueue) Skip(fanoutID int64, count int64) error {
	if count <= 0 {
		// do nothing
		return nil
	}
	qf, err := q.getQueueFront(fanoutID)
	if err != nil {
		return err
	}

	_, err = qf.updateQueueFrontIndex(count)
	if err != nil {
		return err
	}

	return nil
}

func (q *FileFanoutQueue) getQueueFront(fanoutID int64) (*QueueFront, error) {
	q.queueGetLock.Lock()
	defer q.queueGetLock.Unlock()
	qf := q.frontIndexMap[fanoutID]
	if qf != nil {
		return qf, nil
	}

	qf = &QueueFront{
		fanoutID:   fanoutID,
		frontIndex: 0,
	}
	err := qf.open(q.path)
	if err != nil {
		return nil, err
	}
	q.frontIndexMap[fanoutID] = qf
	return qf, nil
}

func (q *QueueFront) open(path string) error {

	frontFilePath := path + "/" + FanoutFrontFileName + strconv.Itoa(int(q.fanoutID)) + "/"
	err := os.MkdirAll(frontFilePath, os.ModeDir)
	if err != nil {
		return err
	}

	// create index file
	q.fanoutDatafile = &DB{
		path:            frontFilePath + GetFileName(filePrefix, fileSuffix, q.fanoutID),
		InitialMmapSize: defaultFrontPageSize,
		opened:          true,
	}

	err = q.fanoutDatafile.Open(0666)
	if err != nil {
		return err
	}
	q.frontIndex = BytesToInt(q.fanoutDatafile.data[:defaultFrontPageSize])
	Assert(q.frontIndex >= 0, "front index can not be negetive number. value is %v", q.frontIndex)

	return nil
}

func (q *QueueFront) updateQueueFrontIndex(count int64) (int64, error) {
	q.queueFrontWriteLock.Lock()
	defer q.queueFrontWriteLock.Unlock()

	queueFrontIndex := q.frontIndex
	nextQueueFrontIndex := queueFrontIndex

	if nextQueueFrontIndex == MaxInt64 {
		nextQueueFrontIndex = 0
	} else {
		nextQueueFrontIndex += count
	}
	q.frontIndex = nextQueueFrontIndex

	bb := IntToBytes(q.frontIndex)
	for idx, b := range bb {
		q.fanoutDatafile.data[idx] = b

	}

	return queueFrontIndex, nil
}
