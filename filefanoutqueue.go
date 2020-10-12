package bigqueue

import (
	"errors"
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

	opened bool
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

	subscribLock sync.Mutex

	subscriber func(int64, []byte, error)
}

// Open the queue files
func (q *FileFanoutQueue) Open(dir string, queueName string, options *Options) error {

	if !q.opened {
		q.opened = true
	} else {
		return errors.New("FileFanoutQueue already opened")
	}

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

	q.fileQueue.Subscribe(q.doSubscribe)

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
	// close file queue
	q.fileQueue.Close()

	for _, v := range q.frontIndexMap {
		if v.fanoutDatafile != nil {
			v.fanoutDatafile.Close()
		}
		v.fanoutDatafile = nil
	}

	q.opened = false
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

// Subscribe do async subscribe by target fanout id
func (q *FileFanoutQueue) Subscribe(fanoutID int64, fn func(int64, []byte, error)) error {
	if fn == nil {
		return errors.New("parameter 'fn' is nil")
	}
	qf, err := q.getQueueFront(fanoutID)
	if err != nil {
		return err
	}

	if qf.subscriber != nil {
		return ErrSubscribeExistErr
	}

	qf.subscriber = fn

	return nil
}

// FreeSubscribe to free subscriber by target fanout id
func (q *FileFanoutQueue) FreeSubscribe(fanoutID int64) {
	qf, err := q.getQueueFront(fanoutID)
	if err != nil {
		return
	}
	qf.subscriber = nil
}

// FreeAllSubscribe to free all subscriber
func (q *FileFanoutQueue) FreeAllSubscribe() {
	for _, qf := range q.frontIndexMap {
		qf.subscriber = nil
	}

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

	err = q.fanoutDatafile.Open(defaultFileMode)
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
	if nextQueueFrontIndex < 0 {
		// if overflow then reset to zero
		nextQueueFrontIndex = 0
	}

	q.frontIndex = nextQueueFrontIndex

	bb := IntToBytes(q.frontIndex)
	for idx, b := range bb {
		q.fanoutDatafile.data[idx] = b

	}

	return queueFrontIndex, nil
}

func (q *FileFanoutQueue) doSubscribe(index int64, data []byte, err error) {
	for fanoutID, v := range q.frontIndexMap {
		if v.subscriber != nil {
			v.subscribLock.Lock()
			defer v.subscribLock.Unlock()
			// here should be care something about blocked callback subscriber
			q.doLoopSubscribe(fanoutID, v.subscriber)
		}

	}
}

func (q *FileFanoutQueue) doLoopSubscribe(fanoutID int64, subscriber func(int64, []byte, error)) {
	if subscriber == nil {
		return
	}
	for {
		index, bb, err := q.Dequeue(fanoutID)
		if bb == nil || len(bb) == 0 {
			break // queue is empty
		}
		subscriber(index, bb, err)
	}

}
