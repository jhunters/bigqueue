package bigqueue

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"sync"
	"time"
)

const (
	// front index page size
	defaultFrontPageSize = 1 << 3
	// meta file page size
	defaultMetaPageSize = 1 << 4
	// DefaultDataPageSize data file size
	DefaultDataPageSize = 128 * 1024 * 1024

	defaultItemLenBits  = 5
	defaultIndexItemLen = 1 << defaultItemLenBits
	// DefaultIndexItemsPerPage items numbers in one page
	DefaultIndexItemsPerPage = 17
	defaultItemsPerPage      = 1 << DefaultIndexItemsPerPage
	// index file size
	defaultIndexPageSize = defaultIndexItemLen * defaultItemsPerPage
	// MaxInt64 max value of int64
	MaxInt64 = 0x7fffffffffffffff
	// IndexFileName file name
	IndexFileName = "index"
	// DataFileName file name
	DataFileName = "data"
	// MetaFileName file name
	MetaFileName = "meta_data"
	// FrontFileName file name
	FrontFileName = "front_index"

	filePrefix = "page-"
	fileSuffix = ".dat"
)

// DefaultOptions default options
var DefaultOptions = &Options{
	DataPageSize:      DefaultDataPageSize,
	indexPageSize:     defaultIndexPageSize,
	IndexItemsPerPage: DefaultIndexItemsPerPage,
	itemsPerPage:      defaultItemsPerPage,
}

// FileQueue queue implements with mapp file
type FileQueue struct {
	// front index of the big queue,
	frontIndex int64

	// head index of the array, this is the read write barrier.
	// readers can only read items before this index, and writes can write this index or after
	headIndex int64

	// tail index of the array,
	// readers can't read items before this tail
	tailIndex int64

	// head index of the data page, this is the to be appended data page index
	headDataPageIndex int64

	// head offset of the data page, this is the to be appended data offset
	headDataItemOffset int64

	// Protects mmap access during remapping.
	// use read and write lock
	lock sync.RWMutex

	// lock for enqueue state management
	enqueueLock sync.Mutex

	// locks for queue front write management
	queueFrontWriteLock sync.Mutex

	path string

	indexFile *DBFactory
	dataFile  *DBFactory
	metaFile  *DB
	frontFile *DB

	// queue options
	options *Options

	// set subscribe action
	subscriber func(int64, []byte, error)

	enqueueChan chan bool

	gcLock sync.Mutex
}

// Open the queue files
func (q *FileQueue) Open(dir string, queueName string, options *Options) error {
	if len(dir) == 0 {
		return errors.New("Parameter 'dir' can not be blank.")
	}

	if len(queueName) == 0 {
		return errors.New("Parameter 'queueName' can not be blank.")
	}

	if options == nil {
		options = DefaultOptions
	}
	q.options = options
	q.options.itemsPerPage = 1 << uint(q.options.IndexItemsPerPage)
	q.options.indexPageSize = defaultIndexItemLen * q.options.itemsPerPage

	path := dir + "/" + queueName

	err := os.MkdirAll(path, os.ModeDir)
	if err != nil {
		return err
	}

	// initialize directories
	q.path = path

	err = q.initDirs()
	if err != nil {
		return err
	}
	err = q.initFrontFile()
	if err != nil {
		return err
	}
	err = q.initMetaFile()
	if err != nil {
		return err
	}

	dataDBFactory := DBFactory{
		filePath:        q.path + "/" + DataFileName,
		filePrefix:      filePrefix,
		fileSuffix:      fileSuffix,
		lockMap:         make(map[int64]*sync.Mutex),
		dbMap:           make(map[int64]*DB),
		InitialMmapSize: q.options.DataPageSize,
	}
	q.dataFile = &dataDBFactory

	indexDBFactory := DBFactory{
		filePath:        q.path + "/" + IndexFileName,
		filePrefix:      filePrefix,
		fileSuffix:      fileSuffix,
		lockMap:         make(map[int64]*sync.Mutex),
		dbMap:           make(map[int64]*DB),
		InitialMmapSize: q.options.indexPageSize,
	}
	q.indexFile = &indexDBFactory

	err = q.initDataPageIndex()
	if err != nil {
		return err
	}

	q.enqueueChan = make(chan bool, 1)
	return nil
}

// IsEmpty to determines whether a queue is empty
func (q *FileQueue) IsEmpty() bool {
	return q.frontIndex >= q.headIndex
}

// isEmpty to determines whether a queue is empty by target frontIndex
func (q *FileQueue) isEmpty(frontIndex int64) bool {
	return frontIndex >= q.headIndex
}

// Size to return total number of items available in the queue.
func (q *FileQueue) Size() int64 {
	sz := q.headIndex - q.frontIndex
	if sz < 0 {
		sz = 0
	}
	return int64(sz)
}

// to calc size by target frontIndex
func (q *FileQueue) size(frontIndex int64) int64 {
	sz := q.headIndex - frontIndex
	if sz < 0 {
		sz = 0
	}
	return int64(sz)
}

// EnqueueAsync adds an item at the queue and HeadIndex will increase
// Asynchouous mode will call back with fn function
func (q *FileQueue) EnqueueAsync(data []byte, fn func(int64, error)) {
	go q.doEnqueueAsync(data, fn)
}

func (q *FileQueue) doEnqueueAsync(data []byte, fn func(int64, error)) {
	index, err := q.Enqueue(data)
	fn(index, err)
}

// Enqueue adds an item at the queue and HeadIndex will increase
func (q *FileQueue) Enqueue(data []byte) (int64, error) {
	sz := len(data)
	if sz == 0 {
		return -1, ErrEnqueueDataNull
	}
	q.lock.RLock()
	defer q.lock.RUnlock()

	q.enqueueLock.Lock()
	defer q.enqueueLock.Unlock()

	// check if have enough space
	if int64(q.headDataItemOffset)+int64(sz) > int64(q.options.DataPageSize) {
		q.headDataPageIndex++
		q.headDataItemOffset = 0
	}

	toAppendDataPageIndex := q.headDataPageIndex
	toAppendDataItemOffset := q.headDataItemOffset

	db, err := q.dataFile.acquireDB(toAppendDataPageIndex)
	if err != nil {
		return -1, err
	}

	// write data
	copy(db.data[toAppendDataItemOffset:toAppendDataItemOffset+int64(sz)], data[0:sz])

	//update to next
	q.headDataItemOffset = q.headDataItemOffset + int64(sz)

	toAppendArrayIndex := q.headIndex
	toAppendIndexPageIndex := toAppendArrayIndex >> uint(q.options.IndexItemsPerPage)

	indexDB, err := q.indexFile.acquireDB(toAppendIndexPageIndex)
	if err != nil {
		return -1, err
	}
	// calc index offset
	toAppendIndexItemOffset := Mod(toAppendArrayIndex, q.options.IndexItemsPerPage) << defaultItemLenBits
	// get byte slice
	b := new(bytes.Buffer)
	binary.Write(b, binary.BigEndian, int64(toAppendDataPageIndex))
	binary.Write(b, binary.BigEndian, int32(toAppendDataItemOffset))
	binary.Write(b, binary.BigEndian, int32(sz))
	binary.Write(b, binary.BigEndian, int64(time.Now().Unix()))
	binary.Write(b, binary.BigEndian, int64(0))

	bb := b.Bytes()
	copy(indexDB.data[toAppendIndexItemOffset:toAppendIndexItemOffset+defaultIndexItemLen], bb[:defaultIndexItemLen])

	// update next to the head index
	q.headIndex = q.headIndex + 1

	// update meta data
	b = new(bytes.Buffer)
	binary.Write(b, binary.BigEndian, q.headIndex)
	binary.Write(b, binary.BigEndian, q.tailIndex)

	bb = b.Bytes()

	sz = len(bb)
	copy(q.metaFile.data[:sz], bb[:])

	go q.changeSubscribeStatus(true)
	return toAppendArrayIndex, nil
}

// Dequeue Retrieves and removes the front of a queue
func (q *FileQueue) Dequeue() (int64, []byte, error) {

	if q.IsEmpty() {
		return -1, nil, nil
	}

	// check and update queue front index info
	index, err := q.updateQueueFrontIndex()
	if err != nil {
		return -1, nil, err
	}
	bb, err := q.peek(index)
	return index, bb, err
}

// Peek Retrieves the item at the front of a queue
// if item exist return with index id, item data
func (q *FileQueue) Peek() (int64, []byte, error) {
	if q.IsEmpty() {
		return -1, nil, nil
	}
	index := q.frontIndex

	bb, err := q.peek(index)
	return index, bb, err
}

// Skip the target n items to front index
func (q *FileQueue) Skip(count int64) error {
	if q.IsEmpty() {
		return nil
	}

	for i := 0; i < int(count); i++ {
		// check and update queue front index info
		_, err := q.updateQueueFrontIndex()
		if err != nil {
			return err
		}

		if q.IsEmpty() {
			return nil
		}
	}
	return nil
}

func (q *FileQueue) peek(index int64) ([]byte, error) {
	// get the queue message from the index
	q.lock.RLock()
	defer q.lock.RUnlock()

	err := q.validateIndex(index)
	if err != nil {
		return nil, err
	}

	bb, err := q.getIndexItemArray(index)
	if err != nil {
		return nil, err
	}
	dataPageIndex := BytesToInt(bb[0:8])
	dataItemOffset := BytesToInt32(bb[8:12])
	dataItemLength := BytesToInt32(bb[12:16])

	dataDB, err := q.dataFile.acquireDB(dataPageIndex)
	if err != nil {
		return nil, err
	}

	ret := make([]byte, dataItemOffset+dataItemLength-dataItemOffset)
	copy(ret, dataDB.data[dataItemOffset:])
	return ret, nil
}

func (q *FileQueue) validateIndex(index int64) error {
	if q.tailIndex <= q.headIndex {
		if index < q.tailIndex || index > q.headIndex {
			return IndexOutOfBoundTH
		}
	} else {
		if index < q.tailIndex && index >= q.headIndex {
			return IndexOutOfBoundTH
		}
	}

	return nil
}

func (q *FileQueue) updateQueueFrontIndex() (int64, error) {
	q.queueFrontWriteLock.Lock()
	defer q.queueFrontWriteLock.Unlock()

	queueFrontIndex := q.frontIndex
	nextQueueFrontIndex := queueFrontIndex

	if nextQueueFrontIndex == MaxInt64 {
		nextQueueFrontIndex = 0
	} else {
		nextQueueFrontIndex++
	}
	q.frontIndex = nextQueueFrontIndex

	bb := IntToBytes(q.frontIndex)
	for idx, b := range bb {
		q.frontFile.data[idx] = b

	}

	return queueFrontIndex, nil
}

func (q *FileQueue) initFrontFile() error {
	// create index file
	q.frontFile = &DB{
		path:            q.path + "/" + FrontFileName + "/" + GetFileName(filePrefix, fileSuffix, 0),
		InitialMmapSize: defaultFrontPageSize,
		opened:          true,
	}

	err := q.frontFile.Open(0666)
	if err != nil {
		return err
	}
	q.frontIndex = BytesToInt(q.frontFile.data[:defaultFrontPageSize])
	Assert(q.frontIndex >= 0, "front index can not be negetive number. value is %v", q.frontIndex)
	return nil
}

func (q *FileQueue) initMetaFile() error {
	// create index file
	q.metaFile = &DB{
		path:            q.path + "/" + MetaFileName + "/" + GetFileName(filePrefix, fileSuffix, 0),
		InitialMmapSize: defaultMetaPageSize,
		opened:          true,
	}

	err := q.metaFile.Open(0666)
	if err != nil {
		return err
	}

	q.headIndex = BytesToInt(q.metaFile.data[:8])
	q.tailIndex = BytesToInt(q.metaFile.data[9:16])

	Assert(q.headIndex >= 0, "head index can not be negetive number. value is %v", q.headIndex)
	Assert(q.tailIndex >= 0, "tail index can not be negetive number. value is %v", q.tailIndex)
	return nil
}

func (q *FileQueue) initDataPageIndex() error {
	if q.IsEmpty() {
		q.headDataPageIndex = 0
		q.headDataItemOffset = 0
		return nil
	}
	// get from index file
	previousIndex := q.headIndex - 1

	bb, err := q.getIndexItemArray(previousIndex)
	if err != nil {
		return err
	}
	previousDataPageIndex := BytesToInt(bb[:8])
	previousDataItemOffset := BytesToInt(bb[9:12])
	perviousDataItemLength := BytesToInt(bb[13:16])

	q.headDataPageIndex = previousDataPageIndex
	q.headDataItemOffset = previousDataItemOffset + perviousDataItemLength

	return nil

}

func (q *FileQueue) getIndexItemArray(index int64) ([]byte, error) {
	// calc index page no
	previousIndexPageIndex := index >> uint(q.options.IndexItemsPerPage)

	indexDB, err := q.indexFile.acquireDB(previousIndexPageIndex)
	if err != nil {
		return nil, err
	}
	// calc index item offset positon
	previousIndexPageOffset := Mod(index, q.options.IndexItemsPerPage) << defaultItemLenBits

	bb := indexDB.data[previousIndexPageOffset : previousIndexPageOffset+defaultIndexItemLen]

	return bb, nil
}

func (q *FileQueue) initDirs() error {
	indexFilePath := q.path + "/" + IndexFileName
	err := os.MkdirAll(indexFilePath, os.ModeDir)
	if err != nil {
		return err
	}

	dataFilePath := q.path + "/" + DataFileName
	err = os.MkdirAll(dataFilePath, os.ModeDir)
	if err != nil {
		return err
	}

	metaFilePath := q.path + "/" + MetaFileName
	err = os.MkdirAll(metaFilePath, os.ModeDir)
	if err != nil {
		return err
	}

	frontFilePath := q.path + "/" + FrontFileName
	err = os.MkdirAll(frontFilePath, os.ModeDir)
	if err != nil {
		return err
	}

	return nil
}

// Close close file queue
func (q *FileQueue) Close() error {
	q.lock.Lock()
	defer q.lock.Unlock()

	// close front index file
	if q.frontFile != nil {
		q.frontFile.Close()
	}

	if q.metaFile != nil {
		q.metaFile.Close()
	}

	if q.indexFile != nil {
		q.indexFile.Close()
	}

	if q.dataFile != nil {
		q.dataFile.Close()
	}

	return nil
}

//Gc Delete all used data files to free disk space.
//
// BigQueue will persist enqueued data in disk files, these data files will remain even after
// the data in them has been dequeued later, so your application is responsible to periodically call
// this method to delete all used data files and free disk space.
func (q *FileQueue) Gc() error {
	q.gcLock.Lock()
	defer q.gcLock.Unlock()
	frontIndex := q.frontIndex

	if frontIndex == 0 {
		return nil
	}

	frontIndex--

	err := q.validateIndex(frontIndex)
	if err != nil {
		return err
	}

	indexPageIndex := frontIndex >> uint(q.options.IndexItemsPerPage)
	bb, err := q.getIndexItemArray(frontIndex)
	if err != nil {
		return err
	}

	dataPageIndex := BytesToInt(bb[:8])
	if indexPageIndex > 0 {
		q.indexFile.removeBeforeIndex(indexPageIndex)
	}

	if dataPageIndex > 0 {
		q.dataFile.removeBeforeIndex(dataPageIndex)
	}

	q.tailIndex = frontIndex

	return nil
}

// Subscribe subscribe a call back function to subscribe message
func (q *FileQueue) Subscribe(fn func(int64, []byte, error)) error {
	if q.enqueueChan == nil {
		return SubscribeFailedNoOpenErr
	}

	if q.subscriber != nil {
		return SubscribeExistErr
	}
	q.subscriber = fn
	go q.doLoopSubscribe()
	return nil
}

// FreeSubscribe free subscriber
func (q *FileQueue) FreeSubscribe() {
	q.subscriber = nil
	go q.changeSubscribeStatusForce(false)
}

func (q *FileQueue) changeSubscribeStatus(s bool) {
	if len(q.enqueueChan) == 0 {
		q.changeSubscribeStatusForce(s)
	}
}

func (q *FileQueue) changeSubscribeStatusForce(s bool) {
	q.enqueueChan <- s
}

func (q *FileQueue) doLoopSubscribe() {
	for {
		if q.subscriber == nil {
			return
		}
		for {
			index, bb, err := q.Dequeue()
			if bb == nil {
				break // queue is empty
			}
			q.subscriber(index, bb, err)
		}

		loop := <-q.enqueueChan

		if !loop {
			break
		}
	}
}
