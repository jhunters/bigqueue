package bigqueue

import (
	"bytes"
	"encoding/binary"
	"errors"
	"log"
	"os"
	"sync"
	"time"
)

const (
	// front index page size
	defaultFrontPageSize = 1 << 3
	// meta file page size
	defaultMetaPageSize = 1 << 4
	// data file size
	DefaultDataPageSize = 128 * 1024 * 1024

	defaultItemLenBits       = 5
	defaultIndexItemLen      = 1 << defaultItemLenBits
	DefaultIndexItemsPerPage = 17
	defaultItemsPerPage      = 1 << DefaultIndexItemsPerPage
	// index file size
	defaultIndexPageSize = defaultIndexItemLen * defaultItemsPerPage

	MaxInt64 = 0x7fffffffffffffff

	IndexFileName = "index"
	DataFileName  = "data"
	MetaFileName  = "meta_data"
	FrontFileName = "front_index"

	filePrefix = "page-"
	fileSuffix = ".dat"
)

// default options
var DefaultOptions = &Options{
	DataPageSize:      DefaultDataPageSize,
	indexPageSize:     defaultIndexPageSize,
	IndexItemsPerPage: DefaultIndexItemsPerPage,
	itemsPerPage:      defaultItemsPerPage,
}

type FileQueue struct {
	// front index of the big queue,
	FrontIndex int64

	// head index of the array, this is the read write barrier.
	// readers can only read items before this index, and writes can write this index or after
	HeadIndex int64

	// tail index of the array,
	// readers can't read items before this tail
	TailIndex int64

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
	Subscriber func(int64, []byte, error)

	enqueueChan chan bool

	gcLock sync.Mutex
}

// Open the queue files
func (q *FileQueue) Open(dir string, queueName string, options *Options) error {
	if len(dir) == 0 {
		return errors.New("parameter 'dir' can not be blank.")
	}

	if len(queueName) == 0 {
		return errors.New("parameter 'queueName' can not be blank.")
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

// Determines whether a queue is empty
func (q *FileQueue) IsEmpty() bool {
	return q.FrontIndex >= q.HeadIndex
}

// Total number of items available in the queue.
func (q *FileQueue) Size() int64 {
	sz := q.HeadIndex - q.FrontIndex
	if sz < 0 {
		sz = 0
	}
	return int64(sz)
}

// Adds an item at the queue and HeadIndex will increase
// Asynchouous mode will call back with fn function
func (q *FileQueue) EnqueueAsync(data []byte, fn func(int64, error)) {
	go q.doEnqueueAsync(data, fn)
}

func (q *FileQueue) doEnqueueAsync(data []byte, fn func(int64, error)) {
	index, err := q.Enqueue(data)
	fn(index, err)
}

// Adds an item at the queue and HeadIndex will increase
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

	toAppendArrayIndex := q.HeadIndex
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
	q.HeadIndex = q.HeadIndex + 1

	// update meta data
	b = new(bytes.Buffer)
	binary.Write(b, binary.BigEndian, q.HeadIndex)
	binary.Write(b, binary.BigEndian, q.TailIndex)

	bb = b.Bytes()

	sz = len(bb)
	copy(q.metaFile.data[:sz], bb[:])

	go q.changeSubscribeStatus(true)
	return toAppendArrayIndex, nil
}

// Retrieves and removes the front of a queue
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

// Retrieves the item at the front of a queue
// if item exist return with index id, item data
func (q *FileQueue) Peek() (int64, []byte, error) {
	if q.IsEmpty() {
		return -1, nil, nil
	}
	index := q.FrontIndex

	bb, err := q.peek(index)
	return index, bb, err
}

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

	bb = dataDB.data[dataItemOffset : dataItemOffset+dataItemLength]
	return bb, nil
}

func (q *FileQueue) validateIndex(index int64) error {
	if q.TailIndex <= q.HeadIndex {
		if index < q.TailIndex || index > q.HeadIndex {
			return IndexOutOfBoundTH
		}
	} else {
		if index < q.TailIndex && index >= q.HeadIndex {
			return IndexOutOfBoundTH
		}
	}

	return nil
}

func (q *FileQueue) updateQueueFrontIndex() (int64, error) {
	q.queueFrontWriteLock.Lock()
	defer q.queueFrontWriteLock.Unlock()

	queueFrontIndex := q.FrontIndex
	nextQueueFrontIndex := queueFrontIndex

	if nextQueueFrontIndex == MaxInt64 {
		nextQueueFrontIndex = 0
	} else {
		nextQueueFrontIndex++
	}
	q.FrontIndex = nextQueueFrontIndex

	bb := IntToBytes(q.FrontIndex)
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
	q.FrontIndex = BytesToInt(q.frontFile.data[:defaultFrontPageSize])
	Assert(q.FrontIndex >= 0, "front index can not be negetive number. value is %v", q.FrontIndex)
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

	q.HeadIndex = BytesToInt(q.metaFile.data[:8])
	q.TailIndex = BytesToInt(q.metaFile.data[9:16])

	Assert(q.HeadIndex >= 0, "head index can not be negetive number. value is %v", q.HeadIndex)
	Assert(q.TailIndex >= 0, "tail index can not be negetive number. value is %v", q.TailIndex)
	return nil
}

func (q *FileQueue) initDataPageIndex() error {
	if q.IsEmpty() {
		q.headDataPageIndex = 0
		q.headDataItemOffset = 0
		return nil
	}
	// get from index file
	previousIndex := q.HeadIndex - 1

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

// Delete all used data files to free disk space.
//
// BigQueue will persist enqueued data in disk files, these data files will remain even after
// the data in them has been dequeued later, so your application is responsible to periodically call
// this method to delete all used data files and free disk space.
func (q *FileQueue) Gc() error {
	q.gcLock.Lock()
	defer q.gcLock.Unlock()
	frontIndex := q.FrontIndex

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

	q.TailIndex = frontIndex

	return nil
}

func (q *FileQueue) Subscribe(fn func(int64, []byte, error)) error {
	if q.enqueueChan == nil {
		return SubscribeFailedNoOpenErr
	}

	if q.Subscriber != nil {
		return SubscribeExistErr
	}
	q.Subscriber = fn
	go q.doLoopSubscribe()
	return nil
}

func (q *FileQueue) FreeSubscribe() {
	q.Subscriber = nil
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
		if q.Subscriber == nil {
			return
		}
		for {
			index, bb, err := q.Dequeue()
			if bb == nil {
				break // queue is empty
			}
			q.Subscriber(index, bb, err)
		}

		loop := <-q.enqueueChan

		if !loop {
			return
		}
	}
	log.Println("loop end")
}
