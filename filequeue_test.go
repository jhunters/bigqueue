package bigqueue

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// TestFileQueue_OpenError to test Open function which without required parameters.
func TestFileQueue_OpenError(t *testing.T) {
	path := Tempfile()
	defer clearFiles(path, "testqueue")
	var queue = &FileQueue{}
	//var queue = new(FileQueue)

	err := queue.Open("", "", nil)
	if err == nil {
		t.Error("Error parameter 'path' should return non-nil err")
	}
	defer queue.Close()

	err = queue.Open(path, "", nil)
	if err == nil {
		t.Error("Error parameter 'queueName' should return non-nil err")
	}
	defer queue.Close()
}

// TestFileQueue_Open to test open file without any error and check the initial size
func TestFileQueue_Open(t *testing.T) {
	path := Tempfile()
	defer clearFiles(path, "testqueue")

	var queue = new(FileQueue)

	err := queue.Open(path, "testqueue", nil)

	if err != nil {
		t.Error(err)
	}
	defer queue.Close()

	sz := queue.Size()
	if sz != 0 {
		t.Error("Init queue size must be zero, but now is", sz)
	}

	empty := queue.IsEmpty()
	if !empty {
		t.Error("Init queue must be empty, but now is not empty")
	}
}

// TestFileQueue_Open to test open file without any error and check the initial size
func TestFileQueue_OpenTwice(t *testing.T) {
	path := Tempfile()
	defer clearFiles(path, "testqueue")

	var queue = new(FileQueue)

	err := queue.Open(path, "testqueue", nil)

	if err != nil {
		t.Error(err)
	}
	defer queue.Close()

	// open again will return error
	err = queue.Open(path, "testqueue", nil)
	if err == nil {
		t.Error("open twice should return error, but actually return nil")
	}

	sz := queue.Size()
	if sz != 0 {
		t.Error("Init queue size must be zero, but now is", sz)
	}

	empty := queue.IsEmpty()
	if !empty {
		t.Error("Init queue must be empty, but now is not empty")
	}
}

// TestFileQueue_Enqueue to test enqueue function
func TestFileQueue_Enqueue(t *testing.T) {
	path := Tempfile()
	defer clearFiles(path, "testqueue")

	var queue = new(FileQueue)
	err := queue.Open(path, "testqueue", nil)

	if err != nil {
		fmt.Println(err)
	}
	defer queue.Close()

	enqueue(queue, []byte("hello xiemalin中文"), 10, t)
}

// TestFileQueue_DequeueEmpty to test dequeue item from an empty queue
func TestFileQueue_DequeueEmpty(t *testing.T) {
	path := Tempfile()
	defer clearFiles(path, "testqueue")

	var queue = new(FileQueue)

	err := queue.Open(path, "testqueue", nil)

	if err != nil {
		fmt.Println(err)
	}
	defer queue.Close()

	dequeueEmpty(queue, t)

}

// TestFileQueue_EnqueueDequeue to test enqueue and dequeue
func TestFileQueue_EnqueueDequeue(t *testing.T) {
	path := Tempfile()
	defer clearFiles(path, "testqueue")

	var queue = new(FileQueue)

	err := queue.Open(path, "testqueue", nil)

	if err != nil {
		fmt.Println(err)
	}
	defer queue.Close()

	enqueue(queue, []byte("hello xiemalin中文"), 10, t)

	dequeue(queue, []byte("hello xiemalin中文"), 10, t)
	// to check there are no message avaiable
	dequeueEmpty(queue, t)

}

// TestFileQueue_Skip to test skip function
func TestFileQueue_Skip(t *testing.T) {
	path := Tempfile()
	defer clearFiles(path, "testqueue")

	var queue = new(FileQueue)

	err := queue.Open(path, "testqueue", nil)

	if err != nil {
		fmt.Println(err)
	}
	defer queue.Close()

	enqueue(queue, []byte("hello xiemalin中文"), 10, t)

	queue.Skip(5)

	dequeue(queue, []byte("hello xiemalin中文"), 5, t)
	// to check there are no message avaiable
	dequeueEmpty(queue, t)

}

// TestFileQueue_Peek to test peek item function
func TestFileQueue_Peek(t *testing.T) {
	path := Tempfile()
	defer clearFiles(path, "testqueue")

	var queue = new(FileQueue)

	err := queue.Open(path, "testqueue", nil)

	if err != nil {
		fmt.Println(err)
	}
	defer queue.Close()

	// peek to an empty queue
	index, bb, err := queue.Peek()
	if err != nil {
		t.Error("Error peek to an empty queue should return nil err, but actually is ", err)
	}

	if index != -1 {
		t.Error("Error peek to an empty queue should return index of '-1', but actually is ", index)
	}

	if bb != nil {
		t.Error("Error peek to an empty queue should return nil, but actually is ", bb)
	}

	enqueue(queue, []byte("hello xiemalin中文"), 10, t)

	index, bb, err = queue.Peek()
	index2, bb2, err2 := queue.Peek()
	if index != index2 || strings.Compare(string(bb), string(bb2)) != 0 || err != err2 {
		t.Error("Error peek twice but return different result ")
	}

}

// TestFileQueue_Gc to test gc func after enqueue and dequeue process
func TestFileQueue_Gc(t *testing.T) {
	path := Tempfile()
	defer clearFiles(path, "testqueue")

	var queue = new(FileQueue)
	// use custom options
	var options = &Options{
		DataPageSize:      128,
		IndexItemsPerPage: 17,
	}

	err := queue.Open(path, "testqueue", options)

	if err != nil {
		t.Error(err)
	}
	defer queue.Close()

	enqueue(queue, []byte("hello xiemalin中文"), 500, t)
	dequeue(queue, []byte("hello xiemalin中文"), 500, t)
	queue.Gc()

}

// TestFileQueue_AutoGc to test automatic gc function while on enqueue and dequeue process
func TestFileQueue_AutoGc(t *testing.T) {
	path := Tempfile()
	defer clearFiles(path, "testqueue")

	var queue = new(FileQueue)
	// use custom options
	var options = &Options{
		DataPageSize:      128,
		IndexItemsPerPage: 17,
		AutoGCBySeconds:   1,
	}

	err := queue.Open(path, "testqueue", options)

	if err != nil {
		t.Error(err)
	}
	defer queue.Close()

	doEnqueue(queue, []byte("hello xiemalin中文"), 500, t)
	dequeue(queue, []byte("hello xiemalin中文"), 500, t)

	time.Sleep(2 * time.Second)
}

// TestFileQueue_Subscribe to test subscribe function
func TestFileQueue_Subscribe(t *testing.T) {
	path := Tempfile()
	defer clearFiles(path, "testqueue")

	i := 0
	var queue = new(FileQueue)

	err := queue.Subscribe(func(index int64, bb []byte, err error) {
		i++
	})
	// here should err
	if err != ErrSubscribeFailedNoOpenErr {
		t.Error("Subscribe shoule return err before queue opened")
	}

	err = queue.Open(path, "testqueue", nil)

	queue.Subscribe(func(index int64, bb []byte, err error) {
		i++
	})

	if err != nil {
		fmt.Println(err)
	}
	defer queue.Close()

	sz := 10

	doEnqueue(queue, []byte("hello xiemalin中文"), sz, t)

	time.Sleep(time.Duration(2) * time.Second)

	if i != sz {
		t.Error("subscribe count should be", sz, " but actually is ", i)
	}

	queue.FreeSubscribe()

}

// TestFileQueue_FreeSubscribe to test free subscribe function
func TestFileQueue_FreeSubscribe(t *testing.T) {
	path := Tempfile()
	defer clearFiles(path, "testqueue")

	i := 0
	var queue = new(FileQueue)

	err := queue.Open(path, "testqueue", nil)

	queue.Subscribe(func(index int64, bb []byte, err error) {
		i++
	})

	if err != nil {
		fmt.Println(err)
	}
	defer queue.Close()

	queue.FreeSubscribe()
	sz := 10
	// no longer receive subscrbie callback
	enqueue(queue, []byte("hello xiemalin中文"), sz, t)

	if i != 0 {
		t.Error("subscribe count should be 0,  but actually is ", i)
	}
}

// TestFileQueue_PeekAll
func TestFileQueue_PeekAll(t *testing.T) {
	path := Tempfile()
	defer clearFiles(path, "testqueue")

	var queue = new(FileQueue)

	err := queue.Open(path, "testqueue", nil)
	if err != nil {
		fmt.Println(err)
	}
	defer queue.Close()

	sz := 10
	// no longer receive subscrbie callback
	enqueue(queue, []byte("hello xiemalin中文"), sz, t)

	r, err := queue.PeekAll()
	if err != nil {
		t.Error(err)
	}
	if len(r) != 10 {
		t.Error("Peek all should return size ", sz)
	}
}

// tempfile returns a temporary file path.
func Tempfile() string {
	return "./bin/temp"
}

func enqueue(queue Queue, content []byte, size int, t *testing.T) {
	doEnqueue(queue, content, size, t)

	sz := queue.Size()
	if sz != int64(size) {
		t.Error("Error enqueue count expect size is ", size, ", but acutal is ", sz)
	}
}

func doEnqueue(queue Queue, content []byte, size int, t *testing.T) {
	for i := 0; i < size; i++ {
		idx, err := queue.Enqueue(content)
		if err != nil {
			t.Error("Enqueue failed with err:", err)
		}

		if idx != int64(i) {
			t.Error("Error enqueue index, current is", idx, " expected is", i)
		}
	}
}

func dequeue(queue Queue, expectContent []byte, expectSize int, t *testing.T) {
	count := 0
	// enqueue 10 items
	for i := 0; i < expectSize; i++ {
		idx, bb, err := queue.Dequeue()

		if err != nil {
			t.Error("Dequeue failed with err:", err)
			continue
		}

		if idx == -1 {
			t.Error("Error enqueue index, current is -1")
			continue
		}
		count++
		if strings.Compare(string(expectContent), string(bb)) != 0 {
			t.Error("Dequeue error with unexcept message from queue, expect is", string(expectContent), "but actually is", string(bb))
		}
	}
	if count != expectSize {
		t.Error("Error dequeue count expect size is ", expectSize, ", but acutal is ", count)
	}

}

func dequeueEmpty(queue Queue, t *testing.T) {
	idx, _, err := queue.Dequeue()

	if err != nil {
		t.Error("Met errors on dequeue action from an empty file queue =>", err)
	}

	if idx != -1 {
		t.Error("Empty queue dequeue index must return -1, but actually is ", idx)
	}
}

func clearFiles(path string, queueName string) {
	RemoveFiles(path + "/" + queueName + "/" + DataFileName)
	RemoveFiles(path + "/" + queueName + "/" + FrontFileName)
	RemoveFiles(path + "/" + queueName + "/" + IndexFileName)
	RemoveFiles(path + "/" + queueName + "/" + MetaFileName)
}
