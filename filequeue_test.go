package bigqueue

import (
	"fmt"
	"strings"
	"testing"
)

func TestFileQueue_OpenError(t *testing.T) {
	path := Tempfile()
	defer clearFiles(path, "testqueue")
	var queue = &FileQueue{
		HeadIndex: 0,
		TailIndex: 0,
	}
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

func TestFileQueue_Open(t *testing.T) {
	path := Tempfile()
	defer clearFiles(path, "testqueue")

	var queue = new(FileQueue)

	err := queue.Open(path, "testqueue", nil)

	if err != nil {
		fmt.Println(err)
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

func TestFileQueue_Enqueue(t *testing.T) {
	path := Tempfile()
	defer clearFiles(path, "testqueue")

	var queue = new(FileQueue)
	err := queue.Open(path, "testqueue", nil)

	if err != nil {
		fmt.Println(err)
	}
	defer queue.Close()

	enqueue(queue, []byte("hello xiemalin"), 10, t)
}

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

func TestFileQueue_EnqueueDequeue(t *testing.T) {
	path := Tempfile()
	defer clearFiles(path, "testqueue")

	var queue = new(FileQueue)

	err := queue.Open(path, "testqueue", nil)

	if err != nil {
		fmt.Println(err)
	}
	defer queue.Close()

	enqueue(queue, []byte("hello xiemalin"), 10, t)

	dequeue(queue, []byte("hello xiemalin"), 10, t)
	// to check there are no message avaiable
	dequeueEmpty(queue, t)

}

func TestFileQueue_Skip(t *testing.T) {
	path := Tempfile()
	defer clearFiles(path, "testqueue")

	var queue = new(FileQueue)

	err := queue.Open(path, "testqueue", nil)

	if err != nil {
		fmt.Println(err)
	}
	defer queue.Close()

	enqueue(queue, []byte("hello xiemalin"), 10, t)

	queue.Skip(5)

	dequeue(queue, []byte("hello xiemalin"), 5, t)
	// to check there are no message avaiable
	dequeueEmpty(queue, t)

}

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

	enqueue(queue, []byte("hello xiemalin"), 10, t)

	index, bb, err = queue.Peek()
	index2, bb2, err2 := queue.Peek()
	if index != index2 || strings.Compare(string(bb), string(bb2)) != 0 || err != err2 {
		t.Error("Error peek twice but return different result ")
	}

}

func TestFileQueue_Gc(t *testing.T) {
	path := Tempfile()
	defer clearFiles(path, "testqueue")

	var queue = new(FileQueue)
	// use custom options
	var options = &Options{
		DataPageSize:      128,
		GcLock:            false,
		IndexItemsPerPage: 17,
	}

	err := queue.Open(path, "testqueue", options)

	if err != nil {
		fmt.Println(err)
	}
	defer queue.Close()

	enqueue(queue, []byte("hello xiemalin"), 500, t)
	dequeue(queue, []byte("hello xiemalin"), 500, t)
	queue.Gc()

}

// tempfile returns a temporary file path.
func Tempfile() string {
	return "./bin/temp"
}

func enqueue(queue Queue, content []byte, size int, t *testing.T) {
	// enqueue 10 items
	for i := 0; i < size; i++ {
		idx, err := queue.Enqueue(content)
		if err != nil {
			t.Error("Enqueue failed with err:", err)
		}

		if idx != int64(i) {
			t.Error("Error enqueue index, current is", idx, " expected is", i)
		}
	}

	sz := queue.Size()
	if sz != int64(size) {
		t.Error("Error enqueue count expect size is ", size, ", but acutal is ", sz)
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
