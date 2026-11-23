package bigqueue

import (
	"fmt"
	"math"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

// TestFileQueue_OpenError to test Open function which without required parameters.
func TestFileQueue_OpenError(t *testing.T) {

	Convey("TestFileQueue_OpenError", t, func() {
		path := Tempfile()
		clearFiles(path, "testqueue")
		defer clearFiles(path, "testqueue")
		Convey("Open with empty path and name", func() {
			var queue = &FileQueue{}
			//var queue = new(FileQueue)
			err := queue.Open("", "", nil)
			So(err, ShouldNotBeNil)
			So(err, ShouldBeError, "parameter 'dir' can not be blank")
			defer queue.Close()
			defer clearFiles(path, "testqueue")
		})

		Convey("Open with empty name", func() {
			var queue = &FileQueue{}
			err := queue.Open(path, "", nil)
			So(err, ShouldNotBeNil)
			So(err, ShouldBeError, "parameter 'queueName' can not be blank")
			defer queue.Close()
		})

	})

}

// TestFileQueue_Open to test open file without any error and check the initial size
func TestFileQueue_Open(t *testing.T) {
	path := Tempfile()
	clearFiles(path, "testqueue")
	defer clearFiles(path, "testqueue")

	Convey("TestFileQueue_Open", t, func() {
		var queue = new(FileQueue)

		err := queue.Open(path, "testqueue", nil)

		if err != nil {
			t.Error(err)
		}
		defer queue.Close()
		defer clearFiles(path, "testqueue")

		sz := queue.Size()
		SoMsg(fmt.Sprintf("Init queue size must be zero, but now is %d", sz), sz, ShouldEqual, 0)

		empty := queue.IsEmpty()
		SoMsg("Init queue must be empty, but now is not empty", empty, ShouldBeTrue)
	})

}

// TestFileQueue_Open to test open file without any error and check the initial size
func TestFileQueue_OpenTwice(t *testing.T) {
	path := Tempfile()
	clearFiles(path, "testqueue")
	defer clearFiles(path, "testqueue")

	Convey("TestFileQueue_OpenTwice", t, func() {
		var queue = new(FileQueue)

		err := queue.Open(path, "testqueue", nil)
		So(err, ShouldBeNil)

		defer queue.Close()
		defer clearFiles(path, "testqueue")

		// open again will return error
		Convey("Already open", func() {
			err = queue.Open(path, "testqueue", nil)
			So(err, ShouldNotBeNil)
			So(err, ShouldBeError, "FileQueue already opened")
		})

		sz := queue.Size()
		SoMsg(fmt.Sprintf("Init queue size must be zero, but now is %d", sz), sz, ShouldEqual, 0)

		empty := queue.IsEmpty()
		SoMsg("Init queue must be empty, but now is not empty", empty, ShouldBeTrue)
	})

}

// TestFileQueue_Enqueue to test enqueue function
func TestFileQueue_Enqueue(t *testing.T) {
	path := Tempfile()
	clearFiles(path, "testqueue")

	var queue = new(FileQueue)
	Convey("TestFileQueue_Enqueue", t, func() {
		err := queue.Open(path, "testqueue", nil)
		So(err, ShouldBeNil)
		enqueue(queue, []byte("hello xiemalin中文"), 10, t)
	})
	defer queue.Close()
	defer clearFiles(path, "testqueue")

}

// TestFileQueue_DequeueEmpty to test dequeue item from an empty queue
func TestFileQueue_DequeueEmpty(t *testing.T) {
	path := Tempfile()
	clearFiles(path, "testqueue")
	defer clearFiles(path, "testqueue")

	Convey("TestFileQueue_DequeueEmpty", t, func() {

		var queue = new(FileQueue)

		err := queue.Open(path, "testqueue", nil)

		So(err, ShouldBeNil)
		dequeueEmpty(queue, t)
		defer queue.Close()
	})

}

// TestFileQueue_EnqueueDequeue to test enqueue and dequeue
func TestFileQueue_EnqueueDequeue(t *testing.T) {
	path := Tempfile()
	clearFiles(path, "testqueue")
	defer clearFiles(path, "testqueue")

	Convey("TestFileQueue_EnqueueDequeue", t, func() {

		var queue = new(FileQueue)
		err := queue.Open(path, "testqueue", nil)
		So(err, ShouldBeNil)
		defer queue.Close()
		enqueue(queue, []byte("hello xiemalin中文"), 10, t)
		dequeue(queue, []byte("hello xiemalin中文"), 10, t)
		// to check there are no message avaiable
		dequeueEmpty(queue, t)
	})

}

// TestFileQueue_Skip to test skip function
func TestFileQueue_Skip(t *testing.T) {
	path := Tempfile()
	clearFiles(path, "testqueue")
	defer clearFiles(path, "testqueue")

	Convey("TestFileQueue_EnqueueDequeue", t, func() {
		var queue = new(FileQueue)

		err := queue.Open(path, "testqueue", nil)

		So(err, ShouldBeNil)
		defer queue.Close()

		enqueue(queue, []byte("hello xiemalin中文"), 10, t)

		queue.Skip(5)

		dequeue(queue, []byte("hello xiemalin中文"), 5, t)
		// to check there are no message avaiable
		dequeueEmpty(queue, t)
	})

}

// TestFileQueue_Peek to test peek item function
func TestFileQueue_Peek(t *testing.T) {
	path := Tempfile()
	clearFiles(path, "testqueue")
	defer clearFiles(path, "testqueue")

	Convey("TestFileQueue_EnqueueDequeue", t, func() {
		var queue = new(FileQueue)

		err := queue.Open(path, "testqueue", nil)

		So(err, ShouldBeNil)
		defer queue.Close()

		// peek to an empty queue
		index, bb, err := queue.Peek()
		So(err, ShouldBeNil)
		SoMsg(fmt.Sprintf("Error peek to an empty queue should return index of '-1', but actually is %d", index), index, ShouldEqual, -1)
		So(bb, ShouldBeNil)

		enqueue(queue, []byte("hello xiemalin中文"), 10, t)

		index, bb, err = queue.Peek()
		index2, bb2, err2 := queue.Peek()
		So(index, ShouldEqual, index2)
		So(bb, ShouldResemble, bb2)
		So(err, ShouldResemble, err2)

	})

}

// TestFileQueue_Gc to test gc func after enqueue and dequeue process
func TestFileQueue_Gc(t *testing.T) {
	path := Tempfile()
	clearFiles(path, "testqueue")
	defer clearFiles(path, "testqueue")

	Convey("TestFileQueue_Gc", t, func() {
		var queue = new(FileQueue)
		// use custom options
		var options = &Options{
			DataPageSize:      128,
			IndexItemsPerPage: 17,
		}

		err := queue.Open(path, "testqueue", options)

		So(err, ShouldBeNil)
		defer queue.Close()

		enqueue(queue, []byte("hello xiemalin中文"), 500, t)
		dequeue(queue, []byte("hello xiemalin中文"), 500, t)
		queue.Gc()
	})

}

// TestFileQueue_AutoGc to test automatic gc function while on enqueue and dequeue process
func TestFileQueue_AutoGc(t *testing.T) {
	path := Tempfile()
	clearFiles(path, "testqueue")
	defer clearFiles(path, "testqueue")

	Convey("TestFileQueue_Gc", t, func() {
		var queue = new(FileQueue)
		// use custom options
		var options = &Options{
			DataPageSize:      128,
			IndexItemsPerPage: 17,
			AutoGCBySeconds:   1,
		}

		err := queue.Open(path, "testqueue", options)

		So(err, ShouldBeNil)
		defer queue.Close()

		doEnqueue(queue, []byte("hello xiemalin中文"), 500, t)
		dequeue(queue, []byte("hello xiemalin中文"), 500, t)

		time.Sleep(2 * time.Second)
	})
}

// autoGCGoroutines counts goroutines running a queue's auto gc loop.
func autoGCGoroutines() int {
	buf := make([]byte, 1<<20)
	buf = buf[:runtime.Stack(buf, true)]
	return strings.Count(string(buf), "bigqueue.(*FileQueue).autoGC.func")
}

// TestFileQueue_CloseStopsAutoGc checks a closed queue stops garbage collecting:
// its gc goroutine would otherwise keep deleting page files in the directory,
// including ones a queue opened later on the same directory is using.
func TestFileQueue_CloseStopsAutoGc(t *testing.T) {
	path := Tempfile()
	clearFiles(path, "testqueue_closegc")
	defer clearFiles(path, "testqueue_closegc")

	Convey("TestFileQueue_CloseStopsAutoGc", t, func() {
		before := autoGCGoroutines()
		var queue = new(FileQueue)
		err := queue.Open(path, "testqueue_closegc", &Options{
			DataPageSize:      128,
			IndexItemsPerPage: 17,
			AutoGCBySeconds:   1,
		})
		So(err, ShouldBeNil)
		So(autoGCGoroutines(), ShouldEqual, before+1)

		queue.Close()

		deadline := time.Now().Add(2 * time.Second)
		for autoGCGoroutines() > before && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		So(autoGCGoroutines(), ShouldEqual, before)
	})
}

// TestFileQueue_CloseTwice checks a second Close returns instead of waiting on
// a gc loop that has already stopped.
func TestFileQueue_CloseTwice(t *testing.T) {
	path := Tempfile()
	clearFiles(path, "testqueue_closetwice")
	defer clearFiles(path, "testqueue_closetwice")

	Convey("TestFileQueue_CloseTwice", t, func() {
		var queue = new(FileQueue)
		err := queue.Open(path, "testqueue_closetwice", &Options{
			DataPageSize:      128,
			IndexItemsPerPage: 17,
			AutoGCBySeconds:   1,
		})
		So(err, ShouldBeNil)
		queue.Close()

		done := make(chan struct{})
		go func() {
			queue.Close()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("second Close did not return")
		}
	})
}

// TestFileQueue_Subscribe to test subscribe function
func TestFileQueue_Subscribe(t *testing.T) {
	path := Tempfile()
	clearFiles(path, "testqueue")
	defer clearFiles(path, "testqueue")

	Convey("TestFileQueue_Subscribe", t, func() {
		i := 0
		var queue = new(FileQueue)

		err := queue.Subscribe(func(index int64, bb []byte, err error) {
			i++
		})
		// here should err
		So(err, ShouldBeError, ErrSubscribeFailedNoOpenErr)

		err = queue.Open(path, "testqueue", nil)

		queue.Subscribe(func(index int64, bb []byte, err error) {
			i++
		})

		So(err, ShouldBeNil)
		defer queue.Close()

		sz := 10

		doEnqueue(queue, []byte("hello xiemalin中文"), sz, t)

		time.Sleep(time.Duration(2) * time.Second)

		So(i, ShouldEqual, sz)

		queue.FreeSubscribe()
	})

}

// TestFileQueue_FreeSubscribe to test free subscribe function
func TestFileQueue_FreeSubscribe(t *testing.T) {
	path := Tempfile()
	clearFiles(path, "testqueue")
	defer clearFiles(path, "testqueue")

	Convey("TestFileQueue_FreeSubscribe", t, func() {

		i := 0
		var queue = new(FileQueue)

		err := queue.Open(path, "testqueue", nil)

		queue.Subscribe(func(index int64, bb []byte, err error) {
			i++
		})

		So(err, ShouldBeNil)
		defer queue.Close()

		queue.FreeSubscribe()
		sz := 10
		// no longer receive subscrbie callback
		enqueue(queue, []byte("hello xiemalin中文"), sz, t)

		SoMsg(fmt.Sprintf("subscribe count should be 0,  but actually is %d", i), i, ShouldEqual, 0)

	})

}

// TestFileQueue_FreeSubscribe_MidCycle to test free subscribe function in the middle of cycle
func TestFileQueue_FreeSubscribe_MidCycle(t *testing.T) {
	path := Tempfile()
	clearFiles(path, "testqueue")
	defer clearFiles(path, "testqueue")

	Convey("TestFileQueue_FreeSubscribe_MidCycle", t, func() {
		i := 0
		var queue = new(FileQueue)

		err := queue.Open(path, "testqueue", nil)

		var wg sync.WaitGroup
		wg.Add(5)

		queue.Subscribe(func(index int64, bb []byte, err error) {
			defer wg.Done()
			i++
			if i == 5 {
				queue.FreeSubscribe()
			}
		})

		if err != nil {
			fmt.Println(err)
		}
		defer queue.Close()

		sz := 10
		doEnqueue(queue, []byte("hello xiemalin中文"), sz, t)

		wg.Wait()

		So(queue.Size(), ShouldBeLessThanOrEqualTo, 5)
	})

}

// TestFileQueue_PeekAll
func TestFileQueue_PeekAll(t *testing.T) {
	path := Tempfile()
	clearFiles(path, "testqueue")
	defer clearFiles(path, "testqueue")

	Convey("TestFileQueue_Subscribe", t, func() {

		var queue = new(FileQueue)

		err := queue.Open(path, "testqueue", nil)
		So(err, ShouldBeNil)
		defer queue.Close()

		sz := 10
		// no longer receive subscrbie callback
		enqueue(queue, []byte("hello xiemalin中文"), sz, t)

		r, indexs, err := queue.PeekAll()
		So(err, ShouldBeNil)
		So(len(r), ShouldEqual, 10)
		So(len(indexs), ShouldEqual, 10)

	})

}

// TestFileQueue_Status
func TestFileQueue_Status(t *testing.T) {
	Convey("Test empty queue status result", t, func() {
		path := Tempfile()
		clearFiles(path, "testqueue")
		var queue = new(FileQueue)

		err := queue.Open(path, "testqueue", nil)
		So(err, ShouldBeNil)
		defer queue.Close()
		defer clearFiles(path, "testqueue")

		qFileStatus := queue.Status()

		So(qFileStatus, ShouldNotBeNil)
		So(qFileStatus.FrontIndex, ShouldEqual, 0)
		So(qFileStatus.HeadIndex, ShouldEqual, 0)
		So(qFileStatus.TailIndex, ShouldEqual, 0)
		So(qFileStatus.HeadDataPageIndex, ShouldEqual, 0)
		So(qFileStatus.HeadDataItemOffset, ShouldEqual, 0)

		So(len(qFileStatus.IndexFileList), ShouldEqual, 1)
		So(len(qFileStatus.DataFileList), ShouldEqual, 0)
		So(qFileStatus.MetaFileInfo, ShouldNotBeNil)
		So(qFileStatus.FrontFileInfo, ShouldNotBeNil)

	})

	Convey("Test non-empty queue status result", t, func() {
		path := Tempfile()

		var queue = new(FileQueue)

		err := queue.Open(path, "testqueue", nil)
		if err != nil {
			t.Error(err)
		}
		defer queue.Close()
		defer clearFiles(path, "testqueue")

		data := []byte("hello xmatthew")
		dataLen := len(data)

		queue.Enqueue(data)
		queue.Dequeue()

		qFileStatus := queue.Status()

		So(qFileStatus, ShouldNotBeNil)
		So(qFileStatus.FrontIndex, ShouldEqual, 1)
		So(qFileStatus.HeadIndex, ShouldEqual, 1)
		So(qFileStatus.TailIndex, ShouldEqual, 0)
		So(qFileStatus.HeadDataPageIndex, ShouldEqual, 0)
		So(qFileStatus.HeadDataItemOffset, ShouldEqual, dataLen)

		So(len(qFileStatus.IndexFileList), ShouldEqual, 1)

		fileInfo := qFileStatus.IndexFileList[0]
		So(fileInfo.CanGC, ShouldBeFalse)
		So(fileInfo.FileIndex, ShouldEqual, 0)

		So(len(qFileStatus.DataFileList), ShouldEqual, 1)
		fileInfo = qFileStatus.IndexFileList[0]
		So(fileInfo.CanGC, ShouldBeFalse)
		So(fileInfo.FileIndex, ShouldEqual, 0)
		So(qFileStatus.MetaFileInfo, ShouldNotBeNil)
		So(qFileStatus.FrontFileInfo, ShouldNotBeNil)

		// after gc
		queue.Enqueue(data)
		queue.Dequeue()
		queue.Gc()
		qFileStatus = queue.Status()
		So(qFileStatus.TailIndex, ShouldEqual, 1)

	})

}

// TestFileQueue_PeekPagination
func TestFileQueue_PeekPagination(t *testing.T) {
	Convey("Test PeekPagination", t, func() {

		path := Tempfile()
		clearFiles(path, "testqueue")

		var queue = new(FileQueue)

		err := queue.Open(path, "testqueue", nil)
		if err != nil {
			t.Error(err)
		}
		defer queue.Close()
		defer clearFiles(path, "testqueue")
		Convey("test PeekPagination on empty queue", func() {
			data, indexs, err := queue.PeekPagination(0, 0)
			So(err, ShouldBeNil)
			So(data, ShouldBeEmpty)
			So(indexs, ShouldBeEmpty)

			data, indexs, err = queue.PeekPagination(1, 1)
			So(err, ShouldBeNil)
			So(data, ShouldBeEmpty)
			So(indexs, ShouldBeEmpty)
		})

		Convey("test PeekPagination on items small than pagesize", func() {
			for i := 0; i < 5; i++ { // add value
				_, err := queue.Enqueue([]byte("hello matthew " + strconv.Itoa(i)))
				So(err, ShouldBeNil)
			}

			data, indexs, err := queue.PeekPagination(0, 0)
			So(err, ShouldBeNil)
			So(len(data), ShouldEqual, 5)
			So(string(data[4]), ShouldEqual, "hello matthew 4")
			So(len(indexs), ShouldEqual, 5)

			data, indexs, err = queue.PeekPagination(1, 10)
			So(err, ShouldBeNil)
			So(len(data), ShouldEqual, 5)
			So(string(data[4]), ShouldEqual, "hello matthew 4")
			So(len(indexs), ShouldEqual, 5)

			data, indexs, err = queue.PeekPagination(2, 10) // large paing
			So(err, ShouldBeNil)
			So(data, ShouldBeEmpty)
			So(indexs, ShouldBeEmpty)

			data, indexs, err = queue.PeekPagination(2, 2)
			So(err, ShouldBeNil)
			So(len(data), ShouldEqual, 2)
			So(string(data[1]), ShouldEqual, "hello matthew 3")
			So(len(indexs), ShouldEqual, 2)
		})

	})
}

func TestMultiGoroutinesEnqueueDequeue(t *testing.T) {
	path := Tempfile()
	clearFiles(path, "testqueue")
	defer clearFiles(path, "testqueue")

	enqueueResult := make(chan int, 10)
	dequeueResult := make(chan int, 10)

	Convey("TestMultiGoroutinesEnqueueDequeue", t, func() {
		var queue = new(FileQueue)
		err := queue.Open(path, "testqueue", nil)
		So(err, ShouldBeNil)
		defer queue.Close()

		syncEnqueu := func(q Queue) {
			q.Enqueue([]byte{1, 2})
			enqueueResult <- 0
		}
		for i := 0; i < 10; i++ {
			go syncEnqueu(queue)
		}

		syncDequeu := func(q Queue) {
			q.Dequeue()
			dequeueResult <- 0
		}
		for i := 0; i < 10; i++ {
			go syncDequeu(queue)
		}

		enqueueSize := 0
		dequeueSize := 0
		for {
			select {
			case <-enqueueResult:
				enqueueSize++
			case <-dequeueResult:
				dequeueSize++
			}

			if enqueueSize == 10 && dequeueSize == 10 {
				return
			}
		}

	})
}

// tempfile returns a temporary file path.
func Tempfile() string {
	return "./bin/temp"
}

func enqueue(queue Queue, content []byte, size int, t *testing.T) {
	doEnqueue(queue, content, size, t)

	sz := queue.Size()
	So(sz, ShouldEqual, size)
}

func doEnqueue(queue Queue, content []byte, size int, t *testing.T) {
	for i := 0; i < size; i++ {
		idx, err := queue.Enqueue(content)
		So(err, ShouldBeNil)

		So(idx, ShouldEqual, i)
	}
}

func dequeue(queue Queue, expectContent []byte, expectSize int, t *testing.T) {
	count := 0
	// enqueue 10 items
	for i := 0; i < expectSize; i++ {
		idx, bb, err := queue.Dequeue()

		So(err, ShouldBeNil)
		So(idx, ShouldNotEqual, -1)
		count++
		So(expectContent, ShouldResemble, bb)
	}

	So(count, ShouldEqual, expectSize)

}

func dequeueEmpty(queue Queue, t *testing.T) {
	idx, _, err := queue.Dequeue()

	So(err, ShouldBeNil)
	So(idx, ShouldEqual, -1)
}

func clearFiles(path string, queueName string) {
	RemoveFiles(path + "/" + queueName + "/" + DataFileName)
	RemoveFiles(path + "/" + queueName + "/" + FrontFileName)
	RemoveFiles(path + "/" + queueName + "/" + IndexFileName)
	RemoveFiles(path + "/" + queueName + "/" + MetaFileName)
}

func TestFileQueue_PeekRange(t *testing.T) {
	Convey("Test PeekRange", t, func() {

		path := Tempfile()
		clearFiles(path, "testqueue")

		var queue = new(FileQueue)

		err := queue.Open(path, "testqueue", nil)
		if err != nil {
			t.Error(err)
		}
		defer queue.Close()
		defer clearFiles(path, "testqueue")
		Convey("test PeekRange on empty queue", func() {
			data, indexs, err := queue.PeekRange(0, 3)
			So(err, ShouldBeNil)
			So(data, ShouldBeEmpty)
			So(indexs, ShouldBeEmpty)
		})

		Convey("test PeekRange from an offset after the front", func() {
			for i := 0; i < 10; i++ { // add value
				_, err := queue.Enqueue([]byte("hello matthew " + strconv.Itoa(i)))
				So(err, ShouldBeNil)
			}

			data, indexs, err := queue.PeekRange(3, 4)
			So(err, ShouldBeNil)
			So(len(data), ShouldEqual, 4)
			So(string(data[0]), ShouldEqual, "hello matthew 3")
			So(string(data[3]), ShouldEqual, "hello matthew 6")
			So(indexs, ShouldResemble, []int64{3, 4, 5, 6})
			So(queue.Size(), ShouldEqual, 10)

			data, _, err = queue.PeekRange(8, 5) // clamped to the end
			So(err, ShouldBeNil)
			So(len(data), ShouldEqual, 2)
			So(string(data[1]), ShouldEqual, "hello matthew 9")

			data, _, err = queue.PeekRange(3, math.MaxInt64) // offset+count overflows
			So(err, ShouldBeNil)
			So(len(data), ShouldEqual, 7)
			So(string(data[6]), ShouldEqual, "hello matthew 9")

			data, indexs, err = queue.PeekRange(10, 1) // past the end
			So(err, ShouldBeNil)
			So(data, ShouldBeEmpty)
			So(indexs, ShouldBeEmpty)

			data, _, err = queue.PeekRange(-1, 2)
			So(err, ShouldBeNil)
			So(data, ShouldBeEmpty)

			data, _, err = queue.PeekRange(0, 0)
			So(err, ShouldBeNil)
			So(data, ShouldBeEmpty)

			So(queue.Skip(2), ShouldBeNil)
			data, indexs, err = queue.PeekRange(1, 1) // relative to the new front
			So(err, ShouldBeNil)
			So(string(data[0]), ShouldEqual, "hello matthew 3")
			So(indexs, ShouldResemble, []int64{3})
		})

	})
}

// TestFileQueue_ReopenEmptyKeepsHeadPage: an empty queue reopened after its
// items reached a later data page goes on from that page, so the first Gc
// after the reopen does not remove the page its new items are on.
func TestFileQueue_ReopenEmptyKeepsHeadPage(t *testing.T) {
	path := Tempfile()
	clearFiles(path, "testqueue")
	defer clearFiles(path, "testqueue")

	Convey("TestFileQueue_ReopenEmptyKeepsHeadPage", t, func() {
		options := func() *Options {
			return &Options{DataPageSize: 128, IndexItemsPerPage: 4}
		}
		content := []byte("0123456789012345678901234567890123456789") // 40 B: 3 items per page
		var queue = new(FileQueue)
		So(queue.Open(path, "testqueue", options()), ShouldBeNil)
		enqueue(queue, content, 7, t)
		dequeue(queue, content, 7, t)
		So(queue.Gc(), ShouldBeNil)
		So(queue.headDataPageIndex, ShouldEqual, 2)
		So(queue.Close(), ShouldBeNil)

		queue = new(FileQueue)
		So(queue.Open(path, "testqueue", options()), ShouldBeNil)
		defer queue.Close()
		So(queue.IsEmpty(), ShouldBeTrue)
		So(queue.headDataPageIndex, ShouldEqual, 2)

		fresh := []byte("after the reopen")
		_, err := queue.Enqueue(fresh)
		So(err, ShouldBeNil)
		So(queue.Gc(), ShouldBeNil)
		data, _, err := queue.PeekRange(0, 1)
		So(err, ShouldBeNil)
		So(data, ShouldResemble, [][]byte{fresh})
	})
}

// TestFileQueue_GcKeepsHeadPage: a queue written before the reopen fix holds
// its head on page 0 below the page of the last dequeued item. Gc leaves the
// head page alone.
func TestFileQueue_GcKeepsHeadPage(t *testing.T) {
	path := Tempfile()
	clearFiles(path, "testqueue")
	defer clearFiles(path, "testqueue")

	Convey("TestFileQueue_GcKeepsHeadPage", t, func() {
		options := func() *Options {
			return &Options{DataPageSize: 128, IndexItemsPerPage: 4}
		}
		content := []byte("0123456789012345678901234567890123456789")
		var queue = new(FileQueue)
		So(queue.Open(path, "testqueue", options()), ShouldBeNil)
		enqueue(queue, content, 7, t)
		dequeue(queue, content, 7, t)
		So(queue.Gc(), ShouldBeNil)
		So(queue.Close(), ShouldBeNil)

		queue = new(FileQueue)
		So(queue.Open(path, "testqueue", options()), ShouldBeNil)
		// The layout the old Open left behind: head on page 0.
		queue.headDataPageIndex, queue.headDataItemOffset = 0, 0
		fresh := []byte("written on page 0")
		_, err := queue.Enqueue(fresh)
		So(err, ShouldBeNil)
		So(queue.Close(), ShouldBeNil)

		queue = new(FileQueue)
		So(queue.Open(path, "testqueue", options()), ShouldBeNil)
		defer queue.Close()
		So(queue.headDataPageIndex, ShouldEqual, 0)
		So(queue.Gc(), ShouldBeNil)
		data, _, err := queue.PeekRange(0, 1)
		So(err, ShouldBeNil)
		So(data, ShouldResemble, [][]byte{fresh})

		// The head climbs past the front's page 2 while the front stays:
		// every page from the first unread item up holds unread items.
		want := [][]byte{fresh}
		for n := 1; n <= 12; n++ {
			_, err := queue.Enqueue(content)
			So(err, ShouldBeNil)
			want = append(want, content)
			So(queue.Gc(), ShouldBeNil)
			data, _, err = queue.PeekRange(0, int64(n+1))
			So(err, ShouldBeNil)
			So(data, ShouldResemble, want)
		}
		So(queue.headDataPageIndex, ShouldBeGreaterThan, 2)
	})
}
