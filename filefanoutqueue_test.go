package bigqueue

import (
	"strconv"
	"strings"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

// TestFanoutQueueOpen to test Open() function
func TestFanoutQueueOpen(t *testing.T) {
	path := Tempfile()
	defer clearFiles(path, "fanoutqueue")
	fq := FileFanoutQueue{}
	err := fq.Open(path, "fanoutqueue", nil)
	if err != nil {
		t.Error("open fanout queue failed", err)
	}
	fq.Close()
}

// TestFanoutQueueOpen to test Open() function
func TestFanoutQueueOpenTwice(t *testing.T) {
	path := Tempfile()
	defer clearFiles(path, "fanoutqueue")
	fq := FileFanoutQueue{}
	err := fq.Open(path, "fanoutqueue", nil)
	if err != nil {
		t.Error("open fanout queue failed", err)
	}

	err = fq.Open(path, "fanoutqueue", nil)
	if err == nil {
		t.Error("open fanout queue twice should return error but actually return nil")
	}
	fq.Close()
}

// TestFanoutQueueIsEmpty to test open a empty directory should return empty queue
func TestFanoutQueueIsEmpty(t *testing.T) {
	path := Tempfile()
	clearFiles(path, "fanoutqueue")
	fanoutID := int64(100)
	defer clearFrontIndexFiles(path, "fanoutqueue", fanoutID)

	fq := FileFanoutQueue{}
	err := fq.Open(path, "fanoutqueue", nil)

	if err != nil {
		t.Error("open fanout queue failed", err)
	}
	defer fq.Close()
	defer clearFiles(path, "fanoutqueue")

	bool := fq.IsEmpty(fanoutID)
	if !bool {
		t.Error("New created queue must be empty")
	}
}

// TestFanoutQueueSize to test queue Size() function
func TestFanoutQueueSize(t *testing.T) {
	path := Tempfile()
	clearFiles(path, "fanoutqueue")
	fanoutID := int64(100)
	defer clearFrontIndexFiles(path, "fanoutqueue", fanoutID)

	fq := FileFanoutQueue{}
	err := fq.Open(path, "fanoutqueue", nil)

	if err != nil {
		t.Error("open fanout queue failed", err)
	}

	defer fq.Close()
	defer clearFiles(path, "fanoutqueue")

	sz := fq.Size(fanoutID)
	if sz != 0 {
		t.Error("New created queue size must be zero")
	}
}

// TestFanoutQueueEnqueue to test enqueue only function
func TestFanoutQueueEnqueue(t *testing.T) {
	path := Tempfile()
	clearFiles(path, "fanoutqueue")
	fanoutID := int64(100)
	defer clearFrontIndexFiles(path, "fanoutqueue", fanoutID)

	fq := FileFanoutQueue{}
	err := fq.Open(path, "fanoutqueue", nil)

	if err != nil {
		t.Error("open fanout queue failed", err)
	}
	defer fq.Close()
	defer clearFiles(path, "fanoutqueue")
	sz := fq.Size(fanoutID)
	if sz != 0 {
		t.Error("New created queue size must be zero")
	}

	_, err = fq.Enqueue([]byte("hello world"))

	if err != nil {
		t.Error("enqueue action failed", err)
	}

	sz = fq.Size(fanoutID)
	if sz != 1 {
		t.Error("New created queue size must be 1", sz)
	}

	bool := fq.IsEmpty(fanoutID)
	if bool {
		t.Error("New created queue must be empty", bool)
	}
}

func clearFrontIndexFiles(path, queueName string, fanoutID int64) {
	RemoveFiles(path + "/" + queueName + "/" + FanoutFrontFileName + strconv.Itoa(int(fanoutID)))
}

// TestFanoutQueueEnqueueDequeue to test enqueue and dequeue function
func TestFanoutQueueEnqueueDequeue(t *testing.T) {
	path := Tempfile()
	clearFiles(path, "fanoutqueue")
	fanoutID := int64(100)
	fanoutID1 := int64(101)
	defer clearFrontIndexFiles(path, "fanoutqueue", fanoutID)
	defer clearFrontIndexFiles(path, "fanoutqueue", fanoutID1)

	fq := FileFanoutQueue{}
	err := fq.Open(path, "fanoutqueue", nil)

	if err != nil {
		t.Error("open fanout queue failed", err)
	}
	defer fq.Close()
	defer clearFiles(path, "fanoutqueue")

	_, err = fq.Enqueue([]byte("hello world"))

	if err != nil {
		t.Error("enqueue action failed", err)
	}

	index, data, _ := fq.Dequeue(fanoutID)
	index1, data1, _ := fq.Dequeue(fanoutID1)
	if index != index1 {
		t.Error("index should same", index, index1)
	}

	if strings.Compare(string(data), string(data1)) != 0 {
		t.Error("data should same")
	}
}

// TestFanoutQueueEnqueuePeek to test Peek() function
func TestFanoutQueueEnqueuePeek(t *testing.T) {
	path := Tempfile()
	clearFiles(path, "fanoutqueue")
	fanoutID := int64(100)
	fanoutID1 := int64(101)
	defer clearFrontIndexFiles(path, "fanoutqueue", fanoutID)
	defer clearFrontIndexFiles(path, "fanoutqueue", fanoutID1)

	fq := FileFanoutQueue{}
	err := fq.Open(path, "fanoutqueue", nil)

	if err != nil {
		t.Error("open fanout queue failed", err)
	}
	defer fq.Close()
	defer clearFiles(path, "fanoutqueue")

	_, err = fq.Enqueue([]byte("hello world"))

	if err != nil {
		t.Error("enqueue action failed", err)
	}

	index, data, _ := fq.Peek(fanoutID)
	index1, data1, _ := fq.Peek(fanoutID1)

	if index != index1 {
		t.Error("index should same", index, index1)
	}

	if strings.Compare(string(data), string(data1)) != 0 {
		t.Error("data should same")
	}

	// test peek all
	dataAll, indexAll, err := fq.PeekAll(fanoutID)
	if err != nil {
		t.Error(err)
	}

	if len(dataAll) != 1 || len(indexAll) != 1 {
		t.Error("peek All size error should be", 1, "but actual is ", len(dataAll))
	}
}

// TestFanoutQueueSkip to test Skip() function
func TestFanoutQueueSkip(t *testing.T) {
	path := Tempfile()
	clearFiles(path, "fanoutqueue")
	fanoutID := int64(100)
	fanoutID1 := int64(101)

	defer clearFrontIndexFiles(path, "fanoutqueue", fanoutID)
	defer clearFrontIndexFiles(path, "fanoutqueue", fanoutID1)

	fq := FileFanoutQueue{}
	err := fq.Open(path, "fanoutqueue", nil)

	if err != nil {
		t.Error("open fanout queue failed", err)
	}
	defer fq.Close()
	defer clearFiles(path, "fanoutqueue")

	for i := 0; i < 10; i++ {
		_, err = fq.Enqueue([]byte("hello world" + strconv.Itoa(i)))

		if err != nil {
			t.Error("enqueue action failed", err)
		}
	}

	fq.Skip(fanoutID, int64(5))
	index, data, _ := fq.Peek(fanoutID)
	if index != 5 {
		t.Error("index should be 5 but actually", index, data)
	}

	fq.Skip(fanoutID1, int64(1))
	index1, data1, _ := fq.Peek(fanoutID1)
	if index1 != 1 {
		t.Error("index should be 1 but actually", index1, data1)
	}
}

// TestFanoutQueueSubscribe to test Subscribe() function with multiple subscriber ids
func TestFanoutQueueSubscribe(t *testing.T) {

	path := Tempfile()
	clearFiles(path, "fanoutqueue")
	fanoutID := int64(100)
	fanoutID1 := int64(101)

	defer clearFrontIndexFiles(path, "fanoutqueue", fanoutID)
	defer clearFrontIndexFiles(path, "fanoutqueue", fanoutID1)

	fq := FileFanoutQueue{}
	err := fq.Open(path, "fanoutqueue", nil)

	if err != nil {
		t.Error("open fanout queue failed", err)
	}
	defer fq.Close()
	defer clearFiles(path, "fanoutqueue")

	fanoutIDCount1, fanoutIDCount2 := 0, 0
	count := 10

	fq.Subscribe(fanoutID, func(index int64, data []byte, err error) {
		fanoutIDCount1++
	})

	for i := 0; i < count; i++ {
		_, err = fq.Enqueue([]byte("hello world" + strconv.Itoa(i)))

		if err != nil {
			t.Error("enqueue action failed", err)
		}
	}

	fq.Subscribe(fanoutID1, func(index int64, data []byte, err error) {
		fanoutIDCount2++
	})

	time.Sleep(time.Duration(3) * time.Second)

	if fanoutIDCount1 != count {
		t.Error("subscribe id=", fanoutID, " count should be ", count, " but actually is ", fanoutIDCount1)
	}

	if fanoutIDCount2 != count {
		t.Error("subscribe id=", fanoutID1, " count should be ", count, " but actually is ", fanoutIDCount2)
	}

}

// TestFileQueue_Status
func TestFanoutQueue_Status(t *testing.T) {
	Convey("Test empty queue status result", t, func() {
		path := Tempfile()
		clearFiles(path, "fanoutqueue")
		fanoutID := int64(100)

		defer clearFrontIndexFiles(path, "fanoutqueue", fanoutID)

		queue := FileFanoutQueue{}
		err := queue.Open(path, "fanoutqueue", nil)

		if err != nil {
			t.Error("open fanout queue failed", err)
		}
		defer queue.Close()
		defer clearFiles(path, "fanoutqueue")

		qFileStatus := queue.Status(fanoutID)

		So(qFileStatus, ShouldNotBeNil)
		So(qFileStatus.FrontIndex, ShouldEqual, 0)
		So(qFileStatus.HeadIndex, ShouldEqual, 0)
		So(qFileStatus.TailIndex, ShouldEqual, 0)
		So(qFileStatus.HeadDataPageIndex, ShouldEqual, 0)
		So(qFileStatus.HeadDataItemOffset, ShouldEqual, 0)

		So(len(qFileStatus.IndexFileList), ShouldEqual, 0)
		So(len(qFileStatus.DataFileList), ShouldEqual, 0)
		So(qFileStatus.MetaFileInfo, ShouldNotBeNil)
		So(qFileStatus.FrontFileInfo, ShouldNotBeNil)

	})

	Convey("Test non-empty queue status result", t, func() {
		path := Tempfile()

		fanoutID := int64(100)

		defer clearFrontIndexFiles(path, "fanoutqueue", fanoutID)

		queue := FileFanoutQueue{}
		err := queue.Open(path, "fanoutqueue", nil)

		if err != nil {
			t.Error("open fanout queue failed", err)
		}
		defer queue.Close()
		defer clearFiles(path, "fanoutqueue")

		data := []byte("hello xmatthew")
		dataLen := len(data)

		queue.Enqueue(data)
		queue.Dequeue(fanoutID)

		qFileStatus := queue.Status(fanoutID)

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

	})

}

func TestFanoutQueue_PeekPagination(t *testing.T) {
	Convey("Test PeekPagination", t, func() {
		path := Tempfile()
		clearFiles(path, "fanoutqueue")
		fanoutID := int64(100)

		defer clearFrontIndexFiles(path, "fanoutqueue", fanoutID)

		queue := FileFanoutQueue{}
		err := queue.Open(path, "fanoutqueue", nil)
		if err != nil {
			t.Error(err)
		}
		defer queue.Close()
		defer clearFiles(path, "fanoutqueue")

		Convey("test PeekPagination on empty queue", func() {
			data, indexs, err := queue.PeekPagination(fanoutID, 0, 0)
			So(err, ShouldBeNil)
			So(data, ShouldBeEmpty)
			So(indexs, ShouldBeEmpty)

			data, indexs, err = queue.PeekPagination(fanoutID, 1, 1)
			So(err, ShouldBeNil)
			So(data, ShouldBeEmpty)
			So(indexs, ShouldBeEmpty)
		})

		Convey("test PeekPagination on items small than pagesize", func() {
			for i := 0; i < 5; i++ { // add value
				_, err := queue.Enqueue([]byte("hello matthew " + strconv.Itoa(i)))
				So(err, ShouldBeNil)
			}

			data, indexs, err := queue.PeekPagination(fanoutID, 0, 0)
			So(err, ShouldBeNil)
			So(len(data), ShouldEqual, 5)
			So(string(data[4]), ShouldEqual, "hello matthew 4")
			So(len(indexs), ShouldEqual, 5)

			data, indexs, err = queue.PeekPagination(fanoutID, 1, 10)
			So(err, ShouldBeNil)
			So(len(data), ShouldEqual, 5)
			So(string(data[4]), ShouldEqual, "hello matthew 4")
			So(len(indexs), ShouldEqual, 5)

			data, indexs, err = queue.PeekPagination(fanoutID, 2, 10) // large paing
			So(err, ShouldBeNil)
			So(data, ShouldBeEmpty)
			So(indexs, ShouldBeEmpty)

			data, indexs, err = queue.PeekPagination(fanoutID, 2, 2)
			So(err, ShouldBeNil)
			So(len(data), ShouldEqual, 2)
			So(string(data[1]), ShouldEqual, "hello matthew 3")
			So(len(indexs), ShouldEqual, 2)
		})

	})
}
