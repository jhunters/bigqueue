package bigqueue

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

// TestFanoutQueueOpen to test Open() function
func TestFanoutQueueOpen(t *testing.T) {
	path := Tempfile()
	defer clearFiles(path, "fanoutqueue")

	Convey("TestFanoutQueueOpen", t, func() {

		fq := FileFanoutQueue{}
		err := fq.Open(path, "fanoutqueue", nil)
		So(err, ShouldBeNil)
		fq.Close()
	})

}

// TestFanoutQueueOpen to test Open() function
func TestFanoutQueueOpenTwice(t *testing.T) {
	path := Tempfile()
	defer clearFiles(path, "fanoutqueue")

	Convey("TestFanoutQueueOpenTwice", t, func() {

		fq := FileFanoutQueue{}
		err := fq.Open(path, "fanoutqueue", nil)
		So(err, ShouldBeNil)

		err = fq.Open(path, "fanoutqueue", nil)
		So(err, ShouldNotBeNil)
		fq.Close()
	})

}

// TestFanoutQueueIsEmpty to test open a empty directory should return empty queue
func TestFanoutQueueIsEmpty(t *testing.T) {
	path := Tempfile()
	clearFiles(path, "fanoutqueue")
	defer clearFiles(path, "fanoutqueue")

	Convey("TestFanoutQueueIsEmpty", t, func() {

		fanoutID := int64(100)
		defer clearFrontIndexFiles(path, "fanoutqueue", fanoutID)

		fq := FileFanoutQueue{}
		err := fq.Open(path, "fanoutqueue", nil)
		So(err, ShouldBeNil)
		defer fq.Close()

		bool := fq.IsEmpty(fanoutID)
		SoMsg("New created queue must be empty", bool, ShouldBeTrue)
	})

}

// TestFanoutQueueSize to test queue Size() function
func TestFanoutQueueSize(t *testing.T) {
	path := Tempfile()
	clearFiles(path, "fanoutqueue")
	defer clearFiles(path, "fanoutqueue")

	Convey("TestFanoutQueueSize", t, func() {
		fanoutID := int64(100)
		defer clearFrontIndexFiles(path, "fanoutqueue", fanoutID)

		fq := FileFanoutQueue{}
		err := fq.Open(path, "fanoutqueue", nil)
		So(err, ShouldBeNil)
		defer fq.Close()

		sz := fq.Size(fanoutID)
		SoMsg("New created queue size must be zero", sz, ShouldEqual, 0)
	})

}

// TestFanoutQueueEnqueue to test enqueue only function
func TestFanoutQueueEnqueue(t *testing.T) {
	path := Tempfile()
	clearFiles(path, "fanoutqueue")
	fanoutID := int64(100)
	defer clearFrontIndexFiles(path, "fanoutqueue", fanoutID)
	defer clearFiles(path, "fanoutqueue")

	Convey("TestFanoutQueueEnqueue", t, func() {
		fq := FileFanoutQueue{}
		err := fq.Open(path, "fanoutqueue", nil)

		So(err, ShouldBeNil)
		defer fq.Close()
		sz := fq.Size(fanoutID)
		SoMsg("New created queue size must be zero", sz, ShouldEqual, 0)

		_, err = fq.Enqueue([]byte("hello world"))

		So(err, ShouldBeNil)

		sz = fq.Size(fanoutID)
		So(sz, ShouldEqual, 1)

		bool := fq.IsEmpty(fanoutID)
		So(bool, ShouldBeFalse)
	})

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
	defer clearFiles(path, "fanoutqueue")

	Convey("TestFanoutQueueEnqueueDequeue", t, func() {
		fq := FileFanoutQueue{}
		err := fq.Open(path, "fanoutqueue", nil)

		So(err, ShouldBeNil)
		defer fq.Close()

		_, err = fq.Enqueue([]byte("hello world"))

		So(err, ShouldBeNil)

		index, data, _ := fq.Dequeue(fanoutID)
		index1, data1, _ := fq.Dequeue(fanoutID1)
		So(index, ShouldEqual, index1)
		So(data, ShouldResemble, data1)
	})

}

// TestFanoutQueueEnqueuePeek to test Peek() function
func TestFanoutQueueEnqueuePeek(t *testing.T) {
	path := Tempfile()
	clearFiles(path, "fanoutqueue")
	fanoutID := int64(100)
	fanoutID1 := int64(101)
	defer clearFrontIndexFiles(path, "fanoutqueue", fanoutID)
	defer clearFrontIndexFiles(path, "fanoutqueue", fanoutID1)
	defer clearFiles(path, "fanoutqueue")

	Convey("TestFanoutQueueEnqueuePeek", t, func() {

		fq := FileFanoutQueue{}
		err := fq.Open(path, "fanoutqueue", nil)

		So(err, ShouldBeNil)
		defer fq.Close()

		_, err = fq.Enqueue([]byte("hello world"))

		So(err, ShouldBeNil)

		index, data, _ := fq.Peek(fanoutID)
		index1, data1, _ := fq.Peek(fanoutID1)

		So(index, ShouldEqual, index1)
		So(data, ShouldResemble, data1)

		// test peek all
		dataAll, indexAll, err := fq.PeekAll(fanoutID)
		So(err, ShouldBeNil)

		So(len(dataAll), ShouldEqual, 1)
		So(len(indexAll), ShouldEqual, 1)
	})

}

// TestFanoutQueueSkip to test Skip() function
func TestFanoutQueueSkip(t *testing.T) {
	path := Tempfile()
	clearFiles(path, "fanoutqueue")
	fanoutID := int64(100)
	fanoutID1 := int64(101)

	defer clearFrontIndexFiles(path, "fanoutqueue", fanoutID)
	defer clearFrontIndexFiles(path, "fanoutqueue", fanoutID1)
	defer clearFiles(path, "fanoutqueue")

	Convey("TestFanoutQueueSkip", t, func() {
		fq := FileFanoutQueue{}
		err := fq.Open(path, "fanoutqueue", nil)

		So(err, ShouldBeNil)
		defer fq.Close()

		for i := 0; i < 10; i++ {
			_, err = fq.Enqueue([]byte("hello world" + strconv.Itoa(i)))
			So(err, ShouldBeNil)
		}

		fq.Skip(fanoutID, int64(5))
		index, data, _ := fq.Peek(fanoutID)
		SoMsg(fmt.Sprintf("index should be 5 but actually %d", index), index, ShouldEqual, 5)
		So(data, ShouldNotBeNil)

		fq.Skip(fanoutID1, int64(1))
		index1, data1, _ := fq.Peek(fanoutID1)
		So(index1, ShouldEqual, 1)
		So(data1, ShouldNotBeNil)
	})

}

// TestFanoutQueueSubscribe to test Subscribe() function with multiple subscriber ids
func TestFanoutQueueSubscribe(t *testing.T) {

	path := Tempfile()
	clearFiles(path, "fanoutqueue")
	fanoutID := int64(100)
	fanoutID1 := int64(101)

	defer clearFrontIndexFiles(path, "fanoutqueue", fanoutID)
	defer clearFrontIndexFiles(path, "fanoutqueue", fanoutID1)
	defer clearFiles(path, "fanoutqueue")

	Convey("TestFanoutQueueSubscribe", t, func() {

		fq := FileFanoutQueue{}
		err := fq.Open(path, "fanoutqueue", nil)

		So(err, ShouldBeNil)
		defer fq.Close()

		fanoutIDCount1, fanoutIDCount2 := 0, 0
		count := 10

		fq.Subscribe(fanoutID, func(index int64, data []byte, err error) {
			fanoutIDCount1++
		})

		for i := 0; i < count; i++ {
			_, err = fq.Enqueue([]byte("hello world" + strconv.Itoa(i)))
			So(err, ShouldBeNil)
		}

		fq.Subscribe(fanoutID1, func(index int64, data []byte, err error) {
			fanoutIDCount2++
		})

		time.Sleep(time.Duration(3) * time.Second)

		So(fanoutIDCount1, ShouldEqual, count)
		So(fanoutIDCount2, ShouldEqual, count)
	})

}

// TestFileQueue_Status
func TestFanoutQueue_Status(t *testing.T) {
	Convey("Test empty queue status result", t, func() {
		path := Tempfile()
		clearFiles(path, "fanoutqueue")
		fanoutID := int64(100)

		defer clearFrontIndexFiles(path, "fanoutqueue", fanoutID)
		defer clearFiles(path, "fanoutqueue")

		queue := FileFanoutQueue{}
		err := queue.Open(path, "fanoutqueue", nil)

		if err != nil {
			t.Error("open fanout queue failed", err)
		}
		defer queue.Close()

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
